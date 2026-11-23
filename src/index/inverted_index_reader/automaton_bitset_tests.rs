use std::io;
use std::ops::Range;
use std::sync::Arc;

use common::HasLen;
use futures_util::stream::{self, BoxStream, StreamExt};
use tantivy_fst::Regex;

use super::InvertedIndexReader;
use crate::directory::{FileHandle, FileSlice, OwnedBytes};
use crate::schema::{Schema, STRING};
use crate::{Executor, Index, IndexWriter, TantivyDocument};

#[test]
fn test_create_bitset_for_automaton() -> crate::Result<()> {
    let mut schema_builder = Schema::builder();
    let field = schema_builder.add_text_field("field", STRING);
    let index = Index::create_in_ram(schema_builder.build());
    let mut writer: IndexWriter = index.writer_for_tests()?;
    writer.add_document(doc!(field => "apple", field => "banana"))?;
    writer.add_document(doc!(field => "apricot"))?;
    writer.add_document(doc!(field => "banana", field => "berry"))?;
    writer.add_document(doc!(field => "carrot"))?;
    // Exercise full postings blocks as well as short lists and duplicate matches.
    for _ in 0..260 {
        writer.add_document(doc!(field => "apple", field => "apricot"))?;
    }
    writer.commit()?;
    let searcher = index.reader()?.searcher();
    let segment_reader = searcher.segment_reader(0);
    let inverted_index = segment_reader.inverted_index(field)?;
    let executor = Executor::multi_thread(1, "warmup-test-")?;
    for (pattern, expected) in [
        ("b.*", vec![0, 2]),
        ("z.*", vec![]),
        ("a.*", [vec![0, 1], (4..264).collect()].concat()),
        ("apple|berry", [vec![0, 2], (4..264).collect()].concat()),
        (".*", (0..264).collect()),
    ] {
        let bitset = futures::executor::block_on(inverted_index.create_bitset_for_automaton(
            Regex::new(pattern).unwrap(),
            segment_reader.max_doc(),
            &executor,
        ))?;
        assert_eq!(bitset.len(), expected.len());
        let docs = (0..segment_reader.max_doc()).filter(|&doc| bitset.contains(doc));
        itertools::assert_equal(docs, expected);
    }
    Ok(())
}

#[test]
fn test_create_bitset_for_automaton_empty_index() -> crate::Result<()> {
    let inverted_index = InvertedIndexReader::empty(crate::schema::IndexRecordOption::Basic);
    let executor = Executor::single_thread();
    let bitset = futures::executor::block_on(inverted_index.create_bitset_for_automaton(
        Regex::new(".*").unwrap(),
        0,
        &executor,
    ))?;
    assert_eq!(bitset.len(), 0);
    Ok(())
}

#[derive(Debug)]
struct StreamingPostings {
    postings_file_bytes: OwnedBytes,
    skipped: Range<usize>,
}

impl HasLen for StreamingPostings {
    fn len(&self) -> usize {
        self.postings_file_bytes.len()
    }
}

impl FileHandle for StreamingPostings {
    fn read_bytes(&self, _: Range<usize>) -> io::Result<OwnedBytes> {
        panic!("postings must be streamed");
    }

    fn stream_ranges(
        &self,
        ranges: Box<dyn Iterator<Item = Range<usize>> + Send>,
    ) -> BoxStream<'_, io::Result<OwnedBytes>> {
        stream::iter(ranges.map(|range| {
            assert!(range.end <= self.skipped.start || range.start >= self.skipped.end);
            Ok(self.postings_file_bytes.slice(range))
        }))
        .boxed()
    }
}

#[test]
fn streams_matching_lists_across_decode_batches() -> crate::Result<()> {
    let mut schema = Schema::builder();
    let field = schema.add_text_field("field", STRING);
    let index = Index::create_in_ram(schema.build());
    let mut writer: IndexWriter = index.writer_with_num_threads(1, 64_000_000)?;
    // Enough singleton lists to exceed the 1 MiB decode batch target.
    let mut doc = TantivyDocument::default();
    for i in 0..32_000 {
        doc.add_text(field, format!("match{i:05}"));
    }
    writer.add_document(doc)?;
    writer.add_document(doc!(field => "skip"))?;
    writer.add_document(doc!(field => "z"))?;
    writer.commit()?;

    let searcher = index.reader()?.searcher();
    assert_eq!(searcher.segment_readers().len(), 1);
    let source = searcher.segment_reader(0).inverted_index(field)?;
    let mut reader = InvertedIndexReader::empty(source.record_option);
    reader.termdict = source.termdict.clone();
    let skipped = reader.termdict.get(b"skip")?.unwrap().postings_range;
    let postings_file_bytes = source.postings_file_slice.read_bytes()?;
    let executor = Executor::multi_thread(1, "bitset-test-")?;
    reader.postings_file_slice = FileSlice::new(Arc::new(StreamingPostings {
        postings_file_bytes,
        skipped,
    }));
    let bitset = futures::executor::block_on(reader.create_bitset_for_automaton(
        Regex::new("match.*|z").unwrap(),
        3,
        &executor,
    ))?;
    assert_eq!(bitset.len(), 2);
    assert!(bitset.contains(0) && bitset.contains(2));
    Ok(())
}
