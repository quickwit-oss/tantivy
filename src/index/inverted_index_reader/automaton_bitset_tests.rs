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

#[derive(Debug)]
struct StreamingPostings {
    bytes: OwnedBytes,
    skipped: Range<usize>,
    max_lists: usize,
}

impl HasLen for StreamingPostings {
    fn len(&self) -> usize {
        self.bytes.len()
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
        stream::iter(ranges.take(self.max_lists).map(|range| {
            assert!(range.end <= self.skipped.start || range.start >= self.skipped.end);
            Ok(self.bytes.slice(range))
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
    let bytes = source.postings_file_slice.read_bytes()?;
    let executor = Executor::multi_thread(1, "bitset-test-")?;
    let mut read = |max_lists| {
        let postings = StreamingPostings {
            bytes: bytes.clone(),
            skipped: skipped.clone(),
            max_lists,
        };
        reader.postings_file_slice = FileSlice::new(Arc::new(postings));
        futures::executor::block_on(reader.create_bitset_for_automaton(
            Regex::new("match.*|z").unwrap(),
            3,
            &executor,
        ))
    };
    let bitset = read(usize::MAX)?;
    assert_eq!(bitset.len(), 2);
    assert!(bitset.contains(0) && bitset.contains(2));
    assert_eq!(read(1).unwrap_err().kind(), io::ErrorKind::UnexpectedEof);
    Ok(())
}
