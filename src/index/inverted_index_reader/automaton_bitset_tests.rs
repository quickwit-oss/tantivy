use std::future::Future;
use std::io;
use std::ops::Range;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};

use common::{BitSet, HasLen};
use futures_util::stream::{self, BoxStream, StreamExt};
use tantivy_fst::Regex;

use super::InvertedIndexReader;
use crate::directory::{FileHandle, FileSlice, OwnedBytes};
use crate::query::BitSetDocSet;
use crate::schema::{IndexRecordOption, Schema, TextFieldIndexing, TextOptions};
use crate::termdict::{TermDictionary, TermDictionaryBuilder};
use crate::{DocSet, Executor, Index, IndexWriter, TERMINATED};

// A file either reads normally or injects one failure/cancellation scenario.
#[derive(Clone, Copy, Debug, Default)]
enum ReadBehavior {
    #[default]
    Normal,
    FailOpen,
    Pending,
    FailBody,
    Short,
    Missing,
    Extra,
}

#[derive(Debug, Default)]
struct ReadState {
    reject_sync: bool,
    behavior: ReadBehavior,
    reads: Vec<Range<usize>>,
    buffers: Vec<Weak<[u8]>>,
}

#[derive(Debug)]
struct AsyncFile {
    bytes: OwnedBytes,
    state: Mutex<ReadState>,
    bytes_read: Arc<AtomicUsize>,
}

impl AsyncFile {
    fn new(bytes: OwnedBytes) -> Arc<Self> {
        Arc::new(Self {
            bytes,
            state: Mutex::default(),
            bytes_read: Arc::default(),
        })
    }

    fn read_range(&self, range: Range<usize>) -> io::Result<(OwnedBytes, ReadBehavior)> {
        let mut state = self.state.lock().unwrap();
        state.reads.push(range.clone());
        if matches!(state.behavior, ReadBehavior::FailOpen) {
            return Err(io::Error::other("injected read failure"));
        }
        // Each read owns a fresh buffer, tracked separately from the fixture's bytes.
        let bytes: Arc<[u8]> = Arc::from(&self.bytes[range]);
        state.buffers.push(Arc::downgrade(&bytes));
        Ok((OwnedBytes::new(bytes), state.behavior))
    }

    fn set_behavior(&self, behavior: ReadBehavior) {
        self.state.lock().unwrap().behavior = behavior;
    }

    fn reject_sync(&self) {
        self.state.lock().unwrap().reject_sync = true;
    }

    fn assert_released(&self) {
        assert!(self
            .state
            .lock()
            .unwrap()
            .buffers
            .iter()
            .all(|buffer| buffer.strong_count() == 0));
    }
}

impl HasLen for AsyncFile {
    fn len(&self) -> usize {
        self.bytes.len()
    }
}

impl FileHandle for AsyncFile {
    fn read_bytes(&self, range: Range<usize>) -> io::Result<OwnedBytes> {
        assert!(
            !self.state.lock().unwrap().reject_sync,
            "synchronous body reads forbidden"
        );
        Ok(self.bytes.slice(range))
    }

    // Spell out async_trait's signature to avoid adding a test-only dependency.
    fn read_bytes_async<'a, 'f>(
        &'a self,
        range: Range<usize>,
    ) -> Pin<Box<dyn Future<Output = io::Result<OwnedBytes>> + Send + 'f>>
    where
        'a: 'f,
        Self: 'f,
    {
        Box::pin(async move {
            let (bytes, behavior) = self.read_range(range)?;
            if matches!(behavior, ReadBehavior::Pending) {
                std::future::pending::<()>().await;
            }
            Ok(bytes)
        })
    }

    fn read_bytes_batch_stream(
        &self,
        ranges: Box<dyn Iterator<Item = Range<usize>> + Send>,
    ) -> BoxStream<'_, io::Result<OwnedBytes>> {
        let behavior = self.state.lock().unwrap().behavior;
        if matches!(behavior, ReadBehavior::Missing) {
            return stream::empty().boxed();
        }
        let extra = matches!(behavior, ReadBehavior::Extra).then_some(0..0);
        stream::iter(ranges.chain(extra))
            .then(move |range| async move {
                let (bytes, behavior) = self.read_range(range)?;
                match behavior {
                    ReadBehavior::Pending => std::future::pending::<()>().await,
                    ReadBehavior::FailBody => {
                        return Err(io::Error::other("injected body failure"))
                    }
                    ReadBehavior::Short => return Ok(bytes.slice(0..0)),
                    _ => {}
                }
                self.bytes_read.fetch_add(bytes.len(), Ordering::SeqCst);
                Ok(bytes)
            })
            .boxed()
    }
}

fn fixture(
    option: IndexRecordOption,
) -> crate::Result<(InvertedIndexReader, Arc<AsyncFile>, Arc<AsyncFile>)> {
    let mut schema = Schema::builder();
    let options = TextOptions::default().set_indexing_options(
        TextFieldIndexing::default()
            .set_tokenizer("raw")
            .set_index_option(option),
    );
    let field = schema.add_text_field("field", options);
    let index = Index::create_in_ram(schema.build());
    let mut writer: IndexWriter = index.writer_for_tests()?;
    writer.add_document(doc!(field => "apple", field => "banana"))?;
    writer.add_document(doc!(field => "apricot"))?;
    writer.add_document(doc!(field => "banana", field => "berry"))?;
    writer.add_document(doc!(field => "carrot"))?;
    for _ in 0..260 {
        writer.add_document(doc!(field => "apple", field => "apricot"))?;
    }
    writer.commit()?;
    let searcher = index.reader()?.searcher();
    let original = searcher.segment_reader(0).inverted_index(field)?;
    let mut dictionary = TermDictionaryBuilder::create(Vec::new())?;
    let mut stream = original.terms().stream()?;
    while let Some((key, info)) = stream.next() {
        dictionary.insert(key, info)?;
    }
    let dictionary_file = AsyncFile::new(OwnedBytes::new(dictionary.finish()?));
    // Opening metadata is allowed. All reads during materialization must be asynchronous.
    let termdict = TermDictionary::open(FileSlice::new(dictionary_file.clone()))?;
    dictionary_file.reject_sync();
    let postings_file = AsyncFile::new(original.postings_file_slice.read_bytes()?);
    postings_file.reject_sync();
    let reader = InvertedIndexReader {
        termdict,
        postings_file_slice: FileSlice::new(postings_file.clone()),
        positions_file_slice: FileSlice::empty(),
        record_option: option,
        total_num_tokens: original.total_num_tokens,
    };
    Ok((reader, dictionary_file, postings_file))
}

fn read_bitset(
    reader: &InvertedIndexReader,
    pattern: &str,
    max_doc: u32,
    executor: &Executor,
) -> io::Result<BitSet> {
    futures::executor::block_on(reader.create_bitset_for_automaton(
        Regex::new(pattern).unwrap(),
        max_doc,
        executor,
    ))
}

#[test]
fn owned_bitset_async_reads() -> crate::Result<()> {
    let executor = Executor::multi_thread(1, "owned-bitset-test-")?;
    for option in [
        IndexRecordOption::Basic,
        IndexRecordOption::WithFreqs,
        IndexRecordOption::WithFreqsAndPositions,
    ] {
        for (pattern, expected) in [
            ("carrot", vec![3]), // singleton
            ("b.*", vec![0, 2]), // overlapping short lists
            ("z.*", vec![]),
            ("a.*", [vec![0, 1], (4..264).collect()].concat()), // full blocks and overlap
            ("apple|berry", [vec![0, 2], (4..264).collect()].concat()), // disjoint ranges
            (".*", (0..264).collect()),
        ] {
            let (reader, dictionary_file, postings_file) = fixture(option)?;
            // Expect exactly the matching ranges, even when nearby terms could share a read.
            let dictionary =
                TermDictionary::open(FileSlice::new(Arc::new(dictionary_file.bytes.clone())))?;
            let mut stream = dictionary
                .search(Regex::new(pattern).unwrap())
                .into_stream()?;
            let mut ranges = Vec::new();
            while let Some((_, info)) = stream.next() {
                ranges.push(info.postings_range.clone());
            }
            let bitset = read_bitset(&reader, pattern, 264, &executor)?;
            assert_eq!(dictionary_file.state.lock().unwrap().reads.len(), 1);
            assert_eq!(postings_file.state.lock().unwrap().reads, ranges);
            dictionary_file.assert_released();
            postings_file.assert_released();
            drop(reader);
            assert_eq!(bitset.len(), expected.len());
            let mut docset = BitSetDocSet::from(bitset);
            let mut actual = Vec::new();
            while docset.doc() != TERMINATED {
                actual.push(docset.doc());
                docset.advance();
            }
            assert_eq!(actual, expected, "{pattern} {option:?}");
        }
    }
    Ok(())
}

#[test]
fn owned_bitset_many_terms() -> crate::Result<()> {
    let (mut reader, dictionary, postings) = fixture(IndexRecordOption::Basic)?;
    let source = TermDictionary::open(FileSlice::new(Arc::new(dictionary.bytes.clone())))?;
    // Cross two batch boundaries and leave a final partial batch.
    let apple_len = source.get(b"apple")?.unwrap().postings_range.len();
    let num_terms = 2 * (1024 * 1024 / apple_len + 1) + 1;
    let mut builder = TermDictionaryBuilder::create(Vec::new())?;
    let mut bytes = Vec::new();
    for term in 0..num_terms {
        let key = if term == num_terms - 1 {
            b"berry"
        } else {
            b"apple"
        };
        let mut info = source.get(key)?.unwrap();
        let start = bytes.len();
        bytes.extend_from_slice(&postings.bytes[info.postings_range.clone()]);
        info.postings_range = start..bytes.len();
        builder.insert(format!("{term:08}").as_bytes(), &info)?;
    }
    reader.termdict = TermDictionary::open(FileSlice::from(builder.finish()?))?;
    let postings = AsyncFile::new(OwnedBytes::new(bytes));
    postings.reject_sync();
    reader.postings_file_slice = FileSlice::new(postings.clone());
    let executor = Executor::multi_thread(1, "bitset-test-")?;
    let bitset = read_bitset(&reader, ".*", 264, &executor)?;
    assert_eq!(bitset.len(), 262);
    assert!(bitset.contains(2)); // The final term must also be decoded.
    assert_eq!(postings.bytes_read.load(Ordering::SeqCst), postings.len());
    postings.assert_released();
    Ok(())
}

#[test]
fn owned_bitset_cancellation_releases_buffers() -> crate::Result<()> {
    let executor = Executor::single_thread();
    for pause_dictionary in [true, false] {
        let (reader, dictionary, postings) = fixture(IndexRecordOption::Basic)?;
        let pending_file = if pause_dictionary {
            &dictionary
        } else {
            &postings
        };
        pending_file.set_behavior(ReadBehavior::Pending);
        let mut future =
            Box::pin(reader.create_bitset_for_automaton(Regex::new(".*").unwrap(), 264, &executor));
        futures::executor::block_on(async {
            assert!(futures::poll!(&mut future).is_pending());
        });
        assert!(pending_file
            .state
            .lock()
            .unwrap()
            .buffers
            .iter()
            .any(|buffer| buffer.strong_count() > 0));
        drop(future);
        dictionary.assert_released();
        postings.assert_released();
    }
    Ok(())
}

#[test]
fn owned_bitset_errors() -> crate::Result<()> {
    let executor = Executor::single_thread();
    for behavior in [ReadBehavior::Short, ReadBehavior::Extra] {
        let (reader, _, postings) = fixture(IndexRecordOption::Basic)?;
        postings.set_behavior(behavior);
        assert_eq!(
            read_bitset(&reader, ".*", 264, &executor)
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidData
        );
        postings.assert_released();
    }
    for (fail_dictionary, behavior, expected_message) in [
        (true, ReadBehavior::FailOpen, Some("injected read failure")),
        (false, ReadBehavior::FailOpen, Some("injected read failure")),
        (false, ReadBehavior::FailBody, Some("injected body failure")),
        (false, ReadBehavior::Missing, None),
    ] {
        let (reader, dictionary, postings) = fixture(IndexRecordOption::Basic)?;
        let file = if fail_dictionary {
            &dictionary
        } else {
            &postings
        };
        file.set_behavior(behavior);
        let error = read_bitset(&reader, ".*", 264, &executor).unwrap_err();
        if let Some(message) = expected_message {
            assert_eq!(error.kind(), io::ErrorKind::Other);
            assert_eq!(error.to_string(), message);
        } else {
            assert_eq!(error.kind(), io::ErrorKind::UnexpectedEof);
        }
        dictionary.assert_released();
        postings.assert_released();
    }
    Ok(())
}
