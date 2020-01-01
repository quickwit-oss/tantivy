use crate::directory::{WatchCallbackList, WatchHandle};
use crate::indexer::SegmentRegisters;
use crate::reader::pool::Pool;
use crate::{Index, LeasedItem, Searcher, Segment, SegmentReader};
use std::iter::repeat_with;
use std::sync::{Arc, RwLock, Weak};

struct InnerNRTReader {
    num_searchers: usize,
    index: Index,
    searcher_pool: Pool<Searcher>,
    segment_registers: Arc<RwLock<SegmentRegisters>>,
}

impl InnerNRTReader {
    fn load_segment_readers(&self) -> crate::Result<Vec<SegmentReader>> {
        let segments: Vec<Segment> = {
            let rlock = self.segment_registers.read().unwrap();
            rlock.committed_segment()
        };
        segments
            .iter()
            .map(SegmentReader::open)
            .collect::<crate::Result<Vec<SegmentReader>>>()
    }

    pub fn reload(&self) -> crate::Result<()> {
        let segment_readers: Vec<SegmentReader> = self.load_segment_readers()?;
        let schema = self.index.schema();
        let searchers = repeat_with(|| {
            Searcher::new(schema.clone(), self.index.clone(), segment_readers.clone())
        })
        .take(self.num_searchers)
        .collect();
        self.searcher_pool.publish_new_generation(searchers);
        Ok(())
    }

    pub fn searcher(&self) -> LeasedItem<Searcher> {
        self.searcher_pool.acquire()
    }
}

#[derive(Clone)]
pub struct NRTReader {
    inner: Arc<InnerNRTReader>,
    watch_handle: WatchHandle,
}

impl NRTReader {
    pub fn reload(&self) -> crate::Result<()> {
        self.inner.reload()
    }

    pub fn searcher(&self) -> LeasedItem<Searcher> {
        self.inner.searcher()
    }

    pub(crate) fn create(
        num_searchers: usize,
        index: Index,
        segment_registers: Arc<RwLock<SegmentRegisters>>,
        watch_callback_list: &WatchCallbackList,
    ) -> crate::Result<Self> {
        let inner_reader: Arc<InnerNRTReader> = Arc::new(InnerNRTReader {
            num_searchers,
            index,
            searcher_pool: Pool::new(),
            segment_registers,
        });
        let inner_reader_weak: Weak<InnerNRTReader> = Arc::downgrade(&inner_reader);
        let watch_handle = watch_callback_list.subscribe(Box::new(move || {
            if let Some(nrt_reader_arc) = inner_reader_weak.upgrade() {
                let _ = nrt_reader_arc.reload();
            }
        }));
        inner_reader.reload()?;
        Ok(NRTReader {
            inner: inner_reader,
            watch_handle,
        })
    }
}
