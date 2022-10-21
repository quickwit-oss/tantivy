//! IP Fastfields support efficient scanning for range queries.
//! We use this variant only if the fastfield exists, otherwise the default in `range_query` is
//! used, which uses the term dictionary + postings.

use std::net::Ipv6Addr;
use std::ops::{Bound, RangeInclusive};
use std::sync::Arc;

use common::BinarySerializable;
use fastfield_codecs::{Column, MonotonicallyMappableToU128};

use super::range_query::map_bound;
use super::{ConstScorer, Explanation, Scorer, Weight};
use crate::schema::{Cardinality, Field};
use crate::{DocId, DocSet, Score, SegmentReader, TantivyError, TERMINATED};

/// `IPFastFieldRangeWeight` uses the ip address fast field to execute range queries.
pub struct IPFastFieldRangeWeight {
    field: Field,
    left_bound: Bound<Ipv6Addr>,
    right_bound: Bound<Ipv6Addr>,
}

impl IPFastFieldRangeWeight {
    pub fn new(field: Field, left_bound: &Bound<Vec<u8>>, right_bound: &Bound<Vec<u8>>) -> Self {
        let ip_from_bound_raw_data = |data: &Vec<u8>| {
            let left_ip_u128: u128 =
                u128::from_be(BinarySerializable::deserialize(&mut &data[..]).unwrap());
            Ipv6Addr::from_u128(left_ip_u128)
        };
        let left_bound = map_bound(left_bound, &ip_from_bound_raw_data);
        let right_bound = map_bound(right_bound, &ip_from_bound_raw_data);
        Self {
            field,
            left_bound,
            right_bound,
        }
    }
}

impl Weight for IPFastFieldRangeWeight {
    fn scorer(&self, reader: &SegmentReader, boost: Score) -> crate::Result<Box<dyn Scorer>> {
        let field_type = reader.schema().get_field_entry(self.field).field_type();
        match field_type.fastfield_cardinality().unwrap() {
            Cardinality::SingleValue => {
                let ip_addr_fast_field = reader.fast_fields().ip_addr(self.field)?;
                let value_range =
                    bound_to_value_range(&self.left_bound, &self.right_bound, &ip_addr_fast_field);
                let docset = IpRangeDocSet::new(value_range, ip_addr_fast_field);
                Ok(Box::new(ConstScorer::new(docset, boost)))
            }
            Cardinality::MultiValues => unimplemented!(),
        }
    }

    fn explain(&self, reader: &SegmentReader, doc: DocId) -> crate::Result<Explanation> {
        let mut scorer = self.scorer(reader, 1.0)?;
        if scorer.seek(doc) != doc {
            return Err(TantivyError::InvalidArgument(format!(
                "Document #({}) does not match",
                doc
            )));
        }
        let explanation = Explanation::new("Const", scorer.score());

        Ok(explanation)
    }
}

fn bound_to_value_range(
    left_bound: &Bound<Ipv6Addr>,
    right_bound: &Bound<Ipv6Addr>,
    column: &dyn Column<Ipv6Addr>,
) -> RangeInclusive<Ipv6Addr> {
    let start_value = match left_bound {
        Bound::Included(ip_addr) => *ip_addr,
        Bound::Excluded(ip_addr) => Ipv6Addr::from(ip_addr.to_u128() + 1),
        Bound::Unbounded => column.min_value(),
    };

    let end_value = match right_bound {
        Bound::Included(ip_addr) => *ip_addr,
        Bound::Excluded(ip_addr) => Ipv6Addr::from(ip_addr.to_u128() - 1),
        Bound::Unbounded => column.max_value(),
    };
    start_value..=end_value
}

/// Helper to have a cursor over a vec
struct VecCursor {
    data: Vec<u32>,
    pos_in_data: usize,
}
impl VecCursor {
    fn new() -> Self {
        Self {
            data: Vec::with_capacity(32),
            pos_in_data: 0,
        }
    }
    fn next(&mut self) -> Option<u32> {
        self.pos_in_data += 1;
        self.current()
    }
    #[inline]
    fn current(&self) -> Option<u32> {
        self.data.get(self.pos_in_data).map(|el| *el as u32)
    }

    fn set_data(&mut self, data: Vec<u32>) {
        self.data = data;
        self.pos_in_data = 0;
    }
    fn is_empty(&self) -> bool {
        self.pos_in_data >= self.data.len()
    }
}

struct IpRangeDocSet {
    value_range: RangeInclusive<Ipv6Addr>,
    ip_addr_fast_field: Arc<dyn Column<Ipv6Addr>>,
    fetched_until_doc: u32,
    loaded_docs: VecCursor,
}
impl IpRangeDocSet {
    fn new(
        value_range: RangeInclusive<Ipv6Addr>,
        ip_addr_fast_field: Arc<dyn Column<Ipv6Addr>>,
    ) -> Self {
        let mut ip_range_docset = Self {
            value_range,
            ip_addr_fast_field,
            loaded_docs: VecCursor::new(),
            fetched_until_doc: 0,
        };
        ip_range_docset.fetch_block();
        ip_range_docset
    }

    /// Returns true if more data could be fetched
    fn fetch_block(&mut self) {
        let mut horizon: u32 = 1;
        const MAX_HORIZON: u32 = 100_000;
        while self.loaded_docs.is_empty() {
            let finished_to_end = self.fetch_horizon(horizon);
            if finished_to_end {
                break;
            }
            // Fetch more data, increase horizon
            horizon = (horizon * 2).min(MAX_HORIZON);
        }
    }

    /// Fetches a block for docid range [fetched_until_doc .. fetched_until_doc + HORIZON]
    fn fetch_horizon(&mut self, horizon: u32) -> bool {
        let mut end = self.fetched_until_doc + horizon;
        let mut finished_to_end = false;

        let limit = self.ip_addr_fast_field.num_vals();
        if end >= limit {
            end = limit;
            finished_to_end = true;
        }

        let data = self
            .ip_addr_fast_field
            .get_positions_for_value_range(self.value_range.clone(), self.fetched_until_doc..end);
        self.loaded_docs.set_data(data);
        self.fetched_until_doc = end;
        finished_to_end
    }
}

impl DocSet for IpRangeDocSet {
    fn advance(&mut self) -> DocId {
        if let Some(docid) = self.loaded_docs.next() {
            docid as u32
        } else {
            if self.fetched_until_doc >= self.ip_addr_fast_field.num_vals() as u32 {
                return TERMINATED;
            }
            self.fetch_block();
            self.loaded_docs.current().unwrap_or(TERMINATED)
        }
    }

    fn doc(&self) -> DocId {
        self.loaded_docs
            .current()
            .map(|el| el as u32)
            .unwrap_or(TERMINATED)
    }

    fn size_hint(&self) -> u32 {
        0 // heuristic possible by checking number of hits when fetching a block
    }
}
