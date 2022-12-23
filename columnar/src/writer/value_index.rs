use fastfield_codecs::serialize::{MultiValueIndexInfo, SingleValueIndexInfo};

use crate::DocId;

/// The `IndexBuilder` interprets a sequence of
/// calls of the form:
/// (record_doc,record_value+)*
/// and can then serialize the results into an index.
///
/// It has different implementation depending on whether the
/// cardinality is required, optional, or multivalued.
pub(crate) trait IndexBuilder {
    fn record_doc(&mut self, doc: DocId);
    #[inline]
    fn record_value(&mut self) {}
}

/// The RequiredIndexBuilder does nothing.
#[derive(Default)]
pub struct RequiredIndexBuilder;

impl IndexBuilder for RequiredIndexBuilder {
    #[inline(always)]
    fn record_doc(&mut self, _doc: DocId) {}
}

#[derive(Default)]
pub struct OptionalIndexBuilder {
    docs: Vec<DocId>,
}

struct SingleValueArrayIndex<'a> {
    docs: &'a [DocId],
    num_docs: DocId,
}

impl<'a> SingleValueIndexInfo for SingleValueArrayIndex<'a> {
    fn num_vals(&self) -> u32 {
        self.num_docs as u32
    }

    fn num_non_nulls(&self) -> u32 {
        self.docs.len() as u32
    }

    fn iter(&self) -> Box<dyn Iterator<Item = u32> + '_> {
        Box::new(self.docs.iter().copied())
    }
}

impl OptionalIndexBuilder {
    pub fn finish(&mut self, num_docs: DocId) -> impl SingleValueIndexInfo + '_ {
        debug_assert!(self
            .docs
            .last()
            .copied()
            .map(|last_doc| last_doc < num_docs)
            .unwrap_or(true));
        SingleValueArrayIndex {
            docs: &self.docs[..],
            num_docs,
        }
    }

    fn reset(&mut self) {
        self.docs.clear();
    }
}

impl IndexBuilder for OptionalIndexBuilder {
    #[inline(always)]
    fn record_doc(&mut self, doc: DocId) {
        debug_assert!(self
            .docs
            .last()
            .copied()
            .map(|prev_doc| doc > prev_doc)
            .unwrap_or(true));
        self.docs.push(doc);
    }
}

#[derive(Default)]
pub struct MultivaluedIndexBuilder {
    // TODO should we switch to `start_offset`?
    end_values: Vec<DocId>,
    total_num_vals_seen: u32,
}

pub struct MultivaluedValueArrayIndex<'a> {
    end_offsets: &'a [DocId],
}

impl<'a> MultiValueIndexInfo for MultivaluedValueArrayIndex<'a> {
    fn num_docs(&self) -> u32 {
        self.end_offsets.len() as u32
    }

    fn num_vals(&self) -> u32 {
        self.end_offsets.last().copied().unwrap_or(0u32)
    }

    fn iter(&self) -> Box<dyn Iterator<Item = u32> + '_> {
        if self.end_offsets.is_empty() {
            return Box::new(std::iter::empty());
        }
        let n = self.end_offsets.len();
        Box::new(std::iter::once(0u32).chain(self.end_offsets[..n - 1].iter().copied()))
    }
}

impl MultivaluedIndexBuilder {
    pub fn finish(&mut self, num_docs: DocId) -> impl MultiValueIndexInfo + '_ {
        self.end_values
            .resize(num_docs as usize, self.total_num_vals_seen);
        MultivaluedValueArrayIndex {
            end_offsets: &self.end_values[..],
        }
    }

    fn reset(&mut self) {
        self.end_values.clear();
        self.total_num_vals_seen = 0;
    }
}

impl IndexBuilder for MultivaluedIndexBuilder {
    fn record_doc(&mut self, doc: DocId) {
        self.end_values
            .resize(doc as usize, self.total_num_vals_seen);
    }

    fn record_value(&mut self) {
        self.total_num_vals_seen += 1;
    }
}

/// The `SpareIndexBuilders` is there to avoid allocating a
/// new index builder for every single column.
#[derive(Default)]
pub struct SpareIndexBuilders {
    required_index_builder: RequiredIndexBuilder,
    optional_index_builder: OptionalIndexBuilder,
    multivalued_index_builder: MultivaluedIndexBuilder,
}

impl SpareIndexBuilders {
    pub fn borrow_required_index_builder(&mut self) -> &mut RequiredIndexBuilder {
        &mut self.required_index_builder
    }

    pub fn borrow_optional_index_builder(&mut self) -> &mut OptionalIndexBuilder {
        self.optional_index_builder.reset();
        &mut self.optional_index_builder
    }

    pub fn borrow_multivalued_index_builder(&mut self) -> &mut MultivaluedIndexBuilder {
        self.multivalued_index_builder.reset();
        &mut self.multivalued_index_builder
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_optional_value_index_builder() {
        let mut opt_value_index_builder = OptionalIndexBuilder::default();
        opt_value_index_builder.record_doc(0u32);
        opt_value_index_builder.record_value();
        assert_eq!(
            &opt_value_index_builder
                .finish(1u32)
                .iter()
                .collect::<Vec<u32>>(),
            &[0]
        );
        opt_value_index_builder.reset();
        opt_value_index_builder.record_doc(1u32);
        opt_value_index_builder.record_value();
        assert_eq!(
            &opt_value_index_builder
                .finish(2u32)
                .iter()
                .collect::<Vec<u32>>(),
            &[1]
        );
    }

    #[test]
    fn test_multivalued_value_index_builder() {
        let mut multivalued_value_index_builder = MultivaluedIndexBuilder::default();
        multivalued_value_index_builder.record_doc(1u32);
        multivalued_value_index_builder.record_value();
        multivalued_value_index_builder.record_value();
        multivalued_value_index_builder.record_doc(2u32);
        multivalued_value_index_builder.record_value();
        assert_eq!(
            multivalued_value_index_builder
                .finish(4u32)
                .iter()
                .collect::<Vec<u32>>(),
            vec![0, 0, 2, 3]
        );
        multivalued_value_index_builder.reset();
        multivalued_value_index_builder.record_doc(2u32);
        multivalued_value_index_builder.record_value();
        multivalued_value_index_builder.record_value();
        assert_eq!(
            multivalued_value_index_builder
                .finish(4u32)
                .iter()
                .collect::<Vec<u32>>(),
            vec![0, 0, 0, 2]
        );
    }
}
