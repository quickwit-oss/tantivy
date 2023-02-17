use crate::iterable::Iterable;
use crate::RowId;

/// The `IndexBuilder` interprets a sequence of
/// calls of the form:
/// (record_doc,record_value+)*
/// and can then serialize the results into an index to associate docids with their value[s].
///
/// It has different implementation depending on whether the
/// cardinality is required, optional, or multivalued.
pub(crate) trait IndexBuilder {
    fn record_row(&mut self, doc: RowId);
    #[inline]
    fn record_value(&mut self) {}
}

/// The FullIndexBuilder does nothing.
#[derive(Default)]
pub struct FullIndexBuilder;

impl IndexBuilder for FullIndexBuilder {
    #[inline(always)]
    fn record_row(&mut self, _doc: RowId) {}
}

#[derive(Default)]
pub struct OptionalIndexBuilder {
    docs: Vec<RowId>,
}

impl OptionalIndexBuilder {
    pub fn finish(&mut self, num_rows: RowId) -> impl Iterable<RowId> + '_ {
        debug_assert!(self
            .docs
            .last()
            .copied()
            .map(|last_doc| last_doc < num_rows)
            .unwrap_or(true));
        &self.docs[..]
    }

    fn reset(&mut self) {
        self.docs.clear();
    }
}

impl IndexBuilder for OptionalIndexBuilder {
    #[inline(always)]
    fn record_row(&mut self, doc: RowId) {
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
    start_offsets: Vec<RowId>,
    total_num_vals_seen: u32,
}

impl MultivaluedIndexBuilder {
    pub fn finish(&mut self, num_docs: RowId) -> &[u32] {
        self.start_offsets
            .resize(num_docs as usize + 1, self.total_num_vals_seen);
        &self.start_offsets[..]
    }

    fn reset(&mut self) {
        self.start_offsets.clear();
        self.start_offsets.push(0u32);
        self.total_num_vals_seen = 0;
    }
}

impl IndexBuilder for MultivaluedIndexBuilder {
    fn record_row(&mut self, row_id: RowId) {
        self.start_offsets
            .resize(row_id as usize + 1, self.total_num_vals_seen);
    }

    fn record_value(&mut self) {
        self.total_num_vals_seen += 1;
    }
}

/// The `SpareIndexBuilders` is there to avoid allocating a
/// new index builder for every single column.
#[derive(Default)]
pub struct PreallocatedIndexBuilders {
    required_index_builder: FullIndexBuilder,
    optional_index_builder: OptionalIndexBuilder,
    multivalued_index_builder: MultivaluedIndexBuilder,
}

impl PreallocatedIndexBuilders {
    pub fn borrow_required_index_builder(&mut self) -> &mut FullIndexBuilder {
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
        opt_value_index_builder.record_row(0u32);
        opt_value_index_builder.record_value();
        assert_eq!(
            &opt_value_index_builder
                .finish(1u32)
                .boxed_iter()
                .collect::<Vec<u32>>(),
            &[0]
        );
        opt_value_index_builder.reset();
        opt_value_index_builder.record_row(1u32);
        opt_value_index_builder.record_value();
        assert_eq!(
            &opt_value_index_builder
                .finish(2u32)
                .boxed_iter()
                .collect::<Vec<u32>>(),
            &[1]
        );
    }

    #[test]
    fn test_multivalued_value_index_builder() {
        let mut multivalued_value_index_builder = MultivaluedIndexBuilder::default();
        multivalued_value_index_builder.record_row(1u32);
        multivalued_value_index_builder.record_value();
        multivalued_value_index_builder.record_value();
        multivalued_value_index_builder.record_row(2u32);
        multivalued_value_index_builder.record_value();
        assert_eq!(
            multivalued_value_index_builder.finish(4u32).to_vec(),
            vec![0, 0, 2, 3, 3]
        );
        multivalued_value_index_builder.reset();
        multivalued_value_index_builder.record_row(2u32);
        multivalued_value_index_builder.record_value();
        multivalued_value_index_builder.record_value();
        assert_eq!(
            multivalued_value_index_builder.finish(4u32).to_vec(),
            vec![0, 0, 0, 2, 2]
        );
    }
}
