use fastfield::{FastFieldReader, FastValue};
use DocId;

/// Reader for a multivalued `u64` fast field.
///
/// The reader is implemented as two `u64` fast field.
///
/// The `vals_reader` will access the concatenated list of all
/// values for all reader.
/// The `idx_reader` associated, for each document, the index of its first value.
///
#[derive(Clone)]
pub struct MultiValueIntFastFieldReader<Item: FastValue> {
    idx_reader: FastFieldReader<u64>,
    vals_reader: FastFieldReader<Item>,
}

impl<Item: FastValue> MultiValueIntFastFieldReader<Item> {
    pub(crate) fn open(
        idx_reader: FastFieldReader<u64>,
        vals_reader: FastFieldReader<Item>,
    ) -> MultiValueIntFastFieldReader<Item> {
        MultiValueIntFastFieldReader {
            idx_reader,
            vals_reader,
        }
    }

    /// Returns `(start, stop)`, such that the values associated
    /// to the given document are `start..stop`.
    fn range(&self, doc: DocId) -> (u64, u64) {
        let start = self.idx_reader.get(doc);
        let stop = self.idx_reader.get(doc + 1);
        (start, stop)
    }

    /// Returns the array of values associated to the given `doc`.
    pub fn get_vals(&self, doc: DocId, vals: &mut Vec<Item>) {
        let (start, stop) = self.range(doc);
        let len = (stop - start) as usize;
        vals.resize(len, Item::default());
        self.vals_reader.get_range(start as u32, &mut vals[..]);
    }
}

#[cfg(test)]
mod tests {

    use core::Index;
    use schema::{Document, Facet, SchemaBuilder};

    #[test]
    fn test_multifastfield_reader() {
        let mut schema_builder = SchemaBuilder::new();
        let facet_field = schema_builder.add_facet_field("facets");
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut index_writer = index
            .writer_with_num_threads(1, 30_000_000)
            .expect("Failed to create index writer.");
        {
            let mut doc = Document::new();
            doc.add_facet(facet_field, "/category/cat2");
            doc.add_facet(facet_field, "/category/cat1");
            index_writer.add_document(doc);
        }
        {
            let mut doc = Document::new();
            doc.add_facet(facet_field, "/category/cat2");
            index_writer.add_document(doc);
        }
        {
            let mut doc = Document::new();
            doc.add_facet(facet_field, "/category/cat3");
            index_writer.add_document(doc);
        }
        index_writer.commit().expect("Commit failed");
        index.load_searchers().expect("Reloading searchers");
        let searcher = index.searcher();
        let segment_reader = searcher.segment_reader(0);
        let mut facet_reader = segment_reader.facet_reader(facet_field).unwrap();

        let mut facet = Facet::root();
        {
            facet_reader.facet_from_ord(1, &mut facet);
            assert_eq!(facet, Facet::from("/category"));
        }
        {
            facet_reader.facet_from_ord(2, &mut facet);
            assert_eq!(facet, Facet::from("/category/cat1"));
        }
        {
            facet_reader.facet_from_ord(3, &mut facet);
            assert_eq!(format!("{}", facet), "/category/cat2");
            assert_eq!(facet, Facet::from("/category/cat2"));
        }
        {
            facet_reader.facet_from_ord(4, &mut facet);
            assert_eq!(facet, Facet::from("/category/cat3"));
        }

        let mut vals = Vec::new();
        {
            facet_reader.facet_ords(0, &mut vals);
            assert_eq!(&vals[..], &[2, 3]);
        }
        {
            facet_reader.facet_ords(1, &mut vals);
            assert_eq!(&vals[..], &[3]);
        }
        {
            facet_reader.facet_ords(2, &mut vals);
            assert_eq!(&vals[..], &[4]);
        }
    }
}
