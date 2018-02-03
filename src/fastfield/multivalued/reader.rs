use DocId;
use fastfield::FastFieldReader;

use fastfield::U64FastFieldReader;

/// Reader for a multivalued `u64` fast field.
///
/// The reader is implemented as two `u64` fast field.
///
/// The `vals_reader` will access the concatenated list of all
/// values for all reader.
/// The `idx_reader` associated, for each document, the index of its first value.
///
#[derive(Clone)]
pub struct MultiValueIntFastFieldReader {
    idx_reader: U64FastFieldReader,
    vals_reader: U64FastFieldReader,
}

impl MultiValueIntFastFieldReader {
    pub(crate) fn open(
        idx_reader: U64FastFieldReader,
        vals_reader: U64FastFieldReader,
    ) -> MultiValueIntFastFieldReader {
        MultiValueIntFastFieldReader {
            idx_reader: idx_reader,
            vals_reader: vals_reader,
        }
    }

    /// Returns the array of values associated to the given `doc`.
    pub fn get_vals(&self, doc: DocId, vals: &mut Vec<u64>) {
        let start = self.idx_reader.get(doc) as u32;
        let stop = self.idx_reader.get(doc + 1) as u32;
        vals.clear();
        for val_id in start..stop {
            let val = self.vals_reader.get(val_id);
            vals.push(val);
        }
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
            assert_eq!(&vals[..], &[3, 2]);
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
