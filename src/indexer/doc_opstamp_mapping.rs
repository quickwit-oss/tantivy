use crate::DocId;
use crate::Opstamp;

// Doc to opstamp is used to identify which
// document should be deleted.
//
// Since the docset matching the query of a delete operation
// is not computed right when the delete operation is received,
// we need to find a way to evaluate, for each document,
// whether the document was added before or after
// the delete operation. This anteriority is used by comparing
// the docstamp of the document.
//
// The doc to opstamp mapping stores precisely an array
// indexed by doc id and storing the opstamp of the document.
//
// This mapping is (for the moment) stricly increasing
// because of the way document id are allocated.
#[derive(Clone)]
pub enum DocToOpstampMapping<'a> {
    WithMap(&'a [Opstamp]),
    None,
}

impl<'a> From<&'a [u64]> for DocToOpstampMapping<'a> {
    fn from(opstamps: &[Opstamp]) -> DocToOpstampMapping {
        DocToOpstampMapping::WithMap(opstamps)
    }
}

impl<'a> DocToOpstampMapping<'a> {
    /// Given an opstamp return the limit doc id L
    /// such that all doc id D such that
    // D >= L iff opstamp(D) >= than `target_opstamp`.
    //
    // The edge case opstamp = some doc opstamp is in practise
    // never called.
    pub fn compute_doc_limit(&self, target_opstamp: Opstamp) -> DocId {
        match *self {
            DocToOpstampMapping::WithMap(ref doc_opstamps) => {
                match doc_opstamps.binary_search(&target_opstamp) {
                    Ok(doc_id) | Err(doc_id) => doc_id as DocId,
                }
            }
            DocToOpstampMapping::None => DocId::max_value(),
        }
    }
}

#[cfg(test)]
mod tests {

    use super::DocToOpstampMapping;

    #[test]
    fn test_doc_to_opstamp_mapping_none() {
        let doc_to_opstamp_mapping = DocToOpstampMapping::None;
        assert_eq!(
            doc_to_opstamp_mapping.compute_doc_limit(1),
            u32::max_value()
        );
    }

    #[test]
    fn test_doc_to_opstamp_mapping_complex() {
        {
            let doc_to_opstamp_mapping = DocToOpstampMapping::from(&[][..]);
            assert_eq!(doc_to_opstamp_mapping.compute_doc_limit(0u64), 0);
            assert_eq!(doc_to_opstamp_mapping.compute_doc_limit(2u64), 0);
        }
        {
            let doc_to_opstamp_mapping = DocToOpstampMapping::from(&[1u64][..]);
            assert_eq!(doc_to_opstamp_mapping.compute_doc_limit(0u64), 0);
            assert_eq!(doc_to_opstamp_mapping.compute_doc_limit(2u64), 1);
        }
        {
            let doc_to_opstamp_mapping =
                DocToOpstampMapping::from(&[1u64, 12u64, 17u64, 23u64][..]);
            assert_eq!(doc_to_opstamp_mapping.compute_doc_limit(0u64), 0);
            for i in 2u64..13u64 {
                assert_eq!(doc_to_opstamp_mapping.compute_doc_limit(i), 1);
            }
            for i in 13u64..18u64 {
                assert_eq!(doc_to_opstamp_mapping.compute_doc_limit(i), 2);
            }
            for i in 18u64..24u64 {
                assert_eq!(doc_to_opstamp_mapping.compute_doc_limit(i), 3);
            }
            for i in 24u64..30u64 {
                assert_eq!(doc_to_opstamp_mapping.compute_doc_limit(i), 4);
            }
        }
    }
}
