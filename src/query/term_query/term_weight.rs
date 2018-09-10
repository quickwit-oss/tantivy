use super::term_scorer::TermScorer;
use core::SegmentReader;
use docset::DocSet;
use postings::SegmentPostings;
use query::bm25::BM25Weight;
use query::Scorer;
use query::Weight;
use schema::IndexRecordOption;
use Result;
use Term;

pub struct TermWeight {
    term: Term,
    index_record_option: IndexRecordOption,
    similarity_weight: BM25Weight,
}

impl Weight for TermWeight {
    fn scorer(&self, reader: &SegmentReader) -> Result<Box<Scorer>> {
        let field = self.term.field();
        let inverted_index = reader.inverted_index(field);
        let fieldnorm_reader = reader.get_fieldnorms_reader(field);
        let similarity_weight = self.similarity_weight.clone();
        let postings_opt: Option<SegmentPostings> =
            inverted_index.read_postings(&self.term, self.index_record_option);
        if let Some(segment_postings) = postings_opt {
            Ok(Box::new(TermScorer::new(
                segment_postings,
                fieldnorm_reader,
                similarity_weight,
            )))
        } else {
            Ok(Box::new(TermScorer::new(
                SegmentPostings::empty(),
                fieldnorm_reader,
                similarity_weight,
            )))
        }
    }

    fn count(&self, reader: &SegmentReader) -> Result<u32> {
        if reader.num_deleted_docs() == 0 {
            let field = self.term.field();
            Ok(reader
                .inverted_index(field)
                .get_term_info(&self.term)
                .map(|term_info| term_info.doc_freq)
                .unwrap_or(0))
        } else {
            Ok(self.scorer(reader)?.count())
        }
    }
}

impl TermWeight {
    pub fn new(
        term: Term,
        index_record_option: IndexRecordOption,
        similarity_weight: BM25Weight,
    ) -> TermWeight {
        TermWeight {
            term,
            index_record_option,
            similarity_weight,
        }
    }
}
