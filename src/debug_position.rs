use std::panic;

use tantivy;
use tantivy::DocSet;
use tantivy::Postings;
use tantivy::Searcher;
use tantivy::TERMINATED;
use tantivy::schema::Field;
use tantivy::schema::IndexRecordOption;

fn test_field(searcher: &Searcher, field: Field) -> tantivy::Result<()> {
    for segment_reader in searcher.segment_readers() {
        println!("\n\n====\nsegment {:?}", segment_reader.segment_id());
        println!("maxdoc {} del {} ", segment_reader.max_doc(), segment_reader.num_deleted_docs());
        let inv_idx = segment_reader.inverted_index(field)?;
        let termdict = inv_idx.terms();
        println!("num terms {}", termdict.num_terms());
        let mut terms = termdict.stream()?;
        while terms.advance() {
            let term_info = terms.value();
            let mut postings = inv_idx.read_postings_from_terminfo(term_info, tantivy::schema::IndexRecordOption::WithFreqsAndPositions)?;
            let mut seen_doc = 0;
            while postings.doc() != TERMINATED {
                let mut postings_clone= postings.clone();
                // println!("termord {} seen_doc {} termpositions {:?} docfreq {}", terms.term_ord(), seen_doc, term_info.positions_range, term_info.doc_freq);
                let mut positions = Vec::new();
                postings_clone.positions(&mut positions);
                seen_doc += 1;
                postings.advance();
            }
        }
    }
    Ok(())
}

fn main() -> tantivy::Result<()> {
    let index = tantivy::Index::open_in_dir(".")?;
    let reader = index.reader()?;
    let searcher = reader.searcher();
    let schema = index.schema();
    for (field, field_entry) in schema.fields() {
        let field_type = field_entry.field_type();
        let has_position = field_type.get_index_record_option()
            .map(|opt| opt == IndexRecordOption::WithFreqsAndPositions)
            .unwrap_or(false);
        if !has_position {
            continue;
        }
        test_field(&*searcher, field)?;
    }

    Ok(())
}
