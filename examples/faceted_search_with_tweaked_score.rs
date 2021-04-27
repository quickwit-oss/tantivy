use std::collections::HashSet;
use tantivy::collector::TopDocs;
use tantivy::doc;
use tantivy::query::BooleanQuery;
use tantivy::schema::*;
use tantivy::{DocId, Index, Score, SegmentReader};

fn main() -> tantivy::Result<()> {
    let mut schema_builder = Schema::builder();

    let title = schema_builder.add_text_field("title", STORED);
    let ingredient = schema_builder.add_facet_field("ingredient", INDEXED);

    let schema = schema_builder.build();
    let index = Index::create_in_ram(schema.clone());

    let mut index_writer = index.writer(30_000_000)?;

    index_writer.add_document(doc!(
        title => "Fried egg",
        ingredient => Facet::from("/ingredient/egg"),
        ingredient => Facet::from("/ingredient/oil"),
    ));
    index_writer.add_document(doc!(
        title => "Scrambled egg",
        ingredient => Facet::from("/ingredient/egg"),
        ingredient => Facet::from("/ingredient/butter"),
        ingredient => Facet::from("/ingredient/milk"),
        ingredient => Facet::from("/ingredient/salt"),
    ));
    index_writer.add_document(doc!(
        title => "Egg rolls",
        ingredient => Facet::from("/ingredient/egg"),
        ingredient => Facet::from("/ingredient/garlic"),
        ingredient => Facet::from("/ingredient/salt"),
        ingredient => Facet::from("/ingredient/oil"),
        ingredient => Facet::from("/ingredient/tortilla-wrap"),
        ingredient => Facet::from("/ingredient/mushroom"),
    ));
    index_writer.commit()?;

    let reader = index.reader()?;
    let searcher = reader.searcher();
    {
        let facets = vec![
            Facet::from("/ingredient/egg"),
            Facet::from("/ingredient/oil"),
            Facet::from("/ingredient/garlic"),
            Facet::from("/ingredient/mushroom"),
        ];
        let query = BooleanQuery::new_multiterms_query(
            facets
                .iter()
                .map(|key| Term::from_facet(ingredient, &key))
                .collect(),
        );
        let top_docs_by_custom_score =
            TopDocs::with_limit(2).tweak_score(move |segment_reader: &SegmentReader| {
                let ingredient_reader = segment_reader.facet_reader(ingredient).unwrap();
                let facet_dict = ingredient_reader.facet_dict();

                let query_ords: HashSet<u64> = facets
                    .iter()
                    .filter_map(|key| facet_dict.term_ord(key.encoded_str()).unwrap())
                    .collect();

                let mut facet_ords_buffer: Vec<u64> = Vec::with_capacity(20);

                move |doc: DocId, original_score: Score| {
                    ingredient_reader.facet_ords(doc, &mut facet_ords_buffer);
                    let missing_ingredients = facet_ords_buffer
                        .iter()
                        .filter(|ord| !query_ords.contains(ord))
                        .count();
                    let tweak = 1.0 / 4_f32.powi(missing_ingredients as i32);

                    original_score * tweak
                }
            });
        let top_docs = searcher.search(&query, &top_docs_by_custom_score)?;

        let titles: Vec<String> = top_docs
            .iter()
            .map(|(_, doc_id)| {
                searcher
                    .doc(*doc_id)
                    .unwrap()
                    .get_first(title)
                    .unwrap()
                    .text()
                    .unwrap()
                    .to_owned()
            })
            .collect();
        assert_eq!(titles, vec!["Fried egg", "Egg rolls"]);
    }
    Ok(())
}
