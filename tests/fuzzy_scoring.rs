#[cfg(test)]
mod test {
    use maplit::hashmap;
    use tantivy::collector::TopDocs;
    use tantivy::query::FuzzyTermQuery;
    use tantivy::schema::{Schema, Value};
    use tantivy::schema::{STORED, TEXT};
    use tantivy::Index;
    use tantivy::Term;
    use tantivy::{doc, TantivyDocument};

    #[test]
    pub fn test_fuzzy_term() {
        // Define a list of documents to be indexed.  Each entry represents a text
        // that will be associated with the field "country" in the index.
        let docs = vec![
            "WENN ROT WIE RUBIN",
            "WENN ROT WIE ROBIN",
            "WHEN RED LIKE ROBIN",
            "WENN RED AS ROBIN",
            "WHEN ROYAL BLUE ROBIN",
            "IF RED LIKE RUBEN",
            "WHEN GREEN LIKE ROBIN",
            "WENN ROSE LIKE ROBIN",
            "IF PINK LIKE ROBIN",
            "WENN ROT WIE RABIN",
            "WENN BLU WIE ROBIN",
            "WHEN YELLOW LIKE RABBIT",
            "IF BLUE LIKE ROBIN",
            "WHEN ORANGE LIKE RIBBON",
            "WENN VIOLET WIE RUBIX",
            "WHEN INDIGO LIKE ROBBIE",
            "IF TEAL LIKE RUBY",
            "WHEN GOLD LIKE ROB",
            "WENN SILVER WIE ROBY",
            "IF BRONZE LIKE ROBE",
        ];

        // Define the expected scores when queried with "robin" and a fuzziness of 2.
        // This map associates each document text with its expected score.
        let expected_scores = hashmap! {
            "WHEN GREEN LIKE ROBIN" => 1.0,
            "WENN RED AS ROBIN" => 1.0,
            "WHEN RED LIKE ROBIN" => 1.0,
            "WENN ROSE LIKE ROBIN" => 1.0,
            "WENN ROT WIE ROBIN" => 1.0,
            "WHEN ROYAL BLUE ROBIN" => 1.0,
            "IF PINK LIKE ROBIN" => 1.0,
            "IF BLUE LIKE ROBIN" => 1.0,
            "WENN BLU WIE ROBIN" => 1.0,
            "WENN ROT WIE RUBIN" => 0.5,
            "WENN ROT WIE RABIN" => 0.5,
            "IF RED LIKE RUBEN" => 0.33333334,
            "WENN VIOLET WIE RUBIX" => 0.33333334,
            "IF BRONZE LIKE ROBE" => 0.33333334,
            "WENN SILVER WIE ROBY" => 0.33333334,
            "WHEN GOLD LIKE ROB" => 0.33333334,
            "WHEN INDIGO LIKE ROBBIE" => 0.33333334,
        };

        // Build a schema for the index.
        // The schema determines how documents are indexed and searched.
        let mut schema_builder = Schema::builder();

        // Add a text field named "country" to the schema. This field will store the text and
        // is indexed in a way that makes it searchable.
        let country_field = schema_builder.add_text_field("country", TEXT | STORED);
        // Build the schema based on the provided definitions.
        let schema = schema_builder.build();
        // Create a new index in RAM based on the defined schema.
        let index = Index::create_in_ram(schema);
        {
            // Create an index writer with one thread and a certain memory limit.
            // The writer allows us to add documents to the index.
            let mut index_writer = index.writer_with_num_threads(1, 15_000_000).unwrap();

            // Index each document in the docs list.
            for &doc in &docs {
                index_writer
                    .add_document(doc!(country_field => doc))
                    .unwrap();
            }

            // Commit changes to the index. This finalizes the addition of documents.
            index_writer.commit().unwrap();
        }

        // Create a reader for the index to search the indexed documents.
        let reader = index.reader().unwrap();
        let searcher = reader.searcher();

        {
            // Define a term based on the field "country" and the text "robin".
            let term = Term::from_field_text(country_field, "robin");

            // Create a fuzzy query for "robin", a fuzziness of 2, and a prefix length of 0.
            let fuzzy_query = FuzzyTermQuery::new(term, 2, true);

            // Search the index with the fuzzy query and retrieve up to 100 top documents.
            let top_docs = searcher
                .search(&fuzzy_query, &TopDocs::with_limit(100))
                .unwrap();

            // Print out the scores and documents retrieved by the search.
            for (score, adr) in &top_docs {
                let doc: TantivyDocument = searcher.doc(*adr).expect("document");
                println!(
                    "{score}, {:?}",
                    doc.field_values().next().unwrap().1.as_str()
                );
            }

            // Assert that 17 documents match the fuzzy query criteria.
            // We don't expect anything that has a larger fuzziness than 2
            // to be returned in the query, leaving us with 17 expected results.
            assert_eq!(top_docs.len(), 17, "Expected 17 documents");

            // Check the scores of the returned documents against the expected scores.
            for (score, adr) in &top_docs {
                let doc: TantivyDocument = searcher.doc(*adr).expect("document");
                let doc_text = doc.field_values().next().unwrap().1.as_str().unwrap();

                // Ensure the retrieved score for each document is close to the expected score.
                assert!(
                    (score - expected_scores[doc_text]).abs() < f32::EPSILON,
                    "Unexpected score for document {}. Expected: {}, Actual: {}",
                    doc_text,
                    expected_scores[doc_text],
                    score
                );
            }
        }
    }
}
