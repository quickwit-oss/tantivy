mod block_wand;
mod boolean_query;
mod boolean_weight;

pub(crate) use self::block_wand::block_wand;
pub use self::boolean_query::BooleanQuery;

#[cfg(test)]
mod tests {

    use super::*;
    use crate::assert_nearly_equals;
    use crate::collector::tests::TEST_COLLECTOR_WITH_SCORE;
    use crate::collector::TopDocs;
    use crate::query::score_combiner::SumWithCoordsCombiner;
    use crate::query::term_query::TermScorer;
    use crate::query::Intersection;
    use crate::query::Occur;
    use crate::query::Query;
    use crate::query::QueryParser;
    use crate::query::RequiredOptionalScorer;
    use crate::query::Scorer;
    use crate::query::TermQuery;
    use crate::schema::*;
    use crate::Index;
    use crate::{DocAddress, DocId, Score};

    fn aux_test_helper() -> (Index, Field) {
        let mut schema_builder = Schema::builder();
        let text_field = schema_builder.add_text_field("text", TEXT);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        {
            // writing the segment
            let mut index_writer = index.writer_with_num_threads(1, 3_000_000).unwrap();
            {
                index_writer.add_document(doc!(text_field => "a b c"));
                index_writer.add_document(doc!(text_field => "a c"));
                index_writer.add_document(doc!(text_field => "b c"));
                index_writer.add_document(doc!(text_field => "a b c d"));
                index_writer.add_document(doc!(text_field => "d"));
            }
            assert!(index_writer.commit().is_ok());
        }
        (index, text_field)
    }

    #[test]
    pub fn test_boolean_non_all_term_disjunction() {
        let (index, text_field) = aux_test_helper();
        let query_parser = QueryParser::for_index(&index, vec![text_field]);
        let query = query_parser.parse_query("(+a +b) d").unwrap();
        let searcher = index.reader().unwrap().searcher();
        assert_eq!(query.count(&searcher).unwrap(), 3);
    }

    #[test]
    pub fn test_boolean_single_must_clause() {
        let (index, text_field) = aux_test_helper();
        let query_parser = QueryParser::for_index(&index, vec![text_field]);
        let query = query_parser.parse_query("+a").unwrap();
        let searcher = index.reader().unwrap().searcher();
        let weight = query.weight(&searcher, true).unwrap();
        let scorer = weight
            .scorer(searcher.segment_reader(0u32), 1.0f32)
            .unwrap();
        assert!(scorer.is::<TermScorer>());
    }

    #[test]
    pub fn test_boolean_termonly_intersection() {
        let (index, text_field) = aux_test_helper();
        let query_parser = QueryParser::for_index(&index, vec![text_field]);
        let searcher = index.reader().unwrap().searcher();
        {
            let query = query_parser.parse_query("+a +b +c").unwrap();
            let weight = query.weight(&searcher, true).unwrap();
            let scorer = weight
                .scorer(searcher.segment_reader(0u32), 1.0f32)
                .unwrap();
            assert!(scorer.is::<Intersection<TermScorer>>());
        }
        {
            let query = query_parser.parse_query("+a +(b c)").unwrap();
            let weight = query.weight(&searcher, true).unwrap();
            let scorer = weight
                .scorer(searcher.segment_reader(0u32), 1.0f32)
                .unwrap();
            assert!(scorer.is::<Intersection<Box<dyn Scorer>>>());
        }
    }

    #[test]
    pub fn test_boolean_reqopt() {
        let (index, text_field) = aux_test_helper();
        let query_parser = QueryParser::for_index(&index, vec![text_field]);
        let searcher = index.reader().unwrap().searcher();
        {
            let query = query_parser.parse_query("+a b").unwrap();
            let weight = query.weight(&searcher, true).unwrap();
            let scorer = weight
                .scorer(searcher.segment_reader(0u32), 1.0f32)
                .unwrap();
            assert!(scorer.is::<RequiredOptionalScorer<
                Box<dyn Scorer>,
                Box<dyn Scorer>,
                SumWithCoordsCombiner,
            >>());
        }
        {
            let query = query_parser.parse_query("+a b").unwrap();
            let weight = query.weight(&searcher, false).unwrap();
            let scorer = weight
                .scorer(searcher.segment_reader(0u32), 1.0f32)
                .unwrap();
            assert!(scorer.is::<TermScorer>());
        }
    }

    #[test]
    pub fn test_boolean_query() {
        let (index, text_field) = aux_test_helper();

        let make_term_query = |text: &str| {
            let term_query = TermQuery::new(
                Term::from_field_text(text_field, text),
                IndexRecordOption::Basic,
            );
            let query: Box<dyn Query> = Box::new(term_query);
            query
        };

        let reader = index.reader().unwrap();

        let matching_docs = |boolean_query: &dyn Query| {
            reader
                .searcher()
                .search(boolean_query, &TEST_COLLECTOR_WITH_SCORE)
                .unwrap()
                .docs()
                .iter()
                .cloned()
                .map(|doc| doc.1)
                .collect::<Vec<DocId>>()
        };
        {
            let boolean_query = BooleanQuery::from(vec![(Occur::Must, make_term_query("a"))]);
            assert_eq!(matching_docs(&boolean_query), vec![0, 1, 3]);
        }
        {
            let boolean_query = BooleanQuery::from(vec![(Occur::Should, make_term_query("a"))]);
            assert_eq!(matching_docs(&boolean_query), vec![0, 1, 3]);
        }
        {
            let boolean_query = BooleanQuery::from(vec![
                (Occur::Should, make_term_query("a")),
                (Occur::Should, make_term_query("b")),
            ]);
            assert_eq!(matching_docs(&boolean_query), vec![0, 1, 2, 3]);
        }
        {
            let boolean_query = BooleanQuery::from(vec![
                (Occur::Must, make_term_query("a")),
                (Occur::Should, make_term_query("b")),
            ]);
            assert_eq!(matching_docs(&boolean_query), vec![0, 1, 3]);
        }
        {
            let boolean_query = BooleanQuery::from(vec![
                (Occur::Must, make_term_query("a")),
                (Occur::Should, make_term_query("b")),
                (Occur::MustNot, make_term_query("d")),
            ]);
            assert_eq!(matching_docs(&boolean_query), vec![0, 1]);
        }
        {
            let boolean_query = BooleanQuery::from(vec![(Occur::MustNot, make_term_query("d"))]);
            assert_eq!(matching_docs(&boolean_query), Vec::<u32>::new());
        }
    }

    #[test]
    pub fn test_boolean_query_two_excluded() {
        let (index, text_field) = aux_test_helper();

        let make_term_query = |text: &str| {
            let term_query = TermQuery::new(
                Term::from_field_text(text_field, text),
                IndexRecordOption::Basic,
            );
            let query: Box<dyn Query> = Box::new(term_query);
            query
        };

        let reader = index.reader().unwrap();

        let matching_topdocs = |query: &dyn Query| {
            reader
                .searcher()
                .search(query, &TopDocs::with_limit(3))
                .unwrap()
        };

        let score_doc_4: Score; // score of doc 4 should not be influenced by exclusion
        {
            let boolean_query_no_excluded =
                BooleanQuery::from(vec![(Occur::Must, make_term_query("d"))]);
            let topdocs_no_excluded = matching_topdocs(&boolean_query_no_excluded);
            assert_eq!(topdocs_no_excluded.len(), 2);
            let (top_score, top_doc) = topdocs_no_excluded[0];
            assert_eq!(top_doc, DocAddress(0, 4));
            assert_eq!(topdocs_no_excluded[1].1, DocAddress(0, 3)); // ignore score of doc 3.
            score_doc_4 = top_score;
        }

        {
            let boolean_query_two_excluded = BooleanQuery::from(vec![
                (Occur::Must, make_term_query("d")),
                (Occur::MustNot, make_term_query("a")),
                (Occur::MustNot, make_term_query("b")),
            ]);
            let topdocs_excluded = matching_topdocs(&boolean_query_two_excluded);
            assert_eq!(topdocs_excluded.len(), 1);
            let (top_score, top_doc) = topdocs_excluded[0];
            assert_eq!(top_doc, DocAddress(0, 4));
            assert_eq!(top_score, score_doc_4);
        }
    }

    #[test]
    pub fn test_boolean_query_with_weight() {
        let mut schema_builder = Schema::builder();
        let text_field = schema_builder.add_text_field("text", TEXT);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        {
            let mut index_writer = index.writer_with_num_threads(1, 3_000_000).unwrap();
            index_writer.add_document(doc!(text_field => "a b c"));
            index_writer.add_document(doc!(text_field => "a c"));
            index_writer.add_document(doc!(text_field => "b c"));
            assert!(index_writer.commit().is_ok());
        }
        let term_a: Box<dyn Query> = Box::new(TermQuery::new(
            Term::from_field_text(text_field, "a"),
            IndexRecordOption::WithFreqs,
        ));
        let term_b: Box<dyn Query> = Box::new(TermQuery::new(
            Term::from_field_text(text_field, "b"),
            IndexRecordOption::WithFreqs,
        ));
        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        let boolean_query =
            BooleanQuery::from(vec![(Occur::Should, term_a), (Occur::Should, term_b)]);
        let boolean_weight = boolean_query.weight(&searcher, true).unwrap();
        {
            let mut boolean_scorer = boolean_weight
                .scorer(searcher.segment_reader(0u32), 1.0f32)
                .unwrap();
            assert_eq!(boolean_scorer.doc(), 0u32);
            assert_nearly_equals!(boolean_scorer.score(), 0.84163445f32);
        }
        {
            let mut boolean_scorer = boolean_weight
                .scorer(searcher.segment_reader(0u32), 2.0f32)
                .unwrap();
            assert_eq!(boolean_scorer.doc(), 0u32);
            assert_nearly_equals!(boolean_scorer.score(), 1.6832689f32);
        }
    }

    #[test]
    pub fn test_intersection_score() {
        let (index, text_field) = aux_test_helper();

        let make_term_query = |text: &str| {
            let term_query = TermQuery::new(
                Term::from_field_text(text_field, text),
                IndexRecordOption::Basic,
            );
            let query: Box<dyn Query> = Box::new(term_query);
            query
        };
        let reader = index.reader().unwrap();
        let score_docs = |boolean_query: &dyn Query| {
            let fruit = reader
                .searcher()
                .search(boolean_query, &TEST_COLLECTOR_WITH_SCORE)
                .unwrap();
            fruit.scores().to_vec()
        };

        {
            let boolean_query = BooleanQuery::from(vec![
                (Occur::Must, make_term_query("a")),
                (Occur::Must, make_term_query("b")),
            ]);
            assert_eq!(score_docs(&boolean_query), vec![0.977973, 0.84699446]);
        }
    }

    // motivated by #554
    #[test]
    fn test_bm25_several_fields() {
        let mut schema_builder = Schema::builder();
        let title = schema_builder.add_text_field("title", TEXT);
        let text = schema_builder.add_text_field("text", TEXT);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);

        let mut index_writer = index.writer_with_num_threads(1, 3_000_000).unwrap();
        index_writer.add_document(doc!(
            // tf = 1 0
            title =>  "Законы притяжения   Оксана Кулакова",
            // tf = 1 0
            text => "Законы притяжения Оксана Кулакова]  \n\nТема: Сексуальное искусство, Женственность\nТип товара: Запись вебинара (аудио)\nПродолжительность: 1,5 часа\n\nСсылка на вебинар:\n  ",
        ));
        index_writer.add_document(doc!(
            // tf = 1 0
            title => "Любимые русские пироги (Оксана Путан)",
            // tf = 2 0
            text => "http://i95.fastpic.ru/big/2017/0628/9a/615b9c8504d94a3893d7f496ac53539a.jpg \n\nОт издателя\nОксана Путан   профессиональный повар, автор кулинарных книг и известный кулинарный блогер. Ее рецепты отличаются практичностью, доступностью и пользуются огромной популярностью в русскоязычном интернете. Это третья книга автора о самом вкусном и ароматном   настоящих русских пирогах и выпечке!\nДаже новички на кухне легко готовят по ее рецептам. Оксана описывает процесс приготовления настолько подробно и понятно, что вам остается только наслаждаться готовкой и не тратить время на лишние усилия. Готовьте легко и просто!\n\nhttps://www.ozon.ru/context/detail/id/139872462/"
        ));
        index_writer.add_document(doc!(
            // tf = 1 1
            title =>  "PDF Мастер Класс \"Морячок\" (Оксана Лифенко)",
            // tf = 0 0
            text => "https://i.ibb.co/pzvHrDN/I3d U T6 Gg TM.jpg\nhttps://i.ibb.co/NFrb6v6/N0ls Z9nwjb U.jpg\nВ описание входит штаны, кофта, берет, матросский воротник. Описание продается в формате PDF, состоит из 12 страниц формата А4 и может быть напечатано на любом принтере.\nОписание предназначено для кукол BJD RealPuki от FairyLand, но может подойти и другим подобным куклам. Также вы можете вязать этот наряд из обычной пряжи, и он подойдет для куколок побольше.\nhttps://vk.com/market 95724412?w=product 95724412_2212"
        ));
        for _ in 0..1_000 {
            index_writer.add_document(doc!(
                title =>  "a b d e f g",
                text => "maitre corbeau sur un arbre perche tenait dans son bec un fromage Maitre rnard par lodeur alleche lui tint a peu pres ce langage."
            ));
        }
        index_writer.commit().unwrap();
        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        let query_parser = QueryParser::for_index(&index, vec![title, text]);
        let query = query_parser.parse_query("Оксана Лифенко").unwrap();
        let weight = query.weight(&searcher, true).unwrap();
        let mut scorer = weight
            .scorer(searcher.segment_reader(0u32), 1.0f32)
            .unwrap();
        scorer.advance();

        let explanation = query.explain(&searcher, DocAddress(0u32, 0u32)).unwrap();
        assert_eq!(
            explanation.to_pretty_json(),
            r#"{
  "value": 12.997711,
  "description": "BooleanClause. Sum of ...",
  "details": [
    {
      "value": 12.997711,
      "description": "BooleanClause. Sum of ...",
      "details": [
        {
          "value": 6.551476,
          "description": "TermQuery, product of...",
          "details": [
            {
              "value": 2.2,
              "description": "(K1+1)"
            },
            {
              "value": 5.658984,
              "description": "idf, computed as log(1 + (N - n + 0.5) / (n + 0.5))",
              "details": [
                {
                  "value": 3.0,
                  "description": "n, number of docs containing this term"
                },
                {
                  "value": 1003.0,
                  "description": "N, total number of docs"
                }
              ]
            },
            {
              "value": 0.5262329,
              "description": "freq / (freq + k1 * (1 - b + b * dl / avgdl))",
              "details": [
                {
                  "value": 1.0,
                  "description": "freq, occurrences of term within document"
                },
                {
                  "value": 1.2,
                  "description": "k1, term saturation parameter"
                },
                {
                  "value": 0.75,
                  "description": "b, length normalization parameter"
                },
                {
                  "value": 4.0,
                  "description": "dl, length of field"
                },
                {
                  "value": 5.997009,
                  "description": "avgdl, average length of field"
                }
              ]
            }
          ]
        },
        {
          "value": 6.446235,
          "description": "TermQuery, product of...",
          "details": [
            {
              "value": 2.2,
              "description": "(K1+1)"
            },
            {
              "value": 5.9954567,
              "description": "idf, computed as log(1 + (N - n + 0.5) / (n + 0.5))",
              "details": [
                {
                  "value": 2.0,
                  "description": "n, number of docs containing this term"
                },
                {
                  "value": 1003.0,
                  "description": "N, total number of docs"
                }
              ]
            },
            {
              "value": 0.4887212,
              "description": "freq / (freq + k1 * (1 - b + b * dl / avgdl))",
              "details": [
                {
                  "value": 1.0,
                  "description": "freq, occurrences of term within document"
                },
                {
                  "value": 1.2,
                  "description": "k1, term saturation parameter"
                },
                {
                  "value": 0.75,
                  "description": "b, length normalization parameter"
                },
                {
                  "value": 20.0,
                  "description": "dl, length of field"
                },
                {
                  "value": 24.123629,
                  "description": "avgdl, average length of field"
                }
              ]
            }
          ]
        }
      ]
    }
  ]
}"#
        );
    }
}
