use std::collections::{BinaryHeap, HashMap};

use crate::{
    query::{BooleanQuery, BoostQuery, Occur, Query, TermQuery},
    schema::{Field, FieldType, FieldValue, IndexRecordOption, Term, Value},
    tokenizer::{BoxTokenStream, FacetTokenizer, PreTokenizedStream, Tokenizer},
    DocAddress, Result, Searcher, TantivyError,
};

#[derive(Debug, PartialEq)]
struct ScoreTerm {
    pub term: Term,
    pub score: f32,
}

impl ScoreTerm {
    fn new(term: Term, score: f32) -> Self {
        Self { term, score }
    }
}

impl Eq for ScoreTerm {}

impl PartialOrd for ScoreTerm {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.score.partial_cmp(&other.score)
    }
}

impl Ord for ScoreTerm {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.partial_cmp(other).unwrap_or(std::cmp::Ordering::Equal)
    }
}

/// A struct used as helper to build [`MoreLikeThisQuery`]
#[derive(Debug, Clone)]
pub struct MoreLikeThis {
    /// Ignore words which do not occur in at least this many docs.
    pub min_doc_frequency: Option<u64>,
    /// Ignore words which occur in more than this many docs.
    pub max_doc_frequency: Option<u64>,
    /// Ignore words less frequent than this.
    pub min_term_frequency: Option<usize>,
    /// Don't return a query longer than this.
    pub max_query_terms: Option<usize>,
    /// Ignore words if less than this length.
    pub min_word_length: Option<usize>,
    /// Ignore words if greater than this length.
    pub max_word_length: Option<usize>,
    /// Boost factor to use when boosting the terms
    pub boost_factor: Option<f32>,
    /// Current set of stop words.
    pub stop_words: Vec<String>,
}

impl Default for MoreLikeThis {
    fn default() -> Self {
        Self {
            min_doc_frequency: Some(5),
            max_doc_frequency: None,
            min_term_frequency: Some(2),
            max_query_terms: Some(25),
            min_word_length: None,
            max_word_length: None,
            boost_factor: Some(1.0),
            stop_words: vec![],
        }
    }
}

impl MoreLikeThis {
    /// Creates a [`BooleanQuery`] using a document address to collect
    /// the top stored field values.
    pub fn query_with_document(
        &self,
        searcher: &Searcher,
        doc_address: DocAddress,
    ) -> Result<BooleanQuery> {
        let score_terms = self.retrieve_terms_from_doc_address(searcher, doc_address)?;
        let query = self.create_query(score_terms);
        Ok(query)
    }

    /// Creates a [`BooleanQuery`] using a set of field values.
    pub fn query_with_document_fields(
        &self,
        searcher: &Searcher,
        doc_fields: &[(Field, Vec<FieldValue>)],
    ) -> Result<BooleanQuery> {
        let score_terms = self.retrieve_terms_from_doc_fields(searcher, doc_fields)?;
        let query = self.create_query(score_terms);
        Ok(query)
    }

    /// Creates a [`BooleanQuery`] from an ascendingly sorted list of ScoreTerm
    /// This will map the list of ScoreTerm to a list of [`TermQuery`]  and compose a 
    /// BooleanQuery using that list as sub queries. 
    fn create_query(&self, score_terms: Vec<ScoreTerm>) -> BooleanQuery {
        let best_score = score_terms.first().map_or(1f32, |x| x.score);
        let mut queries = Vec::new();

        for ScoreTerm { term, score } in score_terms {
            let mut query: Box<dyn Query> =
                Box::new(TermQuery::new(term, IndexRecordOption::Basic));
            if let Some(factor) = self.boost_factor {
                query = Box::new(BoostQuery::new(query, score * factor / best_score));
            }
            queries.push((Occur::Should, query));
        }
        BooleanQuery::from(queries)
    }

    /// Finds terms for a more-like-this query.
    /// doc_address is the address of document from which to find terms.
    fn retrieve_terms_from_doc_address(
        &self,
        searcher: &Searcher,
        doc_address: DocAddress,
    ) -> Result<Vec<ScoreTerm>> {
        let doc = searcher.doc(doc_address)?;
        let field_to_field_values = doc
            .get_sorted_field_values()
            .iter()
            .map(|(field, values)| {
                (
                    *field,
                    values
                        .iter()
                        .map(|v| (**v).clone())
                        .collect::<Vec<FieldValue>>(),
                )
            })
            .collect::<Vec<_>>();
        self.retrieve_terms_from_doc_fields(searcher, &field_to_field_values)
    }

    /// Finds terms for a more-like-this query.
    /// field_to_field_values is a mapping from field to possible values of taht field.
    fn retrieve_terms_from_doc_fields(
        &self,
        searcher: &Searcher,
        field_to_field_values: &[(Field, Vec<FieldValue>)],
    ) -> Result<Vec<ScoreTerm>> {
        if field_to_field_values.is_empty() {
            return Err(TantivyError::InvalidArgument("Cannot create more like this query on empty field values. The document may not have stored fields".to_string()));
        }

        let mut field_to_term_freq_map = HashMap::new();
        for (field, field_values) in field_to_field_values {
            self.add_term_frequencies(searcher, *field, field_values, &mut field_to_term_freq_map)?;
        }
        self.create_score_term(searcher, field_to_term_freq_map)
    }

    /// Computes the frequency of values for a field while updating the term frequencies 
    /// Note: A FieldValue can be made up of multiple terms. 
    /// We are interested in extracting terms within FieldValue 
    fn add_term_frequencies(
        &self,
        searcher: &Searcher,
        field: Field,
        field_values: &[FieldValue],
        term_frequencies: &mut HashMap<Term, usize>,
    ) -> Result<()> {
        let schema = searcher.schema();
        let tokenizer_manager = searcher.index().tokenizers();

        let field_entry = schema.get_field_entry(field);
        if !field_entry.is_indexed() {
            return Ok(());
        }

        // extract the raw value, possibly tokenizing & filtering to update the term frequency map  
        match field_entry.field_type() {
            FieldType::HierarchicalFacet(_) => {
                let facets: Vec<&str> = field_values
                    .iter()
                    .map(|field_value| match *field_value.value() {
                        Value::Facet(ref facet) => Ok(facet.encoded_str()),
                        _ => Err(TantivyError::InvalidArgument(
                            "invalid field value".to_string(),
                        )),
                    })
                    .collect::<Result<Vec<_>>>()?;
                for fake_str in facets {
                    FacetTokenizer.token_stream(fake_str).process(&mut |token| {
                        if self.is_noise_word(token.text.clone()) {
                            let term = Term::from_field_text(field, &token.text);
                            *term_frequencies.entry(term).or_insert(0) += 1;
                        }
                    });
                }
            }
            FieldType::Str(text_options) => {
                let mut token_streams: Vec<BoxTokenStream> = vec![];
                let mut offsets = vec![];
                let mut total_offset = 0;

                for field_value in field_values {
                    match field_value.value() {
                        Value::PreTokStr(tok_str) => {
                            offsets.push(total_offset);
                            if let Some(last_token) = tok_str.tokens.last() {
                                total_offset += last_token.offset_to;
                            }
                            token_streams.push(PreTokenizedStream::from(tok_str.clone()).into());
                        }
                        Value::Str(ref text) => {
                            if let Some(tokenizer) = text_options
                                .get_indexing_options()
                                .map(|text_indexing_options| {
                                    text_indexing_options.tokenizer().to_string()
                                })
                                .and_then(|tokenizer_name| tokenizer_manager.get(&tokenizer_name))
                            {
                                offsets.push(total_offset);
                                total_offset += text.len();
                                //let v = text.clone();
                                token_streams.push(tokenizer.token_stream(text));
                            }
                        }
                        _ => (),
                    }
                }

                for mut token_stream in token_streams {
                    token_stream.process(&mut |token| {
                        if !self.is_noise_word(token.text.clone()) {
                            let term = Term::from_field_text(field, &token.text);
                            *term_frequencies.entry(term).or_insert(0) += 1;
                        }
                    });
                }
            }
            FieldType::U64(_) => {
                for field_value in field_values {
                    let val = field_value
                        .value()
                        .u64_value()
                        .ok_or(TantivyError::InvalidArgument("invalid value".to_string()))?;
                    if !self.is_noise_word(val.to_string()) {
                        let term = Term::from_field_u64(field, val);
                        *term_frequencies.entry(term).or_insert(0) += 1;
                    }
                }
            }
            FieldType::Date(_) => {
                for field_value in field_values {
                    // TODO: Ask if this is the semantic (timestamp) we want
                    let val = field_value
                        .value()
                        .date_value()
                        .ok_or(TantivyError::InvalidArgument("invalid value".to_string()))?
                        .timestamp();
                    if !self.is_noise_word(val.to_string()) {
                        let term = Term::from_field_i64(field, val);
                        *term_frequencies.entry(term).or_insert(0) += 1;
                    }
                }
            }
            FieldType::I64(_) => {
                for field_value in field_values {
                    let val = field_value
                        .value()
                        .i64_value()
                        .ok_or(TantivyError::InvalidArgument("invalid value".to_string()))?;
                    if !self.is_noise_word(val.to_string()) {
                        let term = Term::from_field_i64(field, val);
                        *term_frequencies.entry(term).or_insert(0) += 1;
                    }
                }
            }
            FieldType::F64(_) => {
                for field_value in field_values {
                    let val = field_value
                        .value()
                        .f64_value()
                        .ok_or(TantivyError::InvalidArgument("invalid value".to_string()))?;
                    if !self.is_noise_word(val.to_string()) {
                        let term = Term::from_field_f64(field, val);
                        *term_frequencies.entry(term).or_insert(0) += 1;
                    }
                }
            }
            _ => {}
        }
        Ok(())
    }

    /// Determines if the term is likely to be of interest based on "more-like-this" settings
    fn is_noise_word(&self, word: String) -> bool {
        let word_length = word.len();
        if word_length == 0 {
            return true;
        }
        if self
            .min_word_length
            .map(|min| word_length < min)
            .unwrap_or(false)
        {
            return true;
        }
        if self
            .max_word_length
            .map(|max| word_length > max)
            .unwrap_or(false)
        {
            return true;
        }
        return self.stop_words.contains(&word);
    }

    /// Couputes the score for each term while ignoring not useful terms
    fn create_score_term(
        &self,
        searcher: &Searcher,
        per_field_term_frequencies: HashMap<Term, usize>,
    ) -> Result<Vec<ScoreTerm>> {
        let mut score_terms = BinaryHeap::new();
        let num_docs = searcher
            .segment_readers()
            .iter()
            .map(|x| x.num_docs() as u64)
            .sum::<u64>();

        for (term, term_frequency) in per_field_term_frequencies.into_iter() {
             // ignore terms with less than min_term_frequency
            if self
                .min_term_frequency
                .map(|x| term_frequency < x)
                .unwrap_or(false)
            {
                continue;
            }

            let doc_freq = searcher.doc_freq(&term)?;

            // ignore terms with less than min_doc_frequency
            if self
                .min_doc_frequency
                .map(|x| doc_freq < x)
                .unwrap_or(false)
            {
                continue;
            }

            // ignore terms with more than max_doc_frequency
            if self
                .max_doc_frequency
                .map(|x| doc_freq > x)
                .unwrap_or(false)
            {
                continue;
            }

            // ignore terms with zero frequency
            if doc_freq == 0 {
                continue;
            }

            // compute similarity & score 
            let idf = self.idf(doc_freq, num_docs);
            let score = (term_frequency as f32) * idf;
            score_terms.push(ScoreTerm::new(term, score));
        }

        // limit ourself to max_query terms. we need to sort so to avoid discarding important terms
        let score_terms = if let Some(max_query_terms) = self.max_query_terms {
            let max_num_terms = std::cmp::min(max_query_terms, score_terms.len());
            score_terms
                .into_sorted_vec()
                .into_iter()
                .take(max_num_terms)
                .collect()
        } else {
            score_terms.into_sorted_vec()
        };
        Ok(score_terms)
    }

    /// Computes the similarity
    fn idf(&self, doc_freq: u64, doc_count: u64) -> f32 {
        let x = ((doc_count - doc_freq) as f32 + 0.5) / (doc_freq as f32 + 0.5);
        (1f32 + x).ln()
    }
}
