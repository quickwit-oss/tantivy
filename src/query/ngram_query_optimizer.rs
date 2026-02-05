/// Query optimization for leveraging word ngram indexes
///
/// This module provides query rewriting to use word ngram indexes when available,
/// significantly speeding up phrase queries by avoiding position matching.
use crate::core::searcher::Searcher;
use crate::indexer::{FrequentTermTracker, NgramType};
use crate::query::term_query::TermQuery;
use crate::query::{BooleanQuery, Occur, Query};
use crate::schema::{Field, FieldType, IndexRecordOption, Schema, Term};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

/// Optimizes phrase queries by rewriting them to use ngram terms when available
pub struct NgramQueryOptimizer {
    schema: Arc<Schema>,
}

impl NgramQueryOptimizer {
    /// Create a new optimizer for the given schema
    pub fn new(schema: Arc<Schema>) -> Self {
        Self { schema }
    }

    /// Try to optimize a phrase query using ngrams
    ///
    /// Returns Some(optimized_query) if optimization is possible, None otherwise
    pub fn optimize_phrase_query(
        &self,
        field: Field,
        terms: &[(usize, Term)],
        slop: u32,
        searcher_opt: Option<&Searcher>,
    ) -> Option<Box<dyn Query>> {
        // Don't optimize if slop is used - ngrams only work for exact phrases
        if slop > 0 {
            return None;
        }

        // Get field configuration
        let field_entry = self.schema.get_field_entry(field);
        let text_options = match field_entry.field_type() {
            FieldType::Str(options) => options,
            _ => return None,
        };

        let indexing_options = text_options.get_indexing_options()?;
        let ngram_config = indexing_options.word_ngrams()?;

        // Early return if ngrams are not enabled
        if !ngram_config.is_enabled() {
            return None;
        }

        // Check if we have positions indexed (fallback to regular phrase query if not)
        let has_positions = indexing_options.index_option().has_positions();
        if !has_positions {
            return None;
        }

        // Check if terms are consecutive (offsets differ by 1)
        let is_consecutive = terms.windows(2).all(|w| w[1].0 == w[0].0 + 1);
        if !is_consecutive {
            return None; // Skip phrases with gaps
        }

        // Try to get frequent terms info from searcher
        let frequent_tracker_opt = searcher_opt.and_then(|searcher| {
            searcher
                .segment_readers()
                .first()
                .and_then(|reader| reader.get_frequent_terms(field.field_id()))
        });

        // Only use ngram optimization if we have frequency data
        // Without it, we can't determine which ngrams were actually indexed
        if frequent_tracker_opt.is_none() {
            return None;
        }

        // Extract just the term strings
        let term_texts: Vec<String> = terms
            .iter()
            .map(|(_, term)| {
                String::from_utf8(term.serialized_value_bytes().to_vec()).unwrap_or_default()
            })
            .collect();

        // Pre-compute term hashes to avoid repeated hashing
        let term_hashes: Vec<u64> = term_texts.iter().map(|t| Self::hash_term(t)).collect();

        // Generate ngram queries based on configured ngram types and term frequencies
        // Pre-allocate with estimated capacity
        let estimated_capacity = if ngram_config.contains_bigrams() {
            term_texts.len().saturating_sub(1)
        } else {
            0
        } + if ngram_config.contains_trigrams() {
            term_texts.len().saturating_sub(2)
        } else {
            0
        };
        let mut ngram_queries = Vec::with_capacity(estimated_capacity);

        // Reusable buffer for ngram text construction
        let mut ngram_buffer = String::with_capacity(64);

        // Try to generate bigrams
        if ngram_config.contains_bigrams() && term_texts.len() >= 2 {
            for i in 0..term_texts.len() - 1 {
                let ngram_type = self.classify_bigram_with_hashes(
                    i,
                    &term_hashes,
                    frequent_tracker_opt.as_ref().map(|t| t.as_ref()),
                );

                // Check if this ngram type should be indexed
                if ngram_config.has_ngram_type(&ngram_type) {
                    // Use buffer to avoid allocation
                    ngram_buffer.clear();
                    ngram_buffer.push_str(&term_texts[i]);
                    ngram_buffer.push(' ');
                    ngram_buffer.push_str(&term_texts[i + 1]);

                    let ngram_term = Term::from_field_text(field, &ngram_buffer);
                    let term_query = TermQuery::new(ngram_term, IndexRecordOption::Basic);
                    ngram_queries.push((Occur::Must, Box::new(term_query) as Box<dyn Query>));
                }
            }
        }

        // Try to generate trigrams
        if ngram_config.contains_trigrams() && term_texts.len() >= 3 {
            for i in 0..term_texts.len() - 2 {
                let ngram_type = self.classify_trigram_with_hashes(
                    i,
                    &term_hashes,
                    frequent_tracker_opt.as_ref().map(|t| t.as_ref()),
                );

                if ngram_config.has_ngram_type(&ngram_type) {
                    // Use buffer to avoid allocation
                    ngram_buffer.clear();
                    ngram_buffer.push_str(&term_texts[i]);
                    ngram_buffer.push(' ');
                    ngram_buffer.push_str(&term_texts[i + 1]);
                    ngram_buffer.push(' ');
                    ngram_buffer.push_str(&term_texts[i + 2]);

                    let ngram_term = Term::from_field_text(field, &ngram_buffer);
                    let term_query = TermQuery::new(ngram_term, IndexRecordOption::Basic);
                    ngram_queries.push((Occur::Must, Box::new(term_query) as Box<dyn Query>));
                }
            }
        }

        // If we generated any ngram queries, return a boolean query combining them
        if !ngram_queries.is_empty() {
            Some(Box::new(BooleanQuery::new(ngram_queries)))
        } else {
            None
        }
    }

    /// Compute hash for a term string
    fn hash_term(term: &str) -> u64 {
        let mut hasher = DefaultHasher::new();
        term.hash(&mut hasher);
        hasher.finish()
    }

    /// Classify bigram based on actual term frequencies (optimized with pre-computed hashes)
    fn classify_bigram_with_hashes(
        &self,
        position: usize,
        term_hashes: &[u64],
        tracker: Option<&FrequentTermTracker>,
    ) -> NgramType {
        if let Some(tracker) = tracker {
            let first_frequent = tracker.is_frequent(term_hashes[position]);
            let second_frequent = tracker.is_frequent(term_hashes[position + 1]);

            match (first_frequent, second_frequent) {
                (true, true) => NgramType::NgramFF,
                (true, false) => NgramType::NgramFR,
                (false, true) => NgramType::NgramRF,
                (false, false) => {
                    return self.classify_bigram_position(position, term_hashes.len())
                }
            }
        } else {
            self.classify_bigram_position(position, term_hashes.len())
        }
    }

    /// Classify trigram based on actual term frequencies (optimized with pre-computed hashes)
    fn classify_trigram_with_hashes(
        &self,
        position: usize,
        term_hashes: &[u64],
        tracker: Option<&FrequentTermTracker>,
    ) -> NgramType {
        if let Some(tracker) = tracker {
            let first_frequent = tracker.is_frequent(term_hashes[position]);
            let second_frequent = tracker.is_frequent(term_hashes[position + 1]);
            let third_frequent = tracker.is_frequent(term_hashes[position + 2]);

            match (first_frequent, second_frequent, third_frequent) {
                (true, true, true) => NgramType::NgramFFF,
                (false, true, true) => NgramType::NgramRFF,
                (true, true, false) => NgramType::NgramFFR,
                (true, false, true) => NgramType::NgramFRF,
                _ => return self.classify_trigram_position(position, term_hashes.len()),
            }
        } else {
            self.classify_trigram_position(position, term_hashes.len())
        }
    }

    /// Classify bigram position to determine its type
    fn classify_bigram_position(&self, position: usize, total_terms: usize) -> NgramType {
        let is_first = position == 0;
        let is_last = position == total_terms - 2; // Last possible bigram

        match (is_first, is_last) {
            (true, true) => NgramType::NgramFF,   // Only one bigram
            (true, false) => NgramType::NgramFF,  // First bigram
            (false, true) => NgramType::NgramFR,  // Last bigram
            (false, false) => NgramType::NgramRF, // Middle bigram
        }
    }

    /// Classify trigram position to determine its type
    fn classify_trigram_position(&self, position: usize, total_terms: usize) -> NgramType {
        let is_first = position == 0;
        let is_last = position == total_terms - 3; // Last possible trigram

        match (is_first, is_last) {
            (true, true) => NgramType::NgramFFF,   // Only one trigram
            (true, false) => NgramType::NgramFFF,  // First trigram
            (false, true) => NgramType::NgramFFR,  // Last trigram
            (false, false) => NgramType::NgramFRF, // Middle trigram
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::indexer::WordNgramSet;
    use crate::WordNgramConfig;

    #[test]
    fn test_bigram_classification() {
        let schema = Arc::new(Schema::builder().build());
        let optimizer = NgramQueryOptimizer::new(schema);

        // Single bigram in 2-term phrase
        assert_eq!(optimizer.classify_bigram_position(0, 2), NgramType::NgramFF);

        // Three-term phrase
        assert_eq!(optimizer.classify_bigram_position(0, 3), NgramType::NgramFF);
        assert_eq!(optimizer.classify_bigram_position(1, 3), NgramType::NgramFR);

        // Four-term phrase
        assert_eq!(optimizer.classify_bigram_position(0, 4), NgramType::NgramFF);
        assert_eq!(optimizer.classify_bigram_position(1, 4), NgramType::NgramRF);
        assert_eq!(optimizer.classify_bigram_position(2, 4), NgramType::NgramFR);
    }

    #[test]
    fn test_trigram_classification() {
        let schema = Arc::new(Schema::builder().build());
        let optimizer = NgramQueryOptimizer::new(schema);

        // Single trigram in 3-term phrase
        assert_eq!(
            optimizer.classify_trigram_position(0, 3),
            NgramType::NgramFFF
        );

        // Four-term phrase
        assert_eq!(
            optimizer.classify_trigram_position(0, 4),
            NgramType::NgramFFF
        );
        assert_eq!(
            optimizer.classify_trigram_position(1, 4),
            NgramType::NgramFFR
        );

        // Five-term phrase
        assert_eq!(
            optimizer.classify_trigram_position(0, 5),
            NgramType::NgramFFF
        );
        assert_eq!(
            optimizer.classify_trigram_position(1, 5),
            NgramType::NgramFRF
        );
        assert_eq!(
            optimizer.classify_trigram_position(2, 5),
            NgramType::NgramFFR
        );
    }

    #[test]
    fn test_optimize_phrase_with_slop() {
        let mut schema_builder = Schema::builder();
        let ngram_config = WordNgramConfig::builder()
            .ngram_types(
                WordNgramSet::new()
                    .with_ngram_ff()
                    .with_ngram_fr()
                    .with_ngram_rf(),
            )
            .build();

        let text_field = schema_builder.add_text_field(
            "text",
            crate::schema::TextOptions::default().set_indexing_options(
                crate::schema::TextFieldIndexing::default()
                    .set_index_option(IndexRecordOption::WithFreqsAndPositions)
                    .set_word_ngrams(ngram_config),
            ),
        );
        let schema = Arc::new(schema_builder.build());
        let optimizer = NgramQueryOptimizer::new(schema);

        let terms = vec![
            (0, Term::from_field_text(text_field, "hello")),
            (1, Term::from_field_text(text_field, "world")),
        ];

        // Should not optimize with slop > 0
        let result = optimizer.optimize_phrase_query(text_field, &terms, 1, None);
        assert!(result.is_none());

        // Should not optimize without searcher (no frequency data available)
        let result = optimizer.optimize_phrase_query(text_field, &terms, 0, None);
        assert!(result.is_none());
    }
}
