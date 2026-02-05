use serde::{Deserialize, Serialize};

/// Configuration for word-level ngram indexing.
///
/// This enables frequency-based word ngram indexing where ngrams are created
/// based on whether individual terms are frequent or rare in the corpus.
/// This can significantly speed up phrase queries at the cost of increased index size.
///
/// Frequent terms are determined by analyzing the corpus during indexing.
/// A term is considered "frequent" if it appears in more than a certain percentage
/// of documents (configurable via `frequent_term_threshold`).
///
/// # Example
///
/// ```rust
/// use tantivy::indexer::WordNgramSet;
///
/// // Enable all bigram types
/// let config = WordNgramSet::new()
///     .with_ngram_ff()
///     .with_ngram_fr()
///     .with_ngram_rf();
///
/// // Enable only frequent-frequent bigrams and trigrams
/// let config = WordNgramSet::new()
///     .with_ngram_ff()
///     .with_ngram_fff();
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct WordNgramSet {
    bits: u8,
}

impl WordNgramSet {
    /// No ngrams - only single terms are indexed (default)
    pub const NONE: u8 = 0b00000000;
    /// Bigram: Frequent-Frequent
    /// Index bigrams where both words are frequent (e.g., "the world")
    pub const NGRAM_FF: u8 = 0b00000001;
    /// Bigram: Frequent-Rare
    /// Index bigrams where first word is frequent, second is rare (e.g., "the elephant")
    pub const NGRAM_FR: u8 = 0b00000010;
    /// Bigram: Rare-Frequent
    /// Index bigrams where first word is rare, second is frequent (e.g., "elephant the")
    pub const NGRAM_RF: u8 = 0b00000100;
    /// Trigram: Frequent-Frequent-Frequent
    /// Index trigrams where all three words are frequent (e.g., "in the world")
    pub const NGRAM_FFF: u8 = 0b00001000;
    /// Trigram: Rare-Frequent-Frequent
    /// Index trigrams where first is rare, others are frequent
    pub const NGRAM_RFF: u8 = 0b00010000;
    /// Trigram: Frequent-Frequent-Rare
    /// Index trigrams where last is rare, others are frequent
    pub const NGRAM_FFR: u8 = 0b00100000;
    /// Trigram: Frequent-Rare-Frequent
    /// Index trigrams where middle is rare, others are frequent
    pub const NGRAM_FRF: u8 = 0b01000000;

    /// Create a new empty ngram set
    pub const fn new() -> Self {
        WordNgramSet { bits: Self::NONE }
    }

    /// Create from bits value
    pub const fn from_bits(bits: u8) -> Self {
        WordNgramSet { bits }
    }

    /// Get the raw bits
    pub const fn bits(&self) -> u8 {
        self.bits
    }

    /// Check if a flag is set
    pub const fn contains(&self, flag: u8) -> bool {
        (self.bits & flag) != 0
    }

    /// Add a flag
    pub const fn with(self, flag: u8) -> Self {
        WordNgramSet {
            bits: self.bits | flag,
        }
    }

    /// Check if empty
    pub const fn is_empty(&self) -> bool {
        self.bits == 0
    }

    /// Builder method for NGRAM_FF
    pub const fn with_ngram_ff(self) -> Self {
        self.with(Self::NGRAM_FF)
    }

    /// Builder method for NGRAM_FR
    pub const fn with_ngram_fr(self) -> Self {
        self.with(Self::NGRAM_FR)
    }

    /// Builder method for NGRAM_RF
    pub const fn with_ngram_rf(self) -> Self {
        self.with(Self::NGRAM_RF)
    }

    /// Builder method for NGRAM_FFF
    pub const fn with_ngram_fff(self) -> Self {
        self.with(Self::NGRAM_FFF)
    }

    /// Builder method for NGRAM_RFF
    pub const fn with_ngram_rff(self) -> Self {
        self.with(Self::NGRAM_RFF)
    }

    /// Builder method for NGRAM_FFR
    pub const fn with_ngram_ffr(self) -> Self {
        self.with(Self::NGRAM_FFR)
    }

    /// Builder method for NGRAM_FRF
    pub const fn with_ngram_frf(self) -> Self {
        self.with(Self::NGRAM_FRF)
    }
}

impl Default for WordNgramSet {
    fn default() -> Self {
        WordNgramSet::new()
    }
}

impl std::ops::BitOr for WordNgramSet {
    type Output = Self;

    fn bitor(self, rhs: Self) -> Self::Output {
        WordNgramSet {
            bits: self.bits | rhs.bits,
        }
    }
}

/// Configuration for word-level ngram indexing
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WordNgramConfig {
    /// Which ngram patterns to index
    pub ngram_set: WordNgramSet,
    
    /// Threshold for determining if a term is "frequent"
    /// A term is frequent if it appears in more than this fraction of documents
    /// Default: 0.01 (1% of documents)
    #[serde(default = "default_frequent_threshold")]
    pub frequent_term_threshold: f32,
    
    /// Maximum number of frequent terms to track
    /// Default: 10,000
    #[serde(default = "default_max_frequent_terms")]
    pub max_frequent_terms: usize,
}

fn default_frequent_threshold() -> f32 {
    0.01
}

fn default_max_frequent_terms() -> usize {
    10_000
}

impl Default for WordNgramConfig {
    fn default() -> Self {
        WordNgramConfig {
            ngram_set: WordNgramSet::new(),
            frequent_term_threshold: default_frequent_threshold(),
            max_frequent_terms: default_max_frequent_terms(),
        }
    }
}

impl WordNgramConfig {
    /// Create a new word ngram configuration with specific flags
    pub fn new(bits: u8) -> Self {
        WordNgramConfig {
            ngram_set: WordNgramSet::from_bits(bits),
            ..Default::default()
        }
    }

    /// Create a new word ngram configuration with a WordNgramSet
    pub fn with_set(ngram_set: WordNgramSet) -> Self {
        WordNgramConfig {
            ngram_set,
            ..Default::default()
        }
    }
    
    /// Set the frequent term threshold
    pub fn with_frequent_threshold(mut self, threshold: f32) -> Self {
        self.frequent_term_threshold = threshold;
        self
    }
    
    /// Set the maximum number of frequent terms to track
    pub fn with_max_frequent_terms(mut self, max: usize) -> Self {
        self.max_frequent_terms = max;
        self
    }
    
    /// Returns true if any ngrams are enabled
    pub fn is_enabled(&self) -> bool {
        !self.ngram_set.is_empty()
    }
    
    /// Check if this configuration includes any bigrams
    pub fn contains_bigrams(&self) -> bool {
        self.ngram_set.contains(WordNgramSet::NGRAM_FF)
            || self.ngram_set.contains(WordNgramSet::NGRAM_FR)
            || self.ngram_set.contains(WordNgramSet::NGRAM_RF)
    }
    
    /// Check if this configuration includes any trigrams
    pub fn contains_trigrams(&self) -> bool {
        self.ngram_set.contains(WordNgramSet::NGRAM_FFF)
            || self.ngram_set.contains(WordNgramSet::NGRAM_RFF)
            || self.ngram_set.contains(WordNgramSet::NGRAM_FFR)
            || self.ngram_set.contains(WordNgramSet::NGRAM_FRF)
    }
    
    /// Check if a specific ngram type is enabled
    pub fn has_ngram_type(&self, ngram_type: &NgramType) -> bool {
        match ngram_type {
            NgramType::SingleTerm => true, // Always enabled
            NgramType::NgramFF => self.ngram_set.contains(WordNgramSet::NGRAM_FF),
            NgramType::NgramFR => self.ngram_set.contains(WordNgramSet::NGRAM_FR),
            NgramType::NgramRF => self.ngram_set.contains(WordNgramSet::NGRAM_RF),
            NgramType::NgramFFF => self.ngram_set.contains(WordNgramSet::NGRAM_FFF),
            NgramType::NgramRFF => self.ngram_set.contains(WordNgramSet::NGRAM_RFF),
            NgramType::NgramFFR => self.ngram_set.contains(WordNgramSet::NGRAM_FFR),
            NgramType::NgramFRF => self.ngram_set.contains(WordNgramSet::NGRAM_FRF),
        }
    }
    
    /// Create a builder for WordNgramConfig
    pub fn builder() -> WordNgramConfigBuilder {
        WordNgramConfigBuilder::default()
    }
}

/// Builder for WordNgramConfig
#[derive(Default)]
pub struct WordNgramConfigBuilder {
    ngram_set: WordNgramSet,
    frequent_term_threshold: Option<f32>,
    max_frequent_terms: Option<usize>,
}

impl WordNgramConfigBuilder {
    /// Set the ngram types to enable
    pub fn ngram_types(mut self, ngram_set: WordNgramSet) -> Self {
        self.ngram_set = ngram_set;
        self
    }
    
    /// Set the frequent term threshold
    pub fn frequent_threshold(mut self, threshold: f32) -> Self {
        self.frequent_term_threshold = Some(threshold);
        self
    }
    
    /// Set the maximum number of frequent terms
    pub fn max_frequent_terms(mut self, max: usize) -> Self {
        self.max_frequent_terms = Some(max);
        self
    }
    
    /// Build the configuration
    pub fn build(self) -> WordNgramConfig {
        WordNgramConfig {
            ngram_set: self.ngram_set,
            frequent_term_threshold: self.frequent_term_threshold.unwrap_or_else(default_frequent_threshold),
            max_frequent_terms: self.max_frequent_terms.unwrap_or_else(default_max_frequent_terms),
        }
    }
}

/// Ngram type identifier for classifying bigrams and trigrams based on term frequency
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(u8)]
pub enum NgramType {
    /// Single term (not an ngram)
    #[default]
    SingleTerm = 0,
    /// Bigram: Frequent-Frequent
    NgramFF = 1,
    /// Bigram: Frequent-Rare
    NgramFR = 2,
    /// Bigram: Rare-Frequent
    NgramRF = 3,
    /// Trigram: Frequent-Frequent-Frequent
    NgramFFF = 4,
    /// Trigram: Rare-Frequent-Frequent
    NgramRFF = 5,
    /// Trigram: Frequent-Frequent-Rare
    NgramFFR = 6,
    /// Trigram: Frequent-Rare-Frequent
    NgramFRF = 7,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ngram_set_flags() {
        let config = WordNgramSet::new()
            .with_ngram_ff()
            .with_ngram_fr();
        assert!(config.contains(WordNgramSet::NGRAM_FF));
        assert!(config.contains(WordNgramSet::NGRAM_FR));
        assert!(!config.contains(WordNgramSet::NGRAM_RF));
    }
}
