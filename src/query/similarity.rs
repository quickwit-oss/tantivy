use Score;
use query::Explanation;
use query::MultiTermAccumulator;

/// Similarity score
pub trait Similarity: MultiTermAccumulator {
    
    /// Compute and returns the similarity score,
    ///
    /// The results are not cached.
    fn score(&self, ) -> Score;
    
    /// Explain the computation of this similarity given all of
    /// terms information.
    ///
    /// `vals` is an array of `(term_ord, term_freq, field_norm)`.
    /// Terms that are not present should not appear in the array. 
    fn explain(&self, vals: &[(usize, u32, u32)]) -> Explanation;
}
