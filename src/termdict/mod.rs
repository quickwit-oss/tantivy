use tantivy_fst::automaton::AlwaysMatch;

mod fst_termdict;
// mod traits;

#[cfg(test)]
mod tests;

/// Position of the term in the sorted list of terms.
pub type TermOrdinal = u64;

/// The term dictionary contains all of the terms in
/// `tantivy index` in a sorted manner.
pub type TermDictionary = self::fst_termdict::TermDictionary;

/// Builder for the new term dictionary.
///
/// Inserting must be done in the order of the `keys`.
pub type TermDictionaryBuilder<W> = self::fst_termdict::TermDictionaryBuilder<W>;

/// Given a list of sorted term streams,
/// returns an iterator over sorted unique terms.
///
/// The item yield is actually a pair with
/// - the term
/// - a slice with the ordinal of the segments containing
/// the terms.
pub type TermMerger<'a> = self::fst_termdict::TermMerger<'a>;

/// `TermStreamer` acts as a cursor over a range of terms of a segment.
/// Terms are guaranteed to be sorted.
pub type TermStreamer<'a, A = AlwaysMatch> = self::fst_termdict::TermStreamer<'a, A>;
