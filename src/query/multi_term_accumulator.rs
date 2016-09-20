
/// Accumulator of the matching terms information
pub trait MultiTermAccumulator {
    /// Update the accumulator given the information of a term.
    /// - term_ord is the term_ordinal
    /// - term_freq is the frequency of the term within the document
    /// - fieldnorm is the number of tokens associated to the field for this document
    ///
    /// The term's update do not have to arrive in a specific order.
    /// Terms that are not present in the document will not be updated.
    fn update(&mut self, term_ord: usize, term_freq: u32, fieldnorm: u32);
    /// Resets the accumulator
    fn clear(&mut self,);
}
