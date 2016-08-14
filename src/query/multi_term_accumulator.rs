pub trait MultiTermAccumulator {
    fn update(&mut self, term_ord: usize, term_freq: u32, fieldnorm: u32);
    fn clear(&mut self,);
}
