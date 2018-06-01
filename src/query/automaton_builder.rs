use fst::Automaton;

pub trait AutomatonBuilder<A>
where
  A: Automaton,
{
  fn build_automaton(&self) -> Box<A>;
}
