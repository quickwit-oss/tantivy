use std::any::Any;

use tantivy_fst::automaton::Automaton;

/// Object-safe trait for type-erased automaton states.
trait DynState: Any + Send + Sync {
    fn clone_boxed(&self) -> Box<dyn DynState>;
    fn as_any(&self) -> &dyn Any;
}

impl<S: Clone + Send + Sync + 'static> DynState for S {
    fn clone_boxed(&self) -> Box<dyn DynState> {
        Box::new(self.clone())
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl Clone for Box<dyn DynState> {
    fn clone(&self) -> Self {
        self.clone_boxed()
    }
}

/// Opaque, cloneable state for [`BoxedAutomaton`].
pub struct BoxedAutomatonState(Box<dyn DynState>);

impl Clone for BoxedAutomatonState {
    fn clone(&self) -> Self {
        BoxedAutomatonState(self.0.clone())
    }
}

/// Object-safe trait for type-erased automatons.
trait DynAutomaton: Send + Sync {
    fn start_boxed(&self) -> BoxedAutomatonState;
    fn is_match_boxed(&self, state: &BoxedAutomatonState) -> bool;
    fn can_match_boxed(&self, state: &BoxedAutomatonState) -> bool;
    fn will_always_match_boxed(&self, state: &BoxedAutomatonState) -> bool;
    fn accept_boxed(&self, state: &BoxedAutomatonState, byte: u8) -> BoxedAutomatonState;
    fn clone_boxed(&self) -> Box<dyn DynAutomaton>;
}

impl<A> DynAutomaton for A
where
    A: Automaton + Clone + Send + Sync + 'static,
    A::State: Clone + Send + Sync + 'static,
{
    fn start_boxed(&self) -> BoxedAutomatonState {
        BoxedAutomatonState(Box::new(self.start()))
    }
    fn is_match_boxed(&self, state: &BoxedAutomatonState) -> bool {
        self.is_match(state.0.as_any().downcast_ref::<A::State>().unwrap())
    }
    fn can_match_boxed(&self, state: &BoxedAutomatonState) -> bool {
        self.can_match(state.0.as_any().downcast_ref::<A::State>().unwrap())
    }
    fn will_always_match_boxed(&self, state: &BoxedAutomatonState) -> bool {
        self.will_always_match(state.0.as_any().downcast_ref::<A::State>().unwrap())
    }
    fn accept_boxed(&self, state: &BoxedAutomatonState, byte: u8) -> BoxedAutomatonState {
        BoxedAutomatonState(Box::new(
            self.accept(state.0.as_any().downcast_ref::<A::State>().unwrap(), byte),
        ))
    }
    fn clone_boxed(&self) -> Box<dyn DynAutomaton> {
        Box::new(self.clone())
    }
}

/// A type-erased automaton that implements [`Automaton`].
///
/// Construct via `BoxedAutomaton::new(automaton)` from any concrete automaton
/// whose state is `Clone + Send + Sync + 'static`.
pub struct BoxedAutomaton(Box<dyn DynAutomaton>);

impl BoxedAutomaton {
    /// Wraps any concrete [`Automaton`] into a type-erased `BoxedAutomaton`.
    pub fn new<A>(automaton: A) -> Self
    where
        A: Automaton + Clone + Send + Sync + 'static,
        A::State: Clone + Send + Sync + 'static,
    {
        BoxedAutomaton(Box::new(automaton))
    }
}

impl Clone for BoxedAutomaton {
    fn clone(&self) -> Self {
        BoxedAutomaton(self.0.clone_boxed())
    }
}

impl Automaton for BoxedAutomaton {
    type State = BoxedAutomatonState;

    fn start(&self) -> Self::State {
        self.0.start_boxed()
    }
    fn is_match(&self, state: &Self::State) -> bool {
        self.0.is_match_boxed(state)
    }
    fn can_match(&self, state: &Self::State) -> bool {
        self.0.can_match_boxed(state)
    }
    fn will_always_match(&self, state: &Self::State) -> bool {
        self.0.will_always_match_boxed(state)
    }
    fn accept(&self, state: &Self::State, byte: u8) -> Self::State {
        self.0.accept_boxed(state, byte)
    }
}
