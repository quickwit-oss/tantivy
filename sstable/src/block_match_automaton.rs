use tantivy_fst::Automaton;

/// Returns whether a block whose starting key (exclusive) and final key (inclusive) can match the
/// provided automaton, without actually looking at the block content.
pub(crate) fn block_match_automaton(
    start_key: Option<&[u8]>,
    end_key: &[u8],
    automaton: &impl Automaton,
) -> bool {
    let initial_block = start_key.is_none();
    let start_key = start_key.unwrap_or(&[]);

    debug_assert!(start_key <= end_key);

    let prefix_len = start_key
        .iter()
        .zip(end_key)
        .take_while(|(c1, c2)| c1 == c2)
        .count();

    let mut base_state = automaton.start();
    for c in &start_key[0..prefix_len] {
        base_state = automaton.accept(&base_state, *c);
    }
    if !automaton.can_match(&base_state) {
        return false;
    }

    if initial_block && automaton.is_match(&base_state) {
        // for other blocks, the start_key is exclusive, and a prefix of it would be in a
        // previous block
        return true;
    }

    // we have 3 distinct case:
    // - keys are `abc` and `abcd` => we test for abc[\0-d].*
    // - keys are `abcd` and `abce` => we test for abc[d-e].*
    // - keys are `abcd` and `abc` => contradiction with start_key < end_key.
    //
    // ideally for [abc, abcde] we could test for abc([\0-c].*|d([\0-d].*|e)?)
    // but let's start simple (and correct), and tighten our bounds latter
    //
    // and for [abcde, abcfg] we could test for abc(d(e.+|[f-\xff].*)|e.*|f([\0-f].*|g)?)
    // abc (
    //  d(e.+|[f-\xff].*) |
    //  e.* |
    //  f([\0-f].*|g)?
    // )
    //
    // these are all written as regex, but can be converted to operations we can do:
    // - [x-y] is a for c in x..y
    // - .* is a can_match()
    // - .+ is a for c in 0..=255 { accept(c).can_match() }
    // - ? is a the thing before can_match(), or current state.is_match()
    // - | means test both side

    let mut start_range = *start_key.get(prefix_len).unwrap_or(&0);
    let end_range = end_key[prefix_len];

    if start_key.len() > prefix_len {
        start_range += 1;
    }
    for c in start_range..end_range {
        let new_state = automaton.accept(&base_state, c);
        if automaton.can_match(&new_state) {
            return true;
        }
    }
    if start_key.len() > prefix_len {
        if start_key.len() <= prefix_len {
            // case [abcd, abcde], we need to handle \0 which wasn't processed
            let new_state = automaton.accept(&base_state, start_range);
            if automaton.can_match(&new_state) {
                eprintln!("ho");
                return true;
            }
        } else if match_range_start(&start_key[prefix_len..], &automaton, &base_state) {
            return true;
        }
    }
    match_range_end(&end_key[prefix_len..], &automaton, &base_state)
}

fn match_range_start<S, A: Automaton<State = S>>(
    start_key: &[u8],
    automaton: &A,
    base_state: &S,
) -> bool {
    // case [abcdef, abcghi], we need to handle
    // - abcd[f-\xff].*
    // - abcde[g-\xff].*
    // - abcdef.+ == abcdef[\0-\xff].*
    let mut state = automaton.accept(base_state, start_key[0]);
    for start_point in &start_key[1..] {
        if !automaton.can_match(&state) {
            return false;
        }
        // handle case where start_point is \xff
        if *start_point < u8::MAX {
            for to_name in (start_point + 1)..=u8::MAX {
                let temp_state = automaton.accept(&state, to_name);
                if automaton.can_match(&temp_state) {
                    return true;
                }
            }
        }
        state = automaton.accept(&state, *start_point);
    }

    if !automaton.can_match(&state) {
        return false;
    }
    for to_name in 0..=u8::MAX {
        let temp_state = automaton.accept(&state, to_name);
        if automaton.can_match(&temp_state) {
            return true;
        }
    }
    false
}

fn match_range_end<S, A: Automaton<State = S>>(
    end_key: &[u8],
    automaton: &A,
    base_state: &S,
) -> bool {
    //  f([\0-f].*|g)?
    // case [abcdef, abcghi], we need to handle
    // - abcg[\0-g].*
    // - abcgh[\0-h].*
    // - abcghi
    let mut state = automaton.accept(base_state, end_key[0]);
    for end_point in &end_key[1..] {
        if !automaton.can_match(&state) {
            return false;
        }
        if automaton.is_match(&state) {
            return true;
        }
        for to_name in 0..*end_point {
            let temp_state = automaton.accept(&state, to_name);
            if automaton.can_match(&temp_state) {
                return true;
            }
        }
        state = automaton.accept(&state, *end_point);
    }

    automaton.is_match(&state)
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;
    use tantivy_fst::Automaton;

    use super::*;

    struct EqBuffer(Vec<u8>);

    impl Automaton for EqBuffer {
        type State = Option<usize>;

        fn start(&self) -> Self::State {
            Some(0)
        }

        fn is_match(&self, state: &Self::State) -> bool {
            *state == Some(self.0.len())
        }

        fn accept(&self, state: &Self::State, byte: u8) -> Self::State {
            state
                .filter(|pos| self.0.get(*pos) == Some(&byte))
                .map(|pos| pos + 1)
        }

        fn can_match(&self, state: &Self::State) -> bool {
            state.is_some()
        }

        fn will_always_match(&self, _state: &Self::State) -> bool {
            false
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(1_000_000_000))]
        #[test]
        fn test_proptest_automaton_match_block(start in any::<Vec<u8>>(), end in any::<Vec<u8>>(), key in any::<Vec<u8>>()) {
            // inverted keys are *not* supported and can return bogus results
            if start < end && !end.is_empty() {
                let expected = start < key && end >= key;
                let automaton = EqBuffer(key);

                assert_eq!(block_match_automaton(Some(&start), &end, &automaton), expected);
            }
        }

        #[test]
        fn test_proptest_automaton_match_first_block(end in any::<Vec<u8>>(), key in any::<Vec<u8>>()) {
            if !end.is_empty() {
                let expected = end >= key;
                let automaton = EqBuffer(key);
                assert_eq!(block_match_automaton(None, &end, &automaton), expected);
            }
        }
    }
}
