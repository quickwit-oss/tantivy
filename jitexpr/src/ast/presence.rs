//! Necessary conditions on the presence of variables.
//!
//! Most functions return null as soon as one of their arguments is null. An expression can
//! therefore often only produce a value, or only evaluate to `true`, if some of its variables are
//! present. For instance, `(EQ (ADD a 1i64) b)` is null unless both `a` and `b` are present.
//!
//! A caller evaluating a predicate over many documents can use this to skip the documents missing
//! these variables without evaluating the expression.

use std::sync::Arc;

use crate::ast::{Function, Literal, UntypedExpr};

/// A boolean formula over the presence of variables.
///
/// It is meant to be used as a necessary condition: it is implied by some property of an
/// expression (producing a value, or evaluating to `true`), but it does not imply it.
///
/// Conditions are kept in a canonical form, so that `Eq` and `Hash` identify conditions that
/// only differ by the order, grouping, or repetition of their children. Two equivalent
/// conditions may still compare as different (e.g. `a ∧ (a ∨ b)` and `a`): deciding logical
/// equivalence in general is too expensive.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum VariablePresenceCondition {
    /// Always satisfied. This condition carries no information.
    Always,
    /// Never satisfied.
    Never,
    /// Satisfied when the variable is present.
    Present(Arc<str>),
    /// Satisfied when all of the conditions are satisfied.
    All(ConditionSet),
    /// Satisfied when at least one of the conditions is satisfied.
    Any(ConditionSet),
}

/// The children of a [`PresenceCondition::All`] or [`PresenceCondition::Any`] node.
///
/// It can only be built through [`PresenceCondition::all`] and [`PresenceCondition::any`], which
/// uphold the following hidden contract, on which the derived `Eq` and `Hash` rely:
/// - children are sorted and distinct, and there are at least two of them;
/// - no child is `Always`, `Never`, or a node of the same kind as the parent.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ConditionSet(Vec<VariablePresenceCondition>);

impl ConditionSet {
    /// Returns the children, in canonical order.
    pub fn iter(&self) -> impl Iterator<Item = &VariablePresenceCondition> {
        self.0.iter()
    }
}

impl VariablePresenceCondition {
    /// Builds the canonical conjunction of `conditions`.
    pub fn all(
        conditions: impl IntoIterator<Item = VariablePresenceCondition>,
    ) -> VariablePresenceCondition {
        let mut children: Vec<VariablePresenceCondition> = Vec::new();
        for condition in conditions {
            match condition {
                VariablePresenceCondition::Always => {}
                VariablePresenceCondition::Never => return VariablePresenceCondition::Never,
                VariablePresenceCondition::All(grand_children) => children.extend(grand_children.0),
                condition => children.push(condition),
            }
        }
        children.sort();
        children.dedup();
        match children.len() {
            0 => VariablePresenceCondition::Always,
            1 => children.pop().unwrap(),
            _ => VariablePresenceCondition::All(ConditionSet(children)),
        }
    }

    /// Builds the canonical disjunction of `conditions`.
    pub fn any(
        conditions: impl IntoIterator<Item = VariablePresenceCondition>,
    ) -> VariablePresenceCondition {
        let mut children: Vec<VariablePresenceCondition> = Vec::new();
        for condition in conditions {
            match condition {
                VariablePresenceCondition::Never => {}
                VariablePresenceCondition::Always => return VariablePresenceCondition::Always,
                VariablePresenceCondition::Any(grand_children) => children.extend(grand_children.0),
                condition => children.push(condition),
            }
        }
        children.sort();
        children.dedup();
        match children.len() {
            0 => VariablePresenceCondition::Never,
            1 => children.pop().unwrap(),
            _ => VariablePresenceCondition::Any(ConditionSet(children)),
        }
    }

    /// Evaluates the condition, given the presence of each variable.
    #[cfg(test)]
    pub fn eval(&self, is_present: &mut impl FnMut(&str) -> bool) -> bool {
        match self {
            VariablePresenceCondition::Always => true,
            VariablePresenceCondition::Never => false,
            VariablePresenceCondition::Present(variable_name) => is_present(variable_name),
            VariablePresenceCondition::All(conditions) => conditions
                .iter()
                .all(|condition| condition.eval(&mut *is_present)),
            VariablePresenceCondition::Any(conditions) => conditions
                .iter()
                .any(|condition| condition.eval(&mut *is_present)),
        }
    }
}

/// Returns a necessary presence condition for `expr` to evaluate to a non-null value.
pub fn required_presence(expr: &UntypedExpr) -> VariablePresenceCondition {
    match expr {
        // Literals never constrain variables. `none` could be mapped to `Never`, but some
        // functions take literal configuration arguments (delimiters, precision, ...), and we
        // prefer not to depend on how each of them handles a `none` there.
        UntypedExpr::Literal(_) => VariablePresenceCondition::Always,
        UntypedExpr::Variable(variable_name) => {
            VariablePresenceCondition::Present(variable_name.clone())
        }
        UntypedExpr::FnCall { function, args } => required_presence_for_fn_call(*function, args),
    }
}

/// Returns a necessary presence condition for `expr` to evaluate to a present `true`.
///
/// See [`required_presence`] for the definition of a present variable.
pub fn required_presence_for_true(expr: &UntypedExpr) -> VariablePresenceCondition {
    match expr {
        UntypedExpr::Literal(Literal::Bool(true)) => VariablePresenceCondition::Always,
        // No other literal is `true`.
        UntypedExpr::Literal(_) => VariablePresenceCondition::Never,
        UntypedExpr::Variable(variable_name) => {
            VariablePresenceCondition::Present(variable_name.clone())
        }
        UntypedExpr::FnCall { function, args } => {
            required_presence_for_true_for_fn_call(*function, args)
        }
    }
}

fn required_presence_for_fn_call(
    function: Function,
    args: &[UntypedExpr],
) -> VariablePresenceCondition {
    // This match must remain exhaustive: classifying a function returning a present value for a
    // null argument as "null in, null out" would make callers skip matching documents.
    match function {
        // A null argument makes the result null.
        //
        // AND belongs here: `(AND false none)` is null.
        Function::Abs
        | Function::Add
        | Function::And
        | Function::Ceil
        | Function::Concat
        | Function::Divide
        | Function::Eq
        | Function::Floor
        | Function::Gt
        | Function::GtEq
        | Function::IntMod
        | Function::Left
        | Function::Lower
        | Function::Lt
        | Function::LtEq
        | Function::Max
        | Function::Min
        | Function::Multiply
        | Function::Pow
        | Function::RegexpExtract
        | Function::Right
        | Function::Round
        | Function::SplitAfter
        | Function::SplitBefore
        | Function::Sqrt
        | Function::Substring
        | Function::SubstringCount
        | Function::Subtract
        | Function::TextJoin
        | Function::Trim
        | Function::Upper => VariablePresenceCondition::all(args.iter().map(required_presence)),
        // OR is null only if all of its arguments are null.
        Function::Or => VariablePresenceCondition::any(args.iter().map(required_presence)),
        // IF is null if its condition is null. Otherwise it takes the presence of the selected
        // branch.
        Function::If => {
            let [condition, when_true, when_false] = args else {
                return VariablePresenceCondition::Always;
            };
            VariablePresenceCondition::all([
                required_presence(condition),
                VariablePresenceCondition::any([
                    required_presence(when_true),
                    required_presence(when_false),
                ]),
            ])
        }
        // These functions always return a present value.
        //
        // REGEXP_LIKE returns `false` for a null input.
        Function::IsNotNull
        | Function::IsNull
        | Function::Neq
        | Function::Not
        | Function::RegexpLike => VariablePresenceCondition::Always,
    }
}

fn required_presence_for_true_for_fn_call(
    function: Function,
    args: &[UntypedExpr],
) -> VariablePresenceCondition {
    match function {
        Function::And => {
            VariablePresenceCondition::all(args.iter().map(required_presence_for_true))
        }
        Function::Or => VariablePresenceCondition::any(args.iter().map(required_presence_for_true)),
        Function::If => {
            let [condition, when_true, when_false] = args else {
                return VariablePresenceCondition::Always;
            };
            VariablePresenceCondition::all([
                required_presence(condition),
                VariablePresenceCondition::any([
                    required_presence_for_true(when_true),
                    required_presence_for_true(when_false),
                ]),
            ])
        }
        Function::IsNotNull => {
            let [arg] = args else {
                return VariablePresenceCondition::Always;
            };
            required_presence(arg)
        }
        // REGEXP_LIKE returns `false` for a null input.
        Function::RegexpLike => {
            let Some(input) = args.first() else {
                return VariablePresenceCondition::Always;
            };
            required_presence(input)
        }
        // A `true` result is in particular a present result. This fallback is therefore correct
        // for any function, including functions added later.
        //
        // It yields `Always` for NOT, NEQ, and IS_NULL, which are `true` when their
        // argument is null.
        _ => required_presence_for_fn_call(function, args),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    use proptest::prelude::*;
    use proptest::strategy::BoxedStrategy;

    use super::*;
    use crate::ast::{InferredTypeSet, deserialize, infer_types_with_target};
    use crate::compile::{StringArena, compile};
    use crate::types::{VarType, VariableValue};

    fn present(variable_name: &str) -> VariablePresenceCondition {
        VariablePresenceCondition::Present(Arc::from(variable_name))
    }

    fn all(conditions: Vec<VariablePresenceCondition>) -> VariablePresenceCondition {
        VariablePresenceCondition::all(conditions)
    }

    fn any(conditions: Vec<VariablePresenceCondition>) -> VariablePresenceCondition {
        VariablePresenceCondition::any(conditions)
    }

    fn for_true(expr: &str) -> VariablePresenceCondition {
        required_presence_for_true(&deserialize(expr).unwrap())
    }

    fn for_value(expr: &str) -> VariablePresenceCondition {
        required_presence(&deserialize(expr).unwrap())
    }

    #[test]
    fn test_all_simplification() {
        assert_eq!(
            VariablePresenceCondition::all([]),
            VariablePresenceCondition::Always
        );
        assert_eq!(
            VariablePresenceCondition::all([VariablePresenceCondition::Always, present("a")]),
            present("a")
        );
        assert_eq!(
            VariablePresenceCondition::all([present("a"), VariablePresenceCondition::Never]),
            VariablePresenceCondition::Never
        );
        assert_eq!(
            VariablePresenceCondition::all([
                present("a"),
                all(vec![present("b"), present("a")]),
                present("c"),
            ]),
            all(vec![present("a"), present("b"), present("c")])
        );
        assert_eq!(
            VariablePresenceCondition::all([any(vec![present("a"), present("b")]), present("c")]),
            all(vec![any(vec![present("a"), present("b")]), present("c")])
        );
    }

    #[test]
    fn test_any_simplification() {
        assert_eq!(
            VariablePresenceCondition::any([]),
            VariablePresenceCondition::Never
        );
        assert_eq!(
            VariablePresenceCondition::any([VariablePresenceCondition::Never, present("a")]),
            present("a")
        );
        assert_eq!(
            VariablePresenceCondition::any([present("a"), VariablePresenceCondition::Always]),
            VariablePresenceCondition::Always
        );
        assert_eq!(
            VariablePresenceCondition::any([present("a"), any(vec![present("b"), present("a")])]),
            any(vec![present("a"), present("b")])
        );
    }

    fn hash_of(condition: &VariablePresenceCondition) -> u64 {
        let mut hasher = DefaultHasher::new();
        condition.hash(&mut hasher);
        hasher.finish()
    }

    fn assert_same(left: VariablePresenceCondition, right: VariablePresenceCondition) {
        assert_eq!(left, right);
        assert_eq!(hash_of(&left), hash_of(&right));
    }

    #[test]
    fn test_canonical_order() {
        let (a, b, c) = (present("a"), present("b"), present("c"));
        assert_same(
            VariablePresenceCondition::all([a.clone(), b.clone()]),
            VariablePresenceCondition::all([b.clone(), a.clone()]),
        );
        assert_same(
            VariablePresenceCondition::any([c.clone(), a.clone(), b.clone()]),
            VariablePresenceCondition::any([b.clone(), c.clone(), a.clone()]),
        );
        assert_ne!(
            VariablePresenceCondition::all([a.clone(), b.clone()]),
            VariablePresenceCondition::any([a.clone(), b.clone()])
        );
        let VariablePresenceCondition::All(children) =
            VariablePresenceCondition::all([c.clone(), a.clone()])
        else {
            panic!("expected an All node");
        };
        assert_eq!(children.iter().collect::<Vec<_>>(), vec![&a, &c]);
    }

    #[test]
    fn test_canonical_grouping_and_repetition() {
        let (a, b, c) = (present("a"), present("b"), present("c"));
        assert_same(
            VariablePresenceCondition::all([
                a.clone(),
                VariablePresenceCondition::all([b.clone(), c.clone()]),
            ]),
            VariablePresenceCondition::all([
                VariablePresenceCondition::all([c.clone(), a.clone()]),
                b.clone(),
            ]),
        );
        assert_same(
            VariablePresenceCondition::all([a.clone(), a.clone()]),
            a.clone(),
        );
        assert_same(
            VariablePresenceCondition::any([
                VariablePresenceCondition::all([a.clone(), b.clone()]),
                VariablePresenceCondition::all([b.clone(), a.clone()]),
            ]),
            VariablePresenceCondition::all([a.clone(), b.clone()]),
        );
    }

    /// A condition tree built without any normalization.
    #[derive(Clone, Debug)]
    enum RawCondition {
        Always,
        Never,
        Present(usize),
        All(Vec<RawCondition>),
        Any(Vec<RawCondition>),
    }

    const RAW_VARIABLES: [&str; 4] = ["a", "b", "c", "d"];

    impl RawCondition {
        fn eval(&self, present_mask: u32) -> bool {
            match self {
                RawCondition::Always => true,
                RawCondition::Never => false,
                RawCondition::Present(variable_ord) => present_mask & (1 << variable_ord) != 0,
                RawCondition::All(children) => {
                    children.iter().all(|child| child.eval(present_mask))
                }
                RawCondition::Any(children) => {
                    children.iter().any(|child| child.eval(present_mask))
                }
            }
        }

        /// Builds the canonical condition, visiting children in reverse order if `reverse`.
        fn build(&self, reverse: bool) -> VariablePresenceCondition {
            let build_children = |children: &[RawCondition]| {
                let mut built: Vec<VariablePresenceCondition> =
                    children.iter().map(|child| child.build(reverse)).collect();
                if reverse {
                    built.reverse();
                }
                built
            };
            match self {
                RawCondition::Always => VariablePresenceCondition::Always,
                RawCondition::Never => VariablePresenceCondition::Never,
                RawCondition::Present(variable_ord) => present(RAW_VARIABLES[*variable_ord]),
                RawCondition::All(children) => {
                    VariablePresenceCondition::all(build_children(children))
                }
                RawCondition::Any(children) => {
                    VariablePresenceCondition::any(build_children(children))
                }
            }
        }
    }

    fn raw_conditions() -> impl Strategy<Value = RawCondition> {
        let leaf = prop_oneof![
            1 => Just(RawCondition::Always),
            1 => Just(RawCondition::Never),
            6 => (0..RAW_VARIABLES.len()).prop_map(RawCondition::Present),
        ];
        leaf.prop_recursive(4, 32, 4, |inner| {
            prop_oneof![
                prop::collection::vec(inner.clone(), 0..4).prop_map(RawCondition::All),
                prop::collection::vec(inner, 0..4).prop_map(RawCondition::Any),
            ]
        })
    }

    /// Checks the hidden contract of `ConditionSet`, recursively.
    fn assert_canonical(condition: &VariablePresenceCondition) {
        let (children, is_all) = match condition {
            VariablePresenceCondition::All(children) => (children, true),
            VariablePresenceCondition::Any(children) => (children, false),
            _ => return,
        };
        let children: Vec<&VariablePresenceCondition> = children.iter().collect();
        assert!(children.len() >= 2, "{condition:?}");
        assert!(
            children.windows(2).all(|pair| pair[0] < pair[1]),
            "{condition:?}"
        );
        for child in &children {
            assert_canonical(child);
            match (child, is_all) {
                (VariablePresenceCondition::Always | VariablePresenceCondition::Never, _) => {
                    panic!("neutral or absorbing child in {condition:?}")
                }
                (VariablePresenceCondition::All(_), true)
                | (VariablePresenceCondition::Any(_), false) => {
                    panic!("same-kind child in {condition:?}")
                }
                _ => {}
            }
        }
    }

    proptest! {
        #[test]
        fn proptest_canonical_form(raw in raw_conditions()) {
            let condition = raw.build(false);
            assert_canonical(&condition);
            let reversed = raw.build(true);
            prop_assert_eq!(&condition, &reversed);
            prop_assert_eq!(hash_of(&condition), hash_of(&reversed));
            for present_mask in 0..(1u32 << RAW_VARIABLES.len()) {
                let mut is_present = |variable_name: &str| {
                    let variable_ord =
                        RAW_VARIABLES.iter().position(|name| *name == variable_name).unwrap();
                    present_mask & (1 << variable_ord) != 0
                };
                prop_assert_eq!(condition.eval(&mut is_present), raw.eval(present_mask));
            }
        }
    }

    fn presence_of<'a>(present_names: &'a [&'a str]) -> impl FnMut(&str) -> bool + 'a {
        move |variable_name: &str| present_names.contains(&variable_name)
    }

    #[test]
    fn test_eval() {
        let condition = all(vec![present("a"), any(vec![present("b"), present("c")])]);
        assert!(condition.eval(&mut presence_of(&["a", "c"])));
        assert!(!condition.eval(&mut presence_of(&["a"])));
        assert!(!condition.eval(&mut presence_of(&["b", "c"])));
        assert!(VariablePresenceCondition::Always.eval(&mut presence_of(&[])));
        assert!(!VariablePresenceCondition::Never.eval(&mut presence_of(&["a"])));
    }

    #[test]
    fn test_literals() {
        assert_eq!(for_true("true"), VariablePresenceCondition::Always);
        assert_eq!(for_true("false"), VariablePresenceCondition::Never);
        assert_eq!(for_true("none"), VariablePresenceCondition::Never);
        assert_eq!(for_value("none"), VariablePresenceCondition::Always);
        assert_eq!(for_value("1u64"), VariablePresenceCondition::Always);
        // `none` in a strict function is conservatively ignored.
        assert_eq!(for_true("(EQ a none)"), present("a"));
    }

    #[test]
    fn test_variable() {
        assert_eq!(for_true("flag"), present("flag"));
        assert_eq!(for_value("a"), present("a"));
    }

    #[test]
    fn test_strict_functions() {
        assert_eq!(
            for_true("(EQ (ADD a 1i64) b)"),
            all(vec![present("a"), present("b")])
        );
        assert_eq!(for_true("(GT (ABS a) 3i64)"), present("a"));
        assert_eq!(
            for_value(r#"(CONCAT "," "true" (UPPER a) (SUBSTRING b 0i64 2i64))"#),
            all(vec![present("a"), present("b")])
        );
        assert_eq!(for_value(r#"(REGEXP_EXTRACT a "(x+)" 1u64)"#), present("a"));
        assert_eq!(for_value("(ADD)"), VariablePresenceCondition::Always);
    }

    #[test]
    fn test_and_or() {
        assert_eq!(
            for_true("(AND (EQ a 1i64) (LT b 2i64) c)"),
            all(vec![present("a"), present("b"), present("c")])
        );
        assert_eq!(
            for_true("(OR (EQ a 1i64) (EQ b 2i64))"),
            any(vec![present("a"), present("b")])
        );
        assert_eq!(
            for_true("(AND (OR (EQ a 1i64) (EQ b 2i64)) (EQ c 3i64))"),
            all(vec![any(vec![present("a"), present("b")]), present("c")])
        );
        // AND is null as soon as one of its arguments is null.
        assert_eq!(for_value("(AND (NOT a) b)"), present("b"));
        assert_eq!(
            for_value("(OR (EQ a 1i64) (EQ b 2i64))"),
            any(vec![present("a"), present("b")])
        );
    }

    #[test]
    fn test_null_tolerant_functions() {
        assert_eq!(
            for_true("(NOT (EQ a 1i64))"),
            VariablePresenceCondition::Always
        );
        assert_eq!(for_true("(NEQ a 1i64)"), VariablePresenceCondition::Always);
        assert_eq!(for_true("(IS_NULL a)"), VariablePresenceCondition::Always);
        assert_eq!(
            for_value("(IS_NOT_NULL a)"),
            VariablePresenceCondition::Always
        );
        assert_eq!(
            for_true("(IS_NOT_NULL (ADD a b))"),
            all(vec![present("a"), present("b")])
        );
        assert_eq!(
            for_value(r#"(REGEXP_LIKE a "x")"#),
            VariablePresenceCondition::Always
        );
        assert_eq!(for_true(r#"(REGEXP_LIKE a "x")"#), present("a"));
        assert_eq!(
            for_true("(OR (EQ a 1i64) (IS_NULL b))"),
            VariablePresenceCondition::Always
        );
        assert_eq!(for_true("(AND (EQ a 1i64) (IS_NULL b))"), present("a"));
    }

    #[test]
    fn test_if() {
        assert_eq!(
            for_value("(IF c a b)"),
            all(vec![present("c"), any(vec![present("a"), present("b")])])
        );
        assert_eq!(
            for_true("(IF c (EQ a 1i64) (EQ b 1i64))"),
            all(vec![present("c"), any(vec![present("a"), present("b")])])
        );
        assert_eq!(for_true("(IF c true false)"), present("c"));
        assert_eq!(
            for_true("(IF c false false)"),
            VariablePresenceCondition::Never
        );
        assert_eq!(for_value("(IF c 1i64 a)"), present("c"));
    }

    // The property tests below check that the conditions are indeed necessary, by comparing them
    // with the compiled expression over random inputs.

    const VARIABLES: [(&str, VarType); 7] = [
        ("b0", VarType::Bool),
        ("b1", VarType::Bool),
        ("n0", VarType::I64),
        ("n1", VarType::I64),
        ("f0", VarType::F64),
        ("s0", VarType::Str),
        ("s1", VarType::Str),
    ];

    /// Values are indexed by variable, in the order of `VARIABLES`. `None` means null.
    type Assignment = Vec<Option<u8>>;

    fn variable_value(var_type: VarType, value_ord: u8) -> VariableValue<'static> {
        let value_ord = value_ord as usize;
        match var_type {
            VarType::Bool => VariableValue::some([true, false, true, false][value_ord]),
            VarType::I64 => VariableValue::some([0i64, 1, -2, 3][value_ord]),
            VarType::F64 => VariableValue::some([0.0f64, 1.5, -1.0, 2.0][value_ord]),
            VarType::Str => VariableValue::some(["", "a", "ab,a", "ba"][value_ord]),
            VarType::U64 | VarType::None => unreachable!(),
        }
    }

    fn assignments() -> impl Strategy<Value = Vec<Assignment>> {
        let value = prop_oneof![Just(None), (0u8..4).prop_map(Some)];
        prop::collection::vec(prop::collection::vec(value, VARIABLES.len()), 1..16)
    }

    struct ExprStrategies {
        boolean: BoxedStrategy<String>,
        number: BoxedStrategy<String>,
        string: BoxedStrategy<String>,
    }

    fn leaves() -> ExprStrategies {
        let pick = |choices: &'static [&'static str]| {
            prop::sample::select(choices)
                .prop_map(str::to_string)
                .boxed()
        };
        ExprStrategies {
            boolean: pick(&["b0", "b1", "true", "false", "none"]),
            number: pick(&["n0", "n1", "f0", "0i64", "3i64", "-2i64", "1.5f64", "none"]),
            string: pick(&["s0", "s1", r#""a""#, r#""""#, "none"]),
        }
    }

    fn unary(arg: &BoxedStrategy<String>, template: &'static str) -> BoxedStrategy<String> {
        arg.clone()
            .prop_map(move |arg| template.replace("$0", &arg))
            .boxed()
    }

    fn binary(
        left: &BoxedStrategy<String>,
        right: &BoxedStrategy<String>,
        template: &'static str,
    ) -> BoxedStrategy<String> {
        (left.clone(), right.clone())
            .prop_map(move |(left, right)| template.replace("$0", &left).replace("$1", &right))
            .boxed()
    }

    fn ternary(
        first: &BoxedStrategy<String>,
        second: &BoxedStrategy<String>,
        third: &BoxedStrategy<String>,
        template: &'static str,
    ) -> BoxedStrategy<String> {
        (first.clone(), second.clone(), third.clone())
            .prop_map(move |(first, second, third)| {
                template
                    .replace("$0", &first)
                    .replace("$1", &second)
                    .replace("$2", &third)
            })
            .boxed()
    }

    /// Returns strategies generating well-typed expressions of the given depth.
    fn exprs(depth: u32) -> ExprStrategies {
        let leaves = leaves();
        if depth == 0 {
            return leaves;
        }
        let ExprStrategies {
            boolean: b,
            number: n,
            string: s,
        } = exprs(depth - 1);
        let any_kind = prop_oneof![b.clone(), n.clone(), s.clone()].boxed();
        let boolean = prop::strategy::Union::new(vec![
            leaves.boolean,
            binary(&b, &b, "(AND $0 $1)"),
            ternary(&b, &b, &b, "(AND $0 $1 $2)"),
            binary(&b, &b, "(OR $0 $1)"),
            ternary(&b, &b, &b, "(OR $0 $1 $2)"),
            unary(&b, "(NOT $0)"),
            unary(&any_kind, "(IS_NULL $0)"),
            unary(&any_kind, "(IS_NOT_NULL $0)"),
            binary(&n, &n, "(EQ $0 $1)"),
            binary(&s, &s, "(EQ $0 $1)"),
            binary(&b, &b, "(EQ $0 $1)"),
            binary(&n, &n, "(NEQ $0 $1)"),
            binary(&s, &s, "(NEQ $0 $1)"),
            binary(&n, &n, "(LT $0 $1)"),
            binary(&n, &n, "(LT_EQ $0 $1)"),
            binary(&n, &n, "(GT $0 $1)"),
            binary(&s, &s, "(GT_EQ $0 $1)"),
            unary(&s, r#"(REGEXP_LIKE $0 "a")"#),
            ternary(&b, &b, &b, "(IF $0 $1 $2)"),
        ])
        .boxed();
        let number = prop::strategy::Union::new(vec![
            leaves.number,
            unary(&n, "(ADD $0)"),
            binary(&n, &n, "(ADD $0 $1)"),
            binary(&n, &n, "(SUBTRACT $0 $1)"),
            binary(&n, &n, "(MULTIPLY $0 $1)"),
            binary(&n, &n, "(DIVIDE $0 $1)"),
            binary(&n, &n, "(POW $0 $1)"),
            binary(&n, &n, "(INT_MOD $0 $1)"),
            binary(&n, &n, "(MIN $0 $1)"),
            binary(&n, &n, "(MAX $0 $1)"),
            unary(&n, "(ABS $0)"),
            unary(&n, "(CEIL $0)"),
            unary(&n, "(FLOOR $0)"),
            unary(&n, "(SQRT $0)"),
            unary(&n, "(ROUND $0)"),
            unary(&n, "(ROUND $0 1i64)"),
            // SUBSTRING_COUNT is not generated: its native implementation builds a slice from a
            // null pointer when the haystack is null, which aborts debug builds.
            ternary(&b, &n, &n, "(IF $0 $1 $2)"),
        ])
        .boxed();
        let string = prop::strategy::Union::new(vec![
            leaves.string,
            unary(&s, "(UPPER $0)"),
            unary(&s, "(LOWER $0)"),
            unary(&s, "(LEFT $0 1i64)"),
            unary(&s, "(RIGHT $0 1i64)"),
            unary(&s, "(SUBSTRING $0 0i64 1i64)"),
            binary(&s, &s, r#"(CONCAT "," "false" $0 $1)"#),
            binary(&s, &s, r#"(TEXT_JOIN "," "true" $0 $1)"#),
            unary(&s, r#"(TRIM $0 "a" "both")"#),
            unary(&s, r#"(SPLIT_AFTER $0 ",")"#),
            unary(&s, r#"(SPLIT_BEFORE $0 "," 0i64)"#),
            unary(&s, r#"(REGEXP_EXTRACT $0 "(a)b" 1u64)"#),
            // IF is not generated for strings: with a null condition, it returns the selected
            // branch instead of null, as the string pointer is not cleared.
        ])
        .boxed();
        ExprStrategies {
            boolean,
            number,
            string,
        }
    }

    /// Compiles `expr_str`, then checks that `required(expr)` holds for every assignment where
    /// `holds(result)` is true.
    ///
    /// Following tantivy's fast field binding, a variable is bound only if its type is accepted by
    /// type inference. Unbound variables are null, and therefore absent.
    fn check_necessary_condition(
        expr_str: &str,
        target_type: InferredTypeSet,
        assignments: &[Assignment],
        required: fn(&UntypedExpr) -> VariablePresenceCondition,
        holds: fn(VarType, VariableValue) -> bool,
    ) -> Result<(), TestCaseError> {
        let expr = deserialize(expr_str).unwrap();
        let Ok(inferred_types) = infer_types_with_target(&expr, target_type) else {
            return Err(TestCaseError::reject("type inference failed"));
        };
        let mut variable_types: HashMap<&str, VarType> =
            HashMap::with_capacity(inferred_types.len());
        for (variable_name, accepted_types) in &inferred_types {
            let (_, var_type) = VARIABLES
                .iter()
                .find(|(name, _)| name == variable_name)
                .unwrap();
            if accepted_types.contains(*var_type) {
                variable_types.insert(*variable_name, *var_type);
            }
        }
        // Some expressions trip debug assertions of the compiler, unrelated to presence. For
        // instance, `(SQRT (CEIL n0))` asks CEIL for a f64, while it always returns an i64.
        let compile_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            compile(&expr, &variable_types)
        }));
        let Ok(Ok(compiled_fn)) = compile_result else {
            return Err(TestCaseError::reject("compilation failed"));
        };
        let condition = required(&expr);
        let mut string_arena = StringArena::default();
        for assignment in assignments {
            let variable_ord = |variable_name: &str| {
                VARIABLES
                    .iter()
                    .position(|(name, _)| *name == variable_name)
                    .unwrap()
            };
            let args: Vec<VariableValue> = compiled_fn
                .inputs()
                .iter()
                .map(
                    |input| match assignment[variable_ord(&input.variable_name)] {
                        Some(value_ord) => variable_value(input.r#type, value_ord),
                        None => VariableValue::none(),
                    },
                )
                .collect();
            // SAFETY: Each slot follows the compiled input order, and uses the input type.
            let result = unsafe { compiled_fn.call(&args, &mut string_arena) };
            if !holds(compiled_fn.result_type(), result) {
                continue;
            }
            let mut is_present = |variable_name: &str| {
                variable_types.contains_key(variable_name)
                    && assignment[variable_ord(variable_name)].is_some()
            };
            prop_assert!(
                condition.eval(&mut is_present),
                "{expr_str} holds for {assignment:?}, but {condition:?} does not"
            );
        }
        Ok(())
    }

    fn is_true(result_type: VarType, result: VariableValue) -> bool {
        // SAFETY: The union member is selected with the result type.
        result_type == VarType::Bool && unsafe { result.as_bool() } == Some(true)
    }

    fn is_present(result_type: VarType, result: VariableValue) -> bool {
        // SAFETY: The union member is selected with the result type.
        unsafe {
            match result_type {
                VarType::Bool => result.as_bool().is_some(),
                VarType::F64 => result.as_f64().is_some(),
                VarType::U64 => result.as_u64().is_some(),
                VarType::I64 => result.as_i64().is_some(),
                VarType::Str => result.as_str().is_some(),
                VarType::None => false,
            }
        }
    }

    proptest! {
        // Compiler debug assertions reject a fraction of the generated expressions.
        #![proptest_config(ProptestConfig {
            max_global_rejects: 1 << 16,
            ..ProptestConfig::with_cases(512)
        })]

        #[test]
        fn proptest_required_presence_for_true_is_necessary(
            expr in exprs(3).boolean,
            assignments in assignments(),
        ) {
            check_necessary_condition(
                &expr,
                InferredTypeSet::BOOLEAN,
                &assignments,
                required_presence_for_true,
                is_true,
            )?;
        }

        #[test]
        fn proptest_required_presence_is_necessary(
            expr in prop_oneof![exprs(3).boolean, exprs(3).number, exprs(3).string],
            assignments in assignments(),
        ) {
            check_necessary_condition(
                &expr,
                InferredTypeSet::ALL,
                &assignments,
                required_presence,
                is_present,
            )?;
        }
    }
}
