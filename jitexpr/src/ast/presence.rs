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
/// Conditions built with [`PresenceCondition::all`] and [`PresenceCondition::any`] are simplified:
/// `All` and `Any` then have at least two distinct children, none of which is `Always`, `Never`,
/// or a node of the same kind.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PresenceCondition {
    /// Always satisfied. This condition carries no information.
    Always,
    /// Never satisfied.
    Never,
    /// Satisfied when the variable is present.
    Present(Arc<str>),
    /// Satisfied when all of the conditions are satisfied.
    All(Vec<PresenceCondition>),
    /// Satisfied when at least one of the conditions is satisfied.
    Any(Vec<PresenceCondition>),
}

impl PresenceCondition {
    /// Builds the simplified conjunction of `conditions`.
    pub fn all(conditions: impl IntoIterator<Item = PresenceCondition>) -> PresenceCondition {
        let mut children: Vec<PresenceCondition> = Vec::new();
        for condition in conditions {
            match condition {
                PresenceCondition::Always => {}
                PresenceCondition::Never => return PresenceCondition::Never,
                PresenceCondition::All(grand_children) => {
                    for grand_child in grand_children {
                        push_unique(&mut children, grand_child);
                    }
                }
                condition => push_unique(&mut children, condition),
            }
        }
        match children.len() {
            0 => PresenceCondition::Always,
            1 => children.pop().unwrap(),
            _ => PresenceCondition::All(children),
        }
    }

    /// Builds the simplified disjunction of `conditions`.
    pub fn any(conditions: impl IntoIterator<Item = PresenceCondition>) -> PresenceCondition {
        let mut children: Vec<PresenceCondition> = Vec::new();
        for condition in conditions {
            match condition {
                PresenceCondition::Never => {}
                PresenceCondition::Always => return PresenceCondition::Always,
                PresenceCondition::Any(grand_children) => {
                    for grand_child in grand_children {
                        push_unique(&mut children, grand_child);
                    }
                }
                condition => push_unique(&mut children, condition),
            }
        }
        match children.len() {
            0 => PresenceCondition::Never,
            1 => children.pop().unwrap(),
            _ => PresenceCondition::Any(children),
        }
    }

    /// Evaluates the condition, given the presence of each variable.
    pub fn eval(&self, is_present: &mut impl FnMut(&str) -> bool) -> bool {
        match self {
            PresenceCondition::Always => true,
            PresenceCondition::Never => false,
            PresenceCondition::Present(variable_name) => is_present(variable_name),
            PresenceCondition::All(conditions) => conditions
                .iter()
                .all(|condition| condition.eval(&mut *is_present)),
            PresenceCondition::Any(conditions) => conditions
                .iter()
                .any(|condition| condition.eval(&mut *is_present)),
        }
    }
}

fn push_unique(conditions: &mut Vec<PresenceCondition>, condition: PresenceCondition) {
    if !conditions.contains(&condition) {
        conditions.push(condition);
    }
}

/// Returns a necessary condition for `expr` to evaluate to a present value.
///
/// A variable is considered present when its value is not null. A variable missing from the
/// variable types given to the compiler is null for all evaluations, and should therefore be
/// considered absent.
pub fn required_presence(expr: &UntypedExpr) -> PresenceCondition {
    match expr {
        // Literals never constrain variables. `none` could be mapped to `Never`, but some
        // functions take literal configuration arguments (delimiters, precision, ...), and we
        // prefer not to depend on how each of them handles a `none` there.
        UntypedExpr::Literal(_) => PresenceCondition::Always,
        UntypedExpr::Variable(variable_name) => PresenceCondition::Present(variable_name.clone()),
        UntypedExpr::FnCall { function, args } => required_presence_for_fn_call(*function, args),
    }
}

/// Returns a necessary condition for `expr` to evaluate to a present `true`.
///
/// See [`required_presence`] for the definition of a present variable.
pub fn required_presence_for_true(expr: &UntypedExpr) -> PresenceCondition {
    match expr {
        UntypedExpr::Literal(Literal::Bool(true)) => PresenceCondition::Always,
        // No other literal is `true`.
        UntypedExpr::Literal(_) => PresenceCondition::Never,
        UntypedExpr::Variable(variable_name) => PresenceCondition::Present(variable_name.clone()),
        UntypedExpr::FnCall { function, args } => {
            required_presence_for_true_for_fn_call(*function, args)
        }
    }
}

fn required_presence_for_fn_call(function: Function, args: &[UntypedExpr]) -> PresenceCondition {
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
        | Function::Upper => PresenceCondition::all(args.iter().map(required_presence)),
        // OR is null only if all of its arguments are null.
        Function::Or => PresenceCondition::any(args.iter().map(required_presence)),
        // IF is null if its condition is null. Otherwise it takes the presence of the selected
        // branch.
        Function::If => {
            let [condition, when_true, when_false] = args else {
                return PresenceCondition::Always;
            };
            PresenceCondition::all([
                required_presence(condition),
                PresenceCondition::any([
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
        | Function::RegexpLike => PresenceCondition::Always,
    }
}

fn required_presence_for_true_for_fn_call(
    function: Function,
    args: &[UntypedExpr],
) -> PresenceCondition {
    match function {
        Function::And => PresenceCondition::all(args.iter().map(required_presence_for_true)),
        Function::Or => PresenceCondition::any(args.iter().map(required_presence_for_true)),
        Function::If => {
            let [condition, when_true, when_false] = args else {
                return PresenceCondition::Always;
            };
            PresenceCondition::all([
                required_presence(condition),
                PresenceCondition::any([
                    required_presence_for_true(when_true),
                    required_presence_for_true(when_false),
                ]),
            ])
        }
        Function::IsNotNull => {
            let [arg] = args else {
                return PresenceCondition::Always;
            };
            required_presence(arg)
        }
        // REGEXP_LIKE returns `false` for a null input.
        Function::RegexpLike => {
            let Some(input) = args.first() else {
                return PresenceCondition::Always;
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

    use proptest::prelude::*;
    use proptest::strategy::BoxedStrategy;

    use super::*;
    use crate::ast::{InferredTypeSet, deserialize, infer_types_with_target};
    use crate::compile::{StringArena, compile};
    use crate::types::{VarType, VariableValue};

    fn present(variable_name: &str) -> PresenceCondition {
        PresenceCondition::Present(Arc::from(variable_name))
    }

    fn all(conditions: Vec<PresenceCondition>) -> PresenceCondition {
        PresenceCondition::All(conditions)
    }

    fn any(conditions: Vec<PresenceCondition>) -> PresenceCondition {
        PresenceCondition::Any(conditions)
    }

    fn for_true(expr: &str) -> PresenceCondition {
        required_presence_for_true(&deserialize(expr).unwrap())
    }

    fn for_value(expr: &str) -> PresenceCondition {
        required_presence(&deserialize(expr).unwrap())
    }

    #[test]
    fn test_all_simplification() {
        assert_eq!(PresenceCondition::all([]), PresenceCondition::Always);
        assert_eq!(
            PresenceCondition::all([PresenceCondition::Always, present("a")]),
            present("a")
        );
        assert_eq!(
            PresenceCondition::all([present("a"), PresenceCondition::Never]),
            PresenceCondition::Never
        );
        assert_eq!(
            PresenceCondition::all([
                present("a"),
                all(vec![present("b"), present("a")]),
                present("c"),
            ]),
            all(vec![present("a"), present("b"), present("c")])
        );
        assert_eq!(
            PresenceCondition::all([any(vec![present("a"), present("b")]), present("c")]),
            all(vec![any(vec![present("a"), present("b")]), present("c")])
        );
    }

    #[test]
    fn test_any_simplification() {
        assert_eq!(PresenceCondition::any([]), PresenceCondition::Never);
        assert_eq!(
            PresenceCondition::any([PresenceCondition::Never, present("a")]),
            present("a")
        );
        assert_eq!(
            PresenceCondition::any([present("a"), PresenceCondition::Always]),
            PresenceCondition::Always
        );
        assert_eq!(
            PresenceCondition::any([present("a"), any(vec![present("b"), present("a")])]),
            any(vec![present("a"), present("b")])
        );
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
        assert!(PresenceCondition::Always.eval(&mut presence_of(&[])));
        assert!(!PresenceCondition::Never.eval(&mut presence_of(&["a"])));
    }

    #[test]
    fn test_literals() {
        assert_eq!(for_true("true"), PresenceCondition::Always);
        assert_eq!(for_true("false"), PresenceCondition::Never);
        assert_eq!(for_true("none"), PresenceCondition::Never);
        assert_eq!(for_value("none"), PresenceCondition::Always);
        assert_eq!(for_value("1u64"), PresenceCondition::Always);
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
        assert_eq!(
            for_value(r#"(REGEXP_EXTRACT a "(x+)" 1u64)"#),
            present("a")
        );
        assert_eq!(for_value("(ADD)"), PresenceCondition::Always);
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
        assert_eq!(
            for_value("(AND (NOT a) b)"),
            present("b")
        );
        assert_eq!(
            for_value("(OR (EQ a 1i64) (EQ b 2i64))"),
            any(vec![present("a"), present("b")])
        );
    }

    #[test]
    fn test_null_tolerant_functions() {
        assert_eq!(for_true("(NOT (EQ a 1i64))"), PresenceCondition::Always);
        assert_eq!(for_true("(NEQ a 1i64)"), PresenceCondition::Always);
        assert_eq!(for_true("(IS_NULL a)"), PresenceCondition::Always);
        assert_eq!(for_value("(IS_NOT_NULL a)"), PresenceCondition::Always);
        assert_eq!(for_true("(IS_NOT_NULL (ADD a b))"), all(vec![present("a"), present("b")]));
        assert_eq!(for_value(r#"(REGEXP_LIKE a "x")"#), PresenceCondition::Always);
        assert_eq!(for_true(r#"(REGEXP_LIKE a "x")"#), present("a"));
        assert_eq!(
            for_true("(OR (EQ a 1i64) (IS_NULL b))"),
            PresenceCondition::Always
        );
        assert_eq!(
            for_true("(AND (EQ a 1i64) (IS_NULL b))"),
            present("a")
        );
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
        assert_eq!(for_true("(IF c false false)"), PresenceCondition::Never);
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
        required: fn(&UntypedExpr) -> PresenceCondition,
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
                .map(|input| {
                    match assignment[variable_ord(&input.variable_name)] {
                        Some(value_ord) => variable_value(input.r#type, value_ord),
                        None => VariableValue::none(),
                    }
                })
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
        #![proptest_config(ProptestConfig::with_cases(512))]

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
