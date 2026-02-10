use std::ops::Bound;

use datafusion::common::{Result, ScalarValue};
use datafusion::logical_expr::expr::InList;
use datafusion::logical_expr::{BinaryExpr, Expr, Operator};
use tantivy::query::{
    AllQuery, BooleanQuery, ExistsQuery, Occur, Query, RangeQuery, RegexQuery, TermQuery,
    TermSetQuery,
};
use tantivy::schema::{Field, FieldType, IndexRecordOption, Schema, Term};

/// Try to convert a DataFusion filter expression into a tantivy query.
///
/// Returns `Ok(Some(query))` if the expression can be fully converted,
/// `Ok(None)` if the expression type is not supported for pushdown.
pub fn df_expr_to_tantivy_query(
    expr: &Expr,
    schema: &Schema,
) -> Result<Option<Box<dyn Query>>> {
    match expr {
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
            convert_binary_expr(left, *op, right, schema)
        }
        Expr::Not(inner) => convert_not(inner, schema),
        Expr::Between(between) => convert_between(between, schema),
        Expr::InList(in_list) => convert_in_list(in_list, schema),
        Expr::IsNotNull(inner) => convert_is_not_null(inner, schema),
        Expr::IsNull(inner) => convert_is_null(inner, schema),
        Expr::Like(like) if !like.case_insensitive => convert_like(like, schema),
        _ => Ok(None),
    }
}

/// Check if an expression can be converted to a tantivy query (without doing it).
pub fn can_convert_expr(expr: &Expr, schema: &Schema) -> bool {
    match expr {
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => match op {
            Operator::And | Operator::Or => {
                can_convert_expr(left, schema) && can_convert_expr(right, schema)
            }
            Operator::Eq | Operator::NotEq
            | Operator::Lt | Operator::LtEq
            | Operator::Gt | Operator::GtEq => {
                is_column_lit_pair(left, right, schema)
                    || is_column_lit_pair(right, left, schema)
            }
            _ => false,
        },
        Expr::Not(inner) => can_convert_expr(inner, schema),
        Expr::Between(between) => {
            matches!(between.expr.as_ref(), Expr::Column(col) if schema.get_field(&col.name).is_ok())
                && matches!(between.low.as_ref(), Expr::Literal(_))
                && matches!(between.high.as_ref(), Expr::Literal(_))
        }
        Expr::InList(in_list) => {
            matches!(in_list.expr.as_ref(), Expr::Column(col) if schema.get_field(&col.name).is_ok())
                && in_list.list.iter().all(|e| matches!(e, Expr::Literal(_)))
        }
        Expr::IsNotNull(inner) | Expr::IsNull(inner) => {
            matches!(inner.as_ref(), Expr::Column(col) if schema.get_field(&col.name).is_ok())
        }
        Expr::Like(like) if !like.case_insensitive => {
            matches!(like.expr.as_ref(), Expr::Column(col) if schema.get_field(&col.name).is_ok())
                && matches!(like.pattern.as_ref(), Expr::Literal(ScalarValue::Utf8(Some(_))))
        }
        _ => false,
    }
}

// --- Conversion helpers ---

fn is_column_lit_pair(a: &Expr, b: &Expr, schema: &Schema) -> bool {
    matches!(a, Expr::Column(col) if schema.get_field(&col.name).is_ok())
        && matches!(b, Expr::Literal(_))
}

fn convert_not(inner: &Expr, schema: &Schema) -> Result<Option<Box<dyn Query>>> {
    let Some(inner_query) = df_expr_to_tantivy_query(inner, schema)? else {
        return Ok(None);
    };
    Ok(Some(negate(inner_query)))
}

/// NOT q → BooleanQuery(Must[AllQuery], MustNot[q])
fn negate(query: Box<dyn Query>) -> Box<dyn Query> {
    Box::new(BooleanQuery::new(vec![
        (Occur::Must, Box::new(AllQuery) as Box<dyn Query>),
        (Occur::MustNot, query),
    ]))
}

fn convert_between(
    between: &datafusion::logical_expr::Between,
    schema: &Schema,
) -> Result<Option<Box<dyn Query>>> {
    let Expr::Column(col) = between.expr.as_ref() else {
        return Ok(None);
    };
    let Ok(field) = schema.get_field(&col.name) else {
        return Ok(None);
    };
    let Some(lo_term) = scalar_to_term(field, &between.low, schema)? else {
        return Ok(None);
    };
    let Some(hi_term) = scalar_to_term(field, &between.high, schema)? else {
        return Ok(None);
    };
    let query: Box<dyn Query> = Box::new(RangeQuery::new(
        Bound::Included(lo_term),
        Bound::Included(hi_term),
    ));
    if between.negated {
        Ok(Some(negate(query)))
    } else {
        Ok(Some(query))
    }
}

/// `col IN (v1, v2, ...)` → TermSetQuery
/// `col NOT IN (v1, v2, ...)` → NOT TermSetQuery
fn convert_in_list(
    in_list: &InList,
    schema: &Schema,
) -> Result<Option<Box<dyn Query>>> {
    let Expr::Column(col) = in_list.expr.as_ref() else {
        return Ok(None);
    };
    let Ok(field) = schema.get_field(&col.name) else {
        return Ok(None);
    };
    let mut terms = Vec::with_capacity(in_list.list.len());
    for item in &in_list.list {
        let Some(term) = scalar_to_term(field, item, schema)? else {
            return Ok(None);
        };
        terms.push(term);
    }
    let query: Box<dyn Query> = Box::new(TermSetQuery::new(terms));
    if in_list.negated {
        Ok(Some(negate(query)))
    } else {
        Ok(Some(query))
    }
}

/// `col IS NOT NULL` → ExistsQuery
fn convert_is_not_null(inner: &Expr, schema: &Schema) -> Result<Option<Box<dyn Query>>> {
    let Expr::Column(col) = inner else {
        return Ok(None);
    };
    if schema.get_field(&col.name).is_err() {
        return Ok(None);
    }
    Ok(Some(Box::new(ExistsQuery::new(col.name.clone(), false))))
}

/// `col IS NULL` → NOT ExistsQuery
fn convert_is_null(inner: &Expr, schema: &Schema) -> Result<Option<Box<dyn Query>>> {
    let Some(exists) = convert_is_not_null(inner, schema)? else {
        return Ok(None);
    };
    Ok(Some(negate(exists)))
}

/// `col LIKE pattern` → RegexQuery (translate SQL wildcards to regex)
fn convert_like(
    like: &datafusion::logical_expr::Like,
    schema: &Schema,
) -> Result<Option<Box<dyn Query>>> {
    let Expr::Column(col) = like.expr.as_ref() else {
        return Ok(None);
    };
    let Ok(field) = schema.get_field(&col.name) else {
        return Ok(None);
    };
    let Expr::Literal(ScalarValue::Utf8(Some(pattern))) = like.pattern.as_ref() else {
        return Ok(None);
    };
    let regex_pattern = sql_like_to_regex(pattern, like.escape_char);
    let regex_query = RegexQuery::from_pattern(&regex_pattern, field)
        .map_err(|e| datafusion::error::DataFusionError::Internal(format!("regex: {e}")))?;
    let query: Box<dyn Query> = Box::new(regex_query);
    if like.negated {
        Ok(Some(negate(query)))
    } else {
        Ok(Some(query))
    }
}

/// Convert a SQL LIKE pattern to a regex pattern.
/// `%` → `.*`, `_` → `.`, literal chars are escaped.
fn sql_like_to_regex(pattern: &str, escape_char: Option<char>) -> String {
    let mut regex = String::with_capacity(pattern.len() + 2);
    regex.push('^');
    let mut chars = pattern.chars();
    while let Some(c) = chars.next() {
        if Some(c) == escape_char {
            if let Some(next) = chars.next() {
                escape_regex_char(next, &mut regex);
            }
        } else {
            match c {
                '%' => regex.push_str(".*"),
                '_' => regex.push('.'),
                _ => escape_regex_char(c, &mut regex),
            }
        }
    }
    regex.push('$');
    regex
}

fn escape_regex_char(c: char, out: &mut String) {
    match c {
        '.' | '+' | '*' | '?' | '(' | ')' | '[' | ']' | '{' | '}' | '\\' | '|' | '^' | '$' => {
            out.push('\\');
            out.push(c);
        }
        _ => out.push(c),
    }
}

fn convert_binary_expr(
    left: &Expr,
    op: Operator,
    right: &Expr,
    schema: &Schema,
) -> Result<Option<Box<dyn Query>>> {
    match op {
        Operator::And => {
            let lq = df_expr_to_tantivy_query(left, schema)?;
            let rq = df_expr_to_tantivy_query(right, schema)?;
            match (lq, rq) {
                (Some(l), Some(r)) => {
                    Ok(Some(Box::new(BooleanQuery::intersection(vec![l, r]))))
                }
                _ => Ok(None),
            }
        }
        Operator::Or => {
            let lq = df_expr_to_tantivy_query(left, schema)?;
            let rq = df_expr_to_tantivy_query(right, schema)?;
            match (lq, rq) {
                (Some(l), Some(r)) => {
                    Ok(Some(Box::new(BooleanQuery::union(vec![l, r]))))
                }
                _ => Ok(None),
            }
        }
        _ => convert_comparison(left, op, right, schema),
    }
}

/// Convert `col <op> lit` or `lit <op> col` to a tantivy query.
fn convert_comparison(
    left: &Expr,
    op: Operator,
    right: &Expr,
    schema: &Schema,
) -> Result<Option<Box<dyn Query>>> {
    // Normalize to (column, op, literal)
    let (col_name, op, lit_expr) = match (left, right) {
        (Expr::Column(col), lit @ Expr::Literal(_)) => (&col.name, op, lit),
        (lit @ Expr::Literal(_), Expr::Column(col)) => (&col.name, flip_op(op), lit),
        _ => return Ok(None),
    };

    let Ok(field) = schema.get_field(col_name) else {
        return Ok(None);
    };
    let Some(term) = scalar_to_term(field, lit_expr, schema)? else {
        return Ok(None);
    };

    let query: Box<dyn Query> = match op {
        Operator::Eq => Box::new(TermQuery::new(term, IndexRecordOption::Basic)),
        Operator::NotEq => {
            negate(Box::new(TermQuery::new(term, IndexRecordOption::Basic)))
        }
        Operator::Gt => Box::new(RangeQuery::new(
            Bound::Excluded(term),
            Bound::Unbounded,
        )),
        Operator::GtEq => Box::new(RangeQuery::new(
            Bound::Included(term),
            Bound::Unbounded,
        )),
        Operator::Lt => Box::new(RangeQuery::new(
            Bound::Unbounded,
            Bound::Excluded(term),
        )),
        Operator::LtEq => Box::new(RangeQuery::new(
            Bound::Unbounded,
            Bound::Included(term),
        )),
        _ => return Ok(None),
    };

    Ok(Some(query))
}

fn flip_op(op: Operator) -> Operator {
    match op {
        Operator::Lt => Operator::Gt,
        Operator::LtEq => Operator::GtEq,
        Operator::Gt => Operator::Lt,
        Operator::GtEq => Operator::LtEq,
        other => other, // Eq, NotEq are symmetric
    }
}

/// Convert a DataFusion literal expression to a tantivy Term for the given field.
fn scalar_to_term(field: Field, expr: &Expr, schema: &Schema) -> Result<Option<Term>> {
    let Expr::Literal(scalar) = expr else {
        return Ok(None);
    };
    let field_entry = schema.get_field_entry(field);
    let term = match field_entry.field_type() {
        FieldType::U64(_) => match scalar {
            ScalarValue::UInt64(Some(v)) => Term::from_field_u64(field, *v),
            ScalarValue::Int64(Some(v)) if *v >= 0 => Term::from_field_u64(field, *v as u64),
            _ => return Ok(None),
        },
        FieldType::I64(_) => match scalar {
            ScalarValue::Int64(Some(v)) => Term::from_field_i64(field, *v),
            ScalarValue::UInt64(Some(v)) => Term::from_field_i64(field, *v as i64),
            _ => return Ok(None),
        },
        FieldType::F64(_) => match scalar {
            ScalarValue::Float64(Some(v)) => Term::from_field_f64(field, *v),
            _ => return Ok(None),
        },
        // Bool: tantivy TermQuery/RangeQuery don't support bool weight creation,
        // so we let DataFusion handle bool filters post-scan.
        FieldType::Bool(_) => return Ok(None),
        FieldType::Str(_) => match scalar {
            ScalarValue::Utf8(Some(v)) => Term::from_field_text(field, v),
            _ => return Ok(None),
        },
        _ => return Ok(None),
    };
    Ok(Some(term))
}
