use std::any::Any;

use arrow::array::BooleanArray;
use arrow::datatypes::DataType;
use datafusion::common::{Result, ScalarValue};
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::ColumnarValue;
use datafusion::logical_expr::{Expr, ScalarFunctionArgs, ScalarUDF, Signature, Volatility};

/// A marker UDF for full-text search predicates.
///
/// `full_text(column, query_string)` returns Boolean and is meant to be
/// pushed down into `TantivyTableProvider` via `supports_filters_pushdown`.
/// The first argument must be a column reference (not a string literal) so
/// that DataFusion's optimizer recognizes it as a filter on that column and
/// pushes it down to the table provider.
///
/// If the filter is not pushed down (e.g. used outside a TantivyTableProvider),
/// it returns `true` for every row as a safe fallback.
///
/// Register with:
/// ```ignore
/// ctx.register_udf(full_text_udf());
/// ```
///
/// Then use in queries:
/// ```sql
/// SELECT * FROM t WHERE full_text(category, 'electronics')
/// ```
#[derive(Debug, PartialEq, Eq, Hash)]
struct FullTextUdf {
    signature: Signature,
}

impl FullTextUdf {
    fn new() -> Self {
        Self {
            signature: Signature::exact(
                vec![DataType::Utf8, DataType::Utf8],
                Volatility::Immutable,
            ),
        }
    }
}

impl datafusion::logical_expr::ScalarUDFImpl for FullTextUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "full_text"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        // Fallback: if not pushed down, return true for all rows.
        let num_rows = args.number_rows;
        Ok(ColumnarValue::Array(std::sync::Arc::new(
            BooleanArray::from(vec![true; num_rows]),
        )))
    }
}

/// Create the `full_text` scalar UDF for registration with a SessionContext.
pub fn full_text_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(FullTextUdf::new())
}

/// Extract `(field_name, query_string)` from an `Expr` if it is a
/// `full_text(column, query)` call. Returns `None` for any other expression.
///
/// The first argument can be a column reference (`full_text(category, ...)`)
/// or a string literal (`full_text('category', ...)`).
pub(crate) fn extract_full_text_call(expr: &Expr) -> Option<(String, String)> {
    if let Expr::ScalarFunction(ScalarFunction { func, args }) = expr {
        if func.name() != "full_text" || args.len() != 2 {
            return None;
        }
        let field_name = match &args[0] {
            Expr::Column(col) => col.name().to_string(),
            Expr::Literal(ScalarValue::Utf8(Some(s)), _) => s.clone(),
            _ => return None,
        };
        let query_string = match &args[1] {
            Expr::Literal(ScalarValue::Utf8(Some(s)), _) => s.clone(),
            _ => return None,
        };
        Some((field_name, query_string))
    } else {
        None
    }
}
