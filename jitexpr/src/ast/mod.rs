use std::collections::HashSet;

mod boilerplate;

/// A literal supported by the first expression-language milestone.
#[derive(Clone, Debug, PartialEq)]
pub enum Literal {
    Bool(bool),
    I64(i64),
    F64(f64),
    String(String),
}

/// An expression independent from its protobuf representation.
#[derive(Clone, Debug, PartialEq)]
pub enum Expr {
    Literal(Literal),
    Variable(String),
    Call { function: Function, args: Vec<Expr> },
}

impl Expr {
    pub fn literal(val: impl Into<Literal>) -> Expr {
        Expr::Literal(val.into())
    }

    pub fn variable(variable_name: impl ToString) -> Expr {
        Expr::Variable(variable_name.to_string())
    }
}

/// A function supported by the first expression-language milestone.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum Function {
    Add,
}

impl Function {
    pub fn call_expr(&self, args: Vec<Expr>) -> Expr {
        Expr::Call {
            function: *self,
            args,
        }
    }
}

impl Expr {
    pub fn list_variable_names(&self) -> HashSet<String> {
        let mut names = HashSet::new();
        match self {
            Expr::Literal(_) => {}
            Expr::Variable(name) => {
                names.insert(name.clone());
            }
            Expr::Call { args, .. } => {
                for arg in args {
                    names.extend(arg.list_variable_names());
                }
            }
        }
        names
    }
}
