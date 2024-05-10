use std::fmt;
use std::ops::Bound;

use crate::query::Occur;
use crate::schema::{Term, Type};
use crate::Score;

#[derive(Clone)]
pub enum LogicalLiteral {
    Term(Term),
    Phrase {
        terms: Vec<(usize, Term)>,
        slop: u32,
        prefix: bool,
    },
    Range {
        field: String,
        value_type: Type,
        lower: Bound<Term>,
        upper: Bound<Term>,
    },
    Set {
        elements: Vec<Term>,
    },
    All,
}

pub enum LogicalAst {
    Clause(Vec<(Occur, LogicalAst)>),
    Leaf(Box<LogicalLiteral>),
    Boost(Box<LogicalAst>, Score),
}

impl LogicalAst {
    pub fn boost(self, boost: Score) -> LogicalAst {
        if (boost - 1.0).abs() < Score::EPSILON {
            self
        } else {
            LogicalAst::Boost(Box::new(self), boost)
        }
    }
}

fn occur_letter(occur: Occur) -> &'static str {
    match occur {
        Occur::Must => "+",
        Occur::MustNot => "-",
        Occur::Should => "",
    }
}

impl fmt::Debug for LogicalAst {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match *self {
            LogicalAst::Clause(ref clause) => {
                if clause.is_empty() {
                    write!(formatter, "<emptyclause>")?;
                } else {
                    let (occur, subquery) = &clause[0];
                    write!(formatter, "({}{subquery:?}", occur_letter(*occur))?;
                    for (occur, subquery) in &clause[1..] {
                        write!(formatter, " {}{subquery:?}", occur_letter(*occur))?;
                    }
                    formatter.write_str(")")?;
                }
                Ok(())
            }
            LogicalAst::Boost(ref ast, boost) => write!(formatter, "{ast:?}^{boost}"),
            LogicalAst::Leaf(ref literal) => write!(formatter, "{literal:?}"),
        }
    }
}

impl From<LogicalLiteral> for LogicalAst {
    fn from(literal: LogicalLiteral) -> LogicalAst {
        LogicalAst::Leaf(Box::new(literal))
    }
}

impl fmt::Debug for LogicalLiteral {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match *self {
            LogicalLiteral::Term(ref term) => write!(formatter, "{term:?}"),
            LogicalLiteral::Phrase {
                ref terms,
                slop,
                prefix,
            } => {
                write!(formatter, "\"{terms:?}\"")?;
                if slop > 0 {
                    write!(formatter, "~{slop:?}")
                } else if prefix {
                    write!(formatter, "*")
                } else {
                    Ok(())
                }
            }
            LogicalLiteral::Range {
                ref lower,
                ref upper,
                ..
            } => write!(formatter, "({lower:?} TO {upper:?})"),
            LogicalLiteral::Set { ref elements, .. } => {
                const MAX_DISPLAYED: usize = 10;

                write!(formatter, "IN [")?;
                for (i, element) in elements.iter().enumerate() {
                    if i == 0 {
                        write!(formatter, "{element:?}")?;
                    } else if i == MAX_DISPLAYED - 1 {
                        write!(
                            formatter,
                            ", {element:?}, ... ({} more)",
                            elements.len() - i - 1
                        )?;
                        break;
                    } else {
                        write!(formatter, ", {element:?}")?;
                    }
                }
                write!(formatter, "]")
            }
            LogicalLiteral::All => write!(formatter, "*"),
        }
    }
}
