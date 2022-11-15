use std::fmt;
use std::ops::Bound;

use crate::query::Occur;
use crate::schema::{Field, Term, Type};
use crate::Score;

#[derive(Clone)]
pub enum LogicalLiteral {
    Term(Term),
    Phrase(Vec<(usize, Term)>, u32),
    Range {
        field: Field,
        value_type: Type,
        lower: Bound<Term>,
        upper: Bound<Term>,
    },
    Set {
        field: Field,
        value_type: Type,
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
                    let (ref occur, ref subquery) = clause[0];
                    write!(formatter, "({}{:?}", occur_letter(*occur), subquery)?;
                    for &(ref occur, ref subquery) in &clause[1..] {
                        write!(formatter, " {}{:?}", occur_letter(*occur), subquery)?;
                    }
                    formatter.write_str(")")?;
                }
                Ok(())
            }
            LogicalAst::Boost(ref ast, boost) => write!(formatter, "{:?}^{}", ast, boost),
            LogicalAst::Leaf(ref literal) => write!(formatter, "{:?}", literal),
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
            LogicalLiteral::Term(ref term) => write!(formatter, "{:?}", term),
            LogicalLiteral::Phrase(ref terms, slop) => {
                write!(formatter, "\"{:?}\"", terms)?;
                if slop > 0 {
                    write!(formatter, "~{:?}", slop)
                } else {
                    Ok(())
                }
            }
            LogicalLiteral::Range {
                ref lower,
                ref upper,
                ..
            } => write!(formatter, "({:?} TO {:?})", lower, upper),
            LogicalLiteral::Set { ref elements, .. } => {
                const MAX_DISPLAYED: usize = 10;

                write!(formatter, "IN [")?;
                for (i, element) in elements.iter().enumerate() {
                    if i == 0 {
                        write!(formatter, "{:?}", element)?;
                    } else if i == MAX_DISPLAYED - 1 {
                        write!(
                            formatter,
                            ", {:?}, ... ({} more)",
                            element,
                            elements.len() - i - 1
                        )?;
                        break;
                    } else {
                        write!(formatter, ", {:?}", element)?;
                    }
                }
                write!(formatter, "]")
            }
            LogicalLiteral::All => write!(formatter, "*"),
        }
    }
}
