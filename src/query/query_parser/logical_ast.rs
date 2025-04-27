use std::fmt;
use std::ops::Bound;

use crate::query::Occur;
use crate::schema::Term;
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
    pub fn boost(self, boost: Score) -> Self {
        if (boost - 1.0).abs() < Score::EPSILON {
            self
        } else {
            Self::Boost(Box::new(self), boost)
        }
    }

    pub fn simplify(self) -> Self {
        match self {
            Self::Clause(clauses) => {
                let mut new_clauses: Vec<(Occur, Self)> = Vec::new();

                for (occur, sub_ast) in clauses {
                    let simplified_sub_ast = sub_ast.simplify();

                    // If clauses below have the same `Occur`, we can pull them up
                    match simplified_sub_ast {
                        Self::Clause(sub_clauses)
                            if (occur == Occur::Should || occur == Occur::Must)
                                && sub_clauses.iter().all(|(o, _)| *o == occur) =>
                        {
                            for sub_clause in sub_clauses {
                                new_clauses.push(sub_clause);
                            }
                        }
                        _ => new_clauses.push((occur, simplified_sub_ast)),
                    }
                }

                Self::Clause(new_clauses)
            }
            Self::Leaf(_) | Self::Boost(_, _) => self,
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
            Self::Clause(ref clause) => {
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
            Self::Boost(ref ast, boost) => write!(formatter, "{ast:?}^{boost}"),
            Self::Leaf(ref literal) => write!(formatter, "{literal:?}"),
        }
    }
}

impl From<LogicalLiteral> for LogicalAst {
    fn from(literal: LogicalLiteral) -> Self {
        Self::Leaf(Box::new(literal))
    }
}

impl fmt::Debug for LogicalLiteral {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match *self {
            Self::Term(ref term) => write!(formatter, "{term:?}"),
            Self::Phrase {
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
            Self::Range {
                ref lower,
                ref upper,
                ..
            } => write!(formatter, "({lower:?} TO {upper:?})"),
            Self::Set { ref elements, .. } => {
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
            Self::All => write!(formatter, "*"),
        }
    }
}
