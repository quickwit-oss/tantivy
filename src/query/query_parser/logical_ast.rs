use crate::query::Occur;
use crate::schema::Field;
use crate::schema::Term;
use crate::schema::Type;
use crate::Score;
use std::fmt;
use std::ops::Bound;

#[derive(Clone)]
pub enum LogicalLiteral {
    Term(Term),
    Phrase(Vec<(usize, Term)>),
    Range {
        field: Field,
        value_type: Type,
        lower: Bound<Term>,
        upper: Bound<Term>,
    },
    All,
}

pub enum LogicalAST {
    Clause(Vec<(Occur, LogicalAST)>),
    Leaf(Box<LogicalLiteral>),
    Boost(Box<LogicalAST>, Score),
}

impl LogicalAST {
    pub fn boost(self, boost: Score) -> LogicalAST {
        if (boost - 1.0).abs() < Score::EPSILON {
            self
        } else {
            LogicalAST::Boost(Box::new(self), boost)
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

impl fmt::Debug for LogicalAST {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match *self {
            LogicalAST::Clause(ref clause) => {
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
            LogicalAST::Boost(ref ast, boost) => write!(formatter, "{:?}^{}", ast, boost),
            LogicalAST::Leaf(ref literal) => write!(formatter, "{:?}", literal),
        }
    }
}

impl From<LogicalLiteral> for LogicalAST {
    fn from(literal: LogicalLiteral) -> LogicalAST {
        LogicalAST::Leaf(Box::new(literal))
    }
}

impl fmt::Debug for LogicalLiteral {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match *self {
            LogicalLiteral::Term(ref term) => write!(formatter, "{:?}", term),
            LogicalLiteral::Phrase(ref terms) => write!(formatter, "\"{:?}\"", terms),
            LogicalLiteral::Range {
                ref lower,
                ref upper,
                ..
            } => write!(formatter, "({:?} TO {:?})", lower, upper),
            LogicalLiteral::All => write!(formatter, "*"),
        }
    }
}
