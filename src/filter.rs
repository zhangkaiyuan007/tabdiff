//! Row filtering for --where: a small conjunctive predicate language,
//! `col OP value [AND col OP value ...]` with OP in = != < <= > >=.
//! Values are numbers, 'single-quoted strings' ('' escapes a quote),
//! true/false, or NULL. Deliberately not SQL: no parens, no OR, no deps.

use std::sync::Arc;

use anyhow::{Context, Result, bail};
use arrow::array::{ArrayRef, BooleanArray, RecordBatch};
use arrow::compute::filter_record_batch;
use arrow::error::ArrowError;

use crate::input::BatchIter;
use crate::value::{Cell, Comparator, cmp_cells, extract};

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Op {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

#[derive(Debug)]
struct Clause {
    column: String,
    op: Op,
    value: Cell,
}

#[derive(Debug)]
pub struct Predicate {
    clauses: Vec<Clause>,
}

impl Predicate {
    pub fn parse(expr: &str) -> Result<Self> {
        let tokens = tokenize(expr).with_context(|| format!("cannot parse --where `{expr}`"))?;
        let mut clauses = vec![];
        let mut it = tokens.into_iter().peekable();
        loop {
            let column = match it.next() {
                Some(Token::Ident(c)) => c,
                other => bail!("expected a column name, found {other:?} in --where `{expr}`"),
            };
            let op = match it.next() {
                Some(Token::Op(op)) => op,
                other => bail!("expected an operator after `{column}`, found {other:?}"),
            };
            let value = match it.next() {
                Some(Token::Value(v)) => v,
                Some(Token::Ident(w)) => match w.to_ascii_lowercase().as_str() {
                    "true" => Cell::Bool(true),
                    "false" => Cell::Bool(false),
                    "null" => Cell::Null,
                    _ => bail!("unquoted value `{w}`; string literals need 'single quotes'"),
                },
                other => bail!("expected a value after the operator, found {other:?}"),
            };
            clauses.push(Clause { column, op, value });
            match it.next() {
                None => break,
                Some(Token::Ident(w)) if w.eq_ignore_ascii_case("and") => continue,
                other => bail!("expected AND or end of expression, found {other:?}"),
            }
        }
        Ok(Self { clauses })
    }

    pub fn columns(&self) -> Vec<&str> {
        self.clauses.iter().map(|c| c.column.as_str()).collect()
    }

    /// Wraps a batch stream, keeping only matching rows.
    pub fn apply(self: &Arc<Self>, src: BatchIter) -> Result<BatchIter> {
        let idx = self
            .clauses
            .iter()
            .map(|c| {
                src.schema
                    .index_of(&c.column)
                    .with_context(|| format!("--where column `{}` not found", c.column))
            })
            .collect::<Result<Vec<_>>>()?;
        let pred = self.clone();
        let schema = src.schema.clone();
        let iter = src.iter.map(move |batch| {
            let batch = batch?;
            pred.filter_batch(&batch, &idx)
                .map_err(|e| ArrowError::ComputeError(e.to_string()))
        });
        Ok(BatchIter {
            schema,
            iter: Box::new(iter),
        })
    }

    fn filter_batch(&self, batch: &RecordBatch, idx: &[usize]) -> Result<RecordBatch> {
        let arrays: Vec<ArrayRef> = idx.iter().map(|&i| batch.column(i).clone()).collect();
        let mut mask = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            let mut keep = true;
            for (clause, array) in self.clauses.iter().zip(&arrays) {
                if !clause.matches(&extract(array.as_ref(), row)?) {
                    keep = false;
                    break;
                }
            }
            mask.push(keep);
        }
        Ok(filter_record_batch(batch, &BooleanArray::from(mask))?)
    }
}

impl Clause {
    fn matches(&self, cell: &Cell) -> bool {
        let cmp = Comparator::default();
        match self.op {
            Op::Eq => cmp.eq(cell, &self.value),
            Op::Ne => !cmp.eq(cell, &self.value),
            // Ordering needs comparable classes: numeric vs numeric or
            // string vs string. Anything else (incl. NULL) never matches.
            Op::Lt | Op::Le | Op::Gt | Op::Ge => {
                if !ord_compatible(cell, &self.value) {
                    return false;
                }
                let ord = cmp_cells(cell, &self.value);
                match self.op {
                    Op::Lt => ord.is_lt(),
                    Op::Le => ord.is_le(),
                    Op::Gt => ord.is_gt(),
                    Op::Ge => ord.is_ge(),
                    _ => unreachable!(),
                }
            }
        }
    }
}

fn ord_compatible(a: &Cell, b: &Cell) -> bool {
    matches!(
        (a, b),
        (
            Cell::Int(_) | Cell::Float(_),
            Cell::Int(_) | Cell::Float(_)
        ) | (Cell::Str(_), Cell::Str(_))
    )
}

#[derive(Debug)]
enum Token {
    Ident(String),
    Op(Op),
    Value(Cell),
}

fn tokenize(s: &str) -> Result<Vec<Token>> {
    let mut tokens = vec![];
    let mut chars = s.chars().peekable();
    while let Some(&c) = chars.peek() {
        match c {
            c if c.is_whitespace() => {
                chars.next();
            }
            '\'' => {
                chars.next();
                let mut out = String::new();
                loop {
                    match chars.next() {
                        None => bail!("unterminated string literal"),
                        Some('\'') => {
                            // '' inside a string is an escaped quote.
                            if chars.peek() == Some(&'\'') {
                                chars.next();
                                out.push('\'');
                            } else {
                                break;
                            }
                        }
                        Some(ch) => out.push(ch),
                    }
                }
                tokens.push(Token::Value(Cell::Str(out)));
            }
            '=' => {
                chars.next();
                tokens.push(Token::Op(Op::Eq));
            }
            '!' => {
                chars.next();
                if chars.next() != Some('=') {
                    bail!("expected `!=`");
                }
                tokens.push(Token::Op(Op::Ne));
            }
            '<' => {
                chars.next();
                if chars.peek() == Some(&'=') {
                    chars.next();
                    tokens.push(Token::Op(Op::Le));
                } else {
                    tokens.push(Token::Op(Op::Lt));
                }
            }
            '>' => {
                chars.next();
                if chars.peek() == Some(&'=') {
                    chars.next();
                    tokens.push(Token::Op(Op::Ge));
                } else {
                    tokens.push(Token::Op(Op::Gt));
                }
            }
            c if c.is_ascii_digit() || c == '-' || c == '.' => {
                let mut num = String::new();
                while let Some(&ch) = chars.peek() {
                    if ch.is_ascii_digit() || ch == '.' || ch == '-' || ch == 'e' || ch == 'E' {
                        num.push(ch);
                        chars.next();
                    } else {
                        break;
                    }
                }
                let cell = if let Ok(i) = num.parse::<i64>() {
                    Cell::Int(i)
                } else if let Ok(f) = num.parse::<f64>() {
                    Cell::Float(f)
                } else {
                    bail!("invalid number `{num}`");
                };
                tokens.push(Token::Value(cell));
            }
            c if c.is_alphanumeric() || c == '_' => {
                let mut ident = String::new();
                while let Some(&ch) = chars.peek() {
                    if ch.is_alphanumeric() || ch == '_' || ch == '.' {
                        ident.push(ch);
                        chars.next();
                    } else {
                        break;
                    }
                }
                tokens.push(Token::Ident(ident));
            }
            other => bail!("unexpected character `{other}`"),
        }
    }
    Ok(tokens)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_conjunction() {
        let p = Predicate::parse("region = 'EU' AND amount >= 2.5 and n != 3").unwrap();
        assert_eq!(p.clauses.len(), 3);
        assert_eq!(p.clauses[0].column, "region");
        assert_eq!(p.clauses[0].op, Op::Eq);
        assert_eq!(p.clauses[0].value, Cell::Str("EU".into()));
        assert_eq!(p.clauses[1].op, Op::Ge);
        assert_eq!(p.clauses[1].value, Cell::Float(2.5));
        assert_eq!(p.clauses[2].value, Cell::Int(3));
    }

    #[test]
    fn parses_escaped_quote_and_keywords() {
        let p = Predicate::parse("name = 'O''Brien' AND active = true AND x = NULL").unwrap();
        assert_eq!(p.clauses[0].value, Cell::Str("O'Brien".into()));
        assert_eq!(p.clauses[1].value, Cell::Bool(true));
        assert_eq!(p.clauses[2].value, Cell::Null);
    }

    #[test]
    fn rejects_bad_input() {
        assert!(Predicate::parse("a =").is_err());
        assert!(Predicate::parse("a = 'x' OR b = 1").is_err());
        assert!(Predicate::parse("a = unquoted").is_err());
        assert!(Predicate::parse("= 5").is_err());
    }

    #[test]
    fn clause_semantics() {
        let p = Predicate::parse("x > 10").unwrap();
        assert!(p.clauses[0].matches(&Cell::Int(11)));
        assert!(p.clauses[0].matches(&Cell::Float(10.5)));
        assert!(!p.clauses[0].matches(&Cell::Int(10)));
        // Type-incompatible ordering never matches, including NULL.
        assert!(!p.clauses[0].matches(&Cell::Str("99".into())));
        assert!(!p.clauses[0].matches(&Cell::Null));
    }
}
