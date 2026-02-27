//! AQL — Arcane Query Language Parser.
//!
//! This is a parser for the Arcane Query Language.
//! It provides a way to parse and validate AQL statements.

use crate::error::{ArcaneError, Result};
use crate::storage::{FieldDef, FieldType, Value};

#[derive(Debug, Clone)]
pub enum Statement {
    CreateBucket {
        name: String,
        fields: Vec<FieldDef>,
        unique: bool,
        forced: bool,
    },
    Insert {
        bucket: String,
        values: Vec<(Option<String>, Value)>,
    },
    BatchInsert {
        bucket: String,
        rows: Vec<Vec<(Option<String>, Value)>>,
    },
    Bulk {
        statements: Vec<Statement>,
    },
    Get {
        bucket: String,
        projection: Projection,
        filter: Option<Filter>,
        order_by: Option<OrderBy>,
    },
    Delete {
        bucket: String,
        filter: Filter,
    },
    Set {
        bucket: String,
        values: Vec<(Option<String>, Value)>,
        filter: Filter,
    },
    Truncate {
        bucket: String,
    },
    Describe {
        bucket: String,
    },
    Commit,
}

#[derive(Debug, Clone)]
pub enum Projection {
    Star,
    Hash,
    Head(usize),
    Tail(usize),
    Fields(Vec<String>),
    Aggregates(Vec<AggregateFunc>),
}

#[derive(Debug, Clone)]
pub enum AggregateFunc {
    Avg(String),
    Sum(String),
    Min(String),
    Max(String),
    Median(String),
    Stddev(String),
    Count(Option<String>),
}

#[derive(Debug, Clone, PartialEq)]
pub enum SortOrder {
    Asc,
    Desc,
}

#[derive(Debug, Clone)]
pub struct OrderBy {
    pub field: String,
    pub order: SortOrder,
}

#[derive(Debug, Clone, PartialEq)]
pub enum CompareOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    Like,
}

#[derive(Debug, Clone)]
pub enum Filter {
    Simple {
        field: String,
        op: CompareOp,
        value: Value,
    },
    And(Box<Filter>, Box<Filter>),
    Or(Box<Filter>, Box<Filter>),
}

#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    Ident(String),
    StringLit(String),
    IntLit(i64),
    FloatLit(f64),
    Bool(bool),
    Null,
    Star,
    Comma,
    Colon,
    Semicolon,
    LParen,
    RParen,
    LBracket,
    RBracket,
    LBrace,
    RBrace,
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    And,
    Or,
    Like,
    Eof,
}

struct Lexer<'a> {
    src: &'a str,
    pos: usize,
}

impl<'a> Lexer<'a> {
    fn new(src: &'a str) -> Self {
        Lexer { src, pos: 0 }
    }

    fn peek_char(&self) -> Option<char> {
        self.src[self.pos..].chars().next()
    }

    fn advance(&mut self) -> Option<char> {
        let c = self.peek_char()?;
        self.pos += c.len_utf8();
        Some(c)
    }

    fn skip_whitespace(&mut self) {
        while let Some(c) = self.peek_char() {
            if c.is_whitespace() {
                self.advance();
            } else {
                break;
            }
        }
    }

    fn read_string(&mut self) -> Result<Token> {
        let mut s = String::new();
        loop {
            match self.advance() {
                None => {
                    return Err(ArcaneError::ParseError {
                        pos: self.pos,
                        msg: "Unterminated string literal".into(),
                    })
                }
                Some('"') => break,
                Some('\\') => match self.advance() {
                    Some('n') => s.push('\n'),
                    Some('t') => s.push('\t'),
                    Some('"') => s.push('"'),
                    Some('\\') => s.push('\\'),
                    Some(c) => s.push(c),
                    None => break,
                },
                Some(c) => s.push(c),
            }
        }
        Ok(Token::StringLit(s))
    }

    fn read_number(&mut self, first: char) -> Token {
        let mut s = String::from(first);
        let mut is_float = false;
        while let Some(c) = self.peek_char() {
            if c.is_ascii_digit() || c == '_' {
                s.push(c);
                self.advance();
            } else if c == '.' && !is_float {
                is_float = true;
                s.push(c);
                self.advance();
            } else {
                break;
            }
        }
        if is_float {
            Token::FloatLit(s.parse().unwrap_or(0.0))
        } else {
            Token::IntLit(s.replace('_', "").parse().unwrap_or(0))
        }
    }

    fn read_ident(&mut self, first: char) -> Token {
        let mut s = String::from(first);
        while let Some(c) = self.peek_char() {
            if c.is_alphanumeric() || c == '_' || c == '!' {
                s.push(c);
                self.advance();
            } else {
                break;
            }
        }
        match s.to_lowercase().as_str() {
            "true" => Token::Bool(true),
            "false" => Token::Bool(false),
            "null" | "__null__" => Token::Null,
            "and" => Token::And,
            "or" => Token::Or,
            "like" => Token::Like,
            _ => Token::Ident(s),
        }
    }

    fn next_token(&mut self) -> Result<Token> {
        self.skip_whitespace();
        match self.advance() {
            None => Ok(Token::Eof),
            Some('#') => {
                while let Some(c) = self.peek_char() {
                    if c == '\n' {
                        break;
                    }
                    self.advance();
                }
                self.next_token()
            }
            Some('"') => self.read_string(),
            Some('*') => Ok(Token::Star),
            Some(',') => Ok(Token::Comma),
            Some(':') => Ok(Token::Colon),
            Some(';') => Ok(Token::Semicolon),
            Some('(') => Ok(Token::LParen),
            Some(')') => Ok(Token::RParen),
            Some('[') => Ok(Token::LBracket),
            Some(']') => Ok(Token::RBracket),
            Some('{') => Ok(Token::LBrace),
            Some('}') => Ok(Token::RBrace),
            Some('=') => Ok(Token::Eq),
            Some('!') => {
                if self.peek_char() == Some('=') {
                    self.advance();
                    Ok(Token::Ne)
                } else {
                    Err(ArcaneError::ParseError {
                        pos: self.pos,
                        msg: "Unexpected character: '!'".into(),
                    })
                }
            }
            Some('<') => {
                if self.peek_char() == Some('=') {
                    self.advance();
                    Ok(Token::Le)
                } else {
                    Ok(Token::Lt)
                }
            }
            Some('>') => {
                if self.peek_char() == Some('=') {
                    self.advance();
                    Ok(Token::Ge)
                } else {
                    Ok(Token::Gt)
                }
            }
            Some(c) if c.is_ascii_digit() || c == '-' => Ok(self.read_number(c)),
            Some(c) if c.is_alphabetic() || c == '_' => Ok(self.read_ident(c)),
            Some(c) => Err(ArcaneError::ParseError {
                pos: self.pos,
                msg: format!("Unexpected character: '{}'", c),
            }),
        }
    }

    fn tokenize(mut self) -> Result<Vec<Token>> {
        let mut tokens = Vec::new();
        loop {
            let tok = self.next_token()?;
            let done = tok == Token::Eof;
            tokens.push(tok);
            if done {
                break;
            }
        }
        Ok(tokens)
    }
}

struct Parser {
    tokens: Vec<Token>,
    pos: usize,
}

impl Parser {
    fn new(tokens: Vec<Token>) -> Self {
        Parser { tokens, pos: 0 }
    }

    fn peek(&self) -> &Token {
        self.tokens.get(self.pos).unwrap_or(&Token::Eof)
    }

    fn advance(&mut self) -> &Token {
        let t = self.tokens.get(self.pos).unwrap_or(&Token::Eof);
        if self.pos < self.tokens.len() {
            self.pos += 1;
        }
        t
    }

    fn expect_ident(&mut self) -> Result<String> {
        match self.advance().clone() {
            Token::Ident(s) => Ok(s),
            other => Err(ArcaneError::ParseError {
                pos: self.pos,
                msg: format!("Expected identifier, got {:?}", other),
            }),
        }
    }

    fn expect_token(&mut self, expected: &Token) -> Result<()> {
        let tok = self.advance().clone();
        if std::mem::discriminant(&tok) == std::mem::discriminant(expected) {
            Ok(())
        } else {
            Err(ArcaneError::ParseError {
                pos: self.pos,
                msg: format!("Expected {:?}, got {:?}", expected, tok),
            })
        }
    }

    fn parse_literal(&mut self) -> Result<Value> {
        match self.advance().clone() {
            Token::StringLit(s) => Ok(Value::String(s)),
            Token::IntLit(i) => Ok(Value::Int(i)),
            Token::FloatLit(f) => Ok(Value::Float(f)),
            Token::Bool(b) => Ok(Value::Bool(b)),
            Token::Null => Ok(Value::Null),
            other => Err(ArcaneError::ParseError {
                pos: self.pos,
                msg: format!("Expected literal value, got {:?}", other),
            }),
        }
    }

    fn parse_field_type(&mut self) -> Result<FieldType> {
        let name = self.expect_ident()?;
        match name.to_lowercase().as_str() {
            "string" => Ok(FieldType::String),
            "int" | "integer" => Ok(FieldType::Int),
            "float" | "double" => Ok(FieldType::Float),
            "bool" | "boolean" => Ok(FieldType::Bool),
            "bytes" | "blob" => Ok(FieldType::Bytes),
            other => Err(ArcaneError::ParseError {
                pos: self.pos,
                msg: format!("Unknown type: '{}'", other),
            }),
        }
    }

    fn parse_create_bucket(&mut self, unique: bool, forced: bool) -> Result<Statement> {
        let name = self.expect_ident()?;
        self.expect_token(&Token::LParen)?;
        let mut fields = Vec::new();
        loop {
            if self.peek() == &Token::RParen {
                break;
            }
            let field_name = self.expect_ident()?;
            self.expect_token(&Token::Colon)?;
            let ty = self.parse_field_type()?;
            fields.push(FieldDef {
                name: field_name,
                ty,
            });
            if self.peek() == &Token::Comma {
                self.advance();
            } else {
                break;
            }
        }
        self.expect_token(&Token::RParen)?;
        Ok(Statement::CreateBucket {
            name,
            fields,
            unique,
            forced,
        })
    }

    fn parse_insert(&mut self) -> Result<Statement> {
        let kw = self.expect_ident()?;
        if kw.to_lowercase() != "into" {
            return Err(ArcaneError::ParseError {
                pos: self.pos,
                msg: format!("Expected 'into', got '{}'", kw),
            });
        }

        let bucket = self.expect_ident()?;
        self.expect_token(&Token::LParen)?;

        if self.peek() == &Token::LBracket {
            return self.parse_batch_insert_rows(bucket);
        }

        let mut values: Vec<(Option<String>, Value)> = Vec::new();

        loop {
            if self.peek() == &Token::RParen {
                break;
            }
            let named = matches!(
                (self.tokens.get(self.pos), self.tokens.get(self.pos + 1)),
                (Some(Token::Ident(_)), Some(Token::Colon))
            );

            if named {
                let field_name = self.expect_ident()?;
                self.expect_token(&Token::Colon)?;
                let val = self.parse_literal()?;
                values.push((Some(field_name), val));
            } else {
                let val = self.parse_literal()?;
                values.push((None, val));
            }

            if self.peek() == &Token::Comma {
                self.advance();
            } else {
                break;
            }
        }
        self.expect_token(&Token::RParen)?;
        Ok(Statement::Insert { bucket, values })
    }

    fn parse_batch_insert_rows(&mut self, bucket: String) -> Result<Statement> {
        let mut rows = Vec::new();

        loop {
            if self.peek() == &Token::RParen {
                break;
            }

            self.expect_token(&Token::LBracket)?;
            let mut values: Vec<(Option<String>, Value)> = Vec::new();

            loop {
                if self.peek() == &Token::RBracket {
                    break;
                }

                let named = matches!(
                    (self.tokens.get(self.pos), self.tokens.get(self.pos + 1)),
                    (Some(Token::Ident(_)), Some(Token::Colon))
                );

                if named {
                    let field_name = self.expect_ident()?;
                    self.expect_token(&Token::Colon)?;
                    let val = self.parse_literal()?;
                    values.push((Some(field_name), val));
                } else {
                    let val = self.parse_literal()?;
                    values.push((None, val));
                }

                if self.peek() == &Token::Comma {
                    self.advance();
                } else {
                    break;
                }
            }

            self.expect_token(&Token::RBracket)?;
            rows.push(values);

            if self.peek() == &Token::Comma {
                self.advance();
            } else {
                break;
            }
        }

        self.expect_token(&Token::RParen)?;
        Ok(Statement::BatchInsert { bucket, rows })
    }

    fn parse_bulk(&mut self) -> Result<Statement> {
        self.expect_token(&Token::LBrace)?;
        let mut statements = Vec::new();

        loop {
            if self.peek() == &Token::RBrace {
                break;
            }
            if self.peek() == &Token::Eof {
                return Err(ArcaneError::ParseError {
                    pos: self.pos,
                    msg: "Unexpected EOF in bulk block".into(),
                });
            }
            statements.push(self.parse_statement()?);
        }

        self.expect_token(&Token::RBrace)?;
        Ok(Statement::Bulk { statements })
    }

    fn parse_get(&mut self) -> Result<Statement> {
        let projection = match self.peek().clone() {
            Token::Star => {
                self.advance();
                Projection::Star
            }
            Token::Ident(ref s) if s == "__hash__" => {
                self.advance();
                Projection::Hash
            }
            Token::Ident(ref s) if s.to_lowercase() == "head" => {
                self.advance();
                self.expect_token(&Token::LParen)?;
                let n = match self.advance().clone() {
                    Token::IntLit(i) => i as usize,
                    other => {
                        return Err(ArcaneError::ParseError {
                            pos: self.pos,
                            msg: format!("Expected int in head(), got {:?}", other),
                        })
                    }
                };
                self.expect_token(&Token::RParen)?;
                Projection::Head(n)
            }
            Token::Ident(ref s) if s.to_lowercase() == "tail" => {
                self.advance();
                self.expect_token(&Token::LParen)?;
                let n = match self.advance().clone() {
                    Token::IntLit(i) => i as usize,
                    other => {
                        return Err(ArcaneError::ParseError {
                            pos: self.pos,
                            msg: format!("Expected int in tail(), got {:?}", other),
                        })
                    }
                };
                self.expect_token(&Token::RParen)?;
                Projection::Tail(n)
            }
            Token::Ident(_) => {
                let mut aggregates = Vec::new();
                let mut fields = Vec::new();
                let mut is_aggregate = false;

                loop {
                    let name = self.expect_ident()?;

                    if self.peek() == &Token::LParen {
                        is_aggregate = true;
                        self.advance();

                        let field = if name.to_lowercase() == "count" && self.peek() == &Token::Star
                        {
                            self.advance();
                            None
                        } else {
                            Some(self.expect_ident()?)
                        };

                        self.expect_token(&Token::RParen)?;

                        let agg = match name.to_lowercase().as_str() {
                            "avg" => AggregateFunc::Avg(field.ok_or_else(|| {
                                ArcaneError::ParseError {
                                    pos: self.pos,
                                    msg: "avg() requires a field name".into(),
                                }
                            })?),
                            "sum" => AggregateFunc::Sum(field.ok_or_else(|| {
                                ArcaneError::ParseError {
                                    pos: self.pos,
                                    msg: "sum() requires a field name".into(),
                                }
                            })?),
                            "min" => AggregateFunc::Min(field.ok_or_else(|| {
                                ArcaneError::ParseError {
                                    pos: self.pos,
                                    msg: "min() requires a field name".into(),
                                }
                            })?),
                            "max" => AggregateFunc::Max(field.ok_or_else(|| {
                                ArcaneError::ParseError {
                                    pos: self.pos,
                                    msg: "max() requires a field name".into(),
                                }
                            })?),
                            "median" => AggregateFunc::Median(field.ok_or_else(|| {
                                ArcaneError::ParseError {
                                    pos: self.pos,
                                    msg: "median() requires a field name".into(),
                                }
                            })?),
                            "stddev" => AggregateFunc::Stddev(field.ok_or_else(|| {
                                ArcaneError::ParseError {
                                    pos: self.pos,
                                    msg: "stddev() requires a field name".into(),
                                }
                            })?),
                            "count" => AggregateFunc::Count(field),
                            _ => {
                                return Err(ArcaneError::ParseError {
                                    pos: self.pos,
                                    msg: format!("Unknown aggregate function: '{}'", name),
                                })
                            }
                        };
                        aggregates.push(agg);
                    } else {
                        fields.push(name);
                    }

                    if self.peek() == &Token::Comma {
                        self.advance();
                    } else {
                        break;
                    }
                }

                if is_aggregate {
                    if !fields.is_empty() {
                        return Err(ArcaneError::ParseError {
                            pos: self.pos,
                            msg: "Cannot mix aggregate functions with regular fields".into(),
                        });
                    }
                    Projection::Aggregates(aggregates)
                } else {
                    Projection::Fields(fields)
                }
            }
            other => {
                return Err(ArcaneError::ParseError {
                    pos: self.pos,
                    msg: format!("Unexpected projection: {:?}", other),
                })
            }
        };

        let from_kw = self.expect_ident()?;
        if from_kw.to_lowercase() != "from" {
            return Err(ArcaneError::ParseError {
                pos: self.pos,
                msg: format!("Expected 'from', got '{}'", from_kw),
            });
        }

        let bucket = self.expect_ident()?;

        let mut filter = None;
        let mut order_by = None;

        if let Token::Ident(ref kw) = self.peek().clone() {
            if kw.to_lowercase() == "where" {
                self.advance();
                filter = Some(self.parse_filter()?);
            }
        }

        if let Token::Ident(ref kw) = self.peek().clone() {
            if kw.to_lowercase() == "order" {
                self.advance();
                let by_kw = self.expect_ident()?;
                if by_kw.to_lowercase() != "by" {
                    return Err(ArcaneError::ParseError {
                        pos: self.pos,
                        msg: format!("Expected 'by' after 'order', got '{}'", by_kw),
                    });
                }
                let field = self.expect_ident()?;

                let order = if let Token::Ident(ref ord) = self.peek().clone() {
                    match ord.to_lowercase().as_str() {
                        "asc" => {
                            self.advance();
                            SortOrder::Asc
                        }
                        "desc" => {
                            self.advance();
                            SortOrder::Desc
                        }
                        _ => SortOrder::Asc,
                    }
                } else {
                    SortOrder::Asc
                };

                order_by = Some(OrderBy { field, order });
            }
        }

        Ok(Statement::Get {
            bucket,
            projection,
            filter,
            order_by,
        })
    }

    fn parse_delete(&mut self) -> Result<Statement> {
        let from_kw = self.expect_ident()?;
        if from_kw.to_lowercase() != "from" {
            return Err(ArcaneError::ParseError {
                pos: self.pos,
                msg: format!("Expected 'from', got '{}'", from_kw),
            });
        }
        let bucket = self.expect_ident()?;

        let where_kw = self.expect_ident()?;
        if where_kw.to_lowercase() != "where" {
            return Err(ArcaneError::ParseError {
                pos: self.pos,
                msg: format!("Expected 'where', got '{}'", where_kw),
            });
        }

        let filter = self.parse_filter()?;

        Ok(Statement::Delete { bucket, filter })
    }

    fn parse_set(&mut self) -> Result<Statement> {
        let bucket = if self.peek() == &Token::LParen {
            return Err(ArcaneError::ParseError {
                pos: self.pos,
                msg: "set command requires bucket name: set <bucket> ( ... ) where ...".into(),
            });
        } else {
            self.expect_ident()?
        };

        self.expect_token(&Token::LParen)?;

        let mut values: Vec<(Option<String>, Value)> = Vec::new();
        loop {
            if self.peek() == &Token::RParen {
                break;
            }

            let named = matches!(
                (self.tokens.get(self.pos), self.tokens.get(self.pos + 1)),
                (Some(Token::Ident(_)), Some(Token::Colon))
            );

            if named {
                let field_name = self.expect_ident()?;
                self.expect_token(&Token::Colon)?;
                let val = self.parse_literal()?;
                values.push((Some(field_name), val));
            } else {
                let val = self.parse_literal()?;
                values.push((None, val));
            }

            if self.peek() == &Token::Comma {
                self.advance();
            } else {
                break;
            }
        }
        self.expect_token(&Token::RParen)?;

        let where_kw = self.expect_ident()?;
        if where_kw.to_lowercase() != "where" {
            return Err(ArcaneError::ParseError {
                pos: self.pos,
                msg: format!("Expected 'where', got '{}'", where_kw),
            });
        }

        let filter = self.parse_filter()?;

        Ok(Statement::Set {
            bucket,
            values,
            filter,
        })
    }

    fn parse_compare_op(&mut self) -> Result<CompareOp> {
        let tok = self.peek().clone();
        match tok {
            Token::Eq => {
                self.advance();
                Ok(CompareOp::Eq)
            }
            Token::Ne => {
                self.advance();
                Ok(CompareOp::Ne)
            }
            Token::Lt => {
                self.advance();
                Ok(CompareOp::Lt)
            }
            Token::Le => {
                self.advance();
                Ok(CompareOp::Le)
            }
            Token::Gt => {
                self.advance();
                Ok(CompareOp::Gt)
            }
            Token::Ge => {
                self.advance();
                Ok(CompareOp::Ge)
            }
            Token::Like => {
                self.advance();
                Ok(CompareOp::Like)
            }
            other => Err(ArcaneError::ParseError {
                pos: self.pos,
                msg: format!("Expected comparison operator, got {:?}", other),
            }),
        }
    }

    fn parse_filter(&mut self) -> Result<Filter> {
        self.parse_or_filter()
    }

    fn parse_or_filter(&mut self) -> Result<Filter> {
        let mut left = self.parse_and_filter()?;

        while self.peek() == &Token::Or {
            self.advance();
            let right = self.parse_and_filter()?;
            left = Filter::Or(Box::new(left), Box::new(right));
        }

        Ok(left)
    }

    fn parse_and_filter(&mut self) -> Result<Filter> {
        let mut left = self.parse_primary_filter()?;

        while self.peek() == &Token::And {
            self.advance();
            let right = self.parse_primary_filter()?;
            left = Filter::And(Box::new(left), Box::new(right));
        }

        Ok(left)
    }

    fn parse_primary_filter(&mut self) -> Result<Filter> {
        if self.peek() == &Token::LParen {
            self.advance();
            let filter = self.parse_filter()?;
            self.expect_token(&Token::RParen)?;
            Ok(filter)
        } else {
            let field = self.expect_ident()?;
            let op = self.parse_compare_op()?;
            let value = self.parse_literal()?;
            Ok(Filter::Simple { field, op, value })
        }
    }

    fn parse_statement(&mut self) -> Result<Statement> {
        let kw = self.expect_ident()?;
        match kw.to_lowercase().as_str() {
            "create" => {
                let mut unique = false;
                let mut forced = false;

                loop {
                    if let Token::Ident(ref s) = self.peek().clone() {
                        match s.to_lowercase().as_str() {
                            "unique" => {
                                unique = true;
                                self.advance();
                            }
                            "forced" => {
                                forced = true;
                                self.advance();
                            }
                            "bucket" => break,
                            _ => {
                                return Err(ArcaneError::ParseError {
                                    pos: self.pos,
                                    msg: format!(
                                        "Expected 'bucket', 'unique', or 'forced', got '{}'",
                                        s
                                    ),
                                })
                            }
                        }
                    } else {
                        break;
                    }
                }

                let next = self.expect_ident()?;
                if next.to_lowercase() != "bucket" {
                    return Err(ArcaneError::ParseError {
                        pos: self.pos,
                        msg: format!("Expected 'bucket', got '{}'", next),
                    });
                }
                self.parse_create_bucket(unique, forced)
            }
            "insert" => self.parse_insert(),
            "bulk" => self.parse_bulk(),
            "get" => self.parse_get(),
            "delete" => self.parse_delete(),
            "set" => self.parse_set(),
            "truncate" => {
                let bucket = self.expect_ident()?;
                Ok(Statement::Truncate { bucket })
            }
            "describe" => {
                let bucket = self.expect_ident()?;
                Ok(Statement::Describe { bucket })
            }
            "commit!" => Ok(Statement::Commit),
            other => Err(ArcaneError::ParseError {
                pos: self.pos,
                msg: format!("Unknown statement keyword: '{}'", other),
            }),
        }
    }
}

/// Parse a single-line AQL statement (comment-stripped).
pub fn parse_statement(line: &str) -> Result<Statement> {
    let line = if let Some(i) = line.find('#') {
        &line[..i]
    } else {
        line
    };

    let line = line.trim();
    let tokens = Lexer::new(line).tokenize()?;
    let mut parser = Parser::new(tokens);
    let stmt = parser.parse_statement()?;

    if parser.peek() == &Token::Semicolon {
        parser.advance();
    }

    Ok(stmt)
}

pub fn parse_script(src: &str) -> Vec<Result<Statement>> {
    let mut statements = Vec::new();
    let mut current = String::new();

    for line in src.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        current.push(' ');
        current.push_str(line);

        if line.ends_with(';') {
            statements.push(parse_statement(&current));
            current.clear();
        }
    }

    if !current.trim().is_empty() {
        statements.push(parse_statement(&current));
    }

    statements
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_create_bucket() {
        let stmt = parse_statement("create bucket Users (name: string, age: int)").unwrap();
        match stmt {
            Statement::CreateBucket {
                name,
                fields,
                unique,
                forced,
            } => {
                assert_eq!(name, "Users");
                assert_eq!(fields.len(), 2);
                assert_eq!(fields[0].name, "name");
                assert_eq!(fields[1].name, "age");
                assert!(!unique);
                assert!(!forced);
            }
            _ => panic!("Expected CreateBucket"),
        }
    }

    #[test]
    fn test_parse_create_unique_bucket() {
        let stmt = parse_statement("create unique bucket Users (name: string)").unwrap();
        match stmt {
            Statement::CreateBucket { unique, forced, .. } => {
                assert!(unique);
                assert!(!forced);
            }
            _ => panic!("Expected CreateBucket"),
        }
    }

    #[test]
    fn test_parse_create_forced_unique_bucket() {
        let stmt = parse_statement("create forced unique bucket Users (name: string)").unwrap();
        match stmt {
            Statement::CreateBucket { unique, forced, .. } => {
                assert!(unique);
                assert!(forced);
            }
            _ => panic!("Expected CreateBucket"),
        }
    }

    #[test]
    fn test_parse_insert_positional() {
        let stmt = parse_statement("insert into Users (\"Alice\", 30)").unwrap();
        match stmt {
            Statement::Insert { bucket, values } => {
                assert_eq!(bucket, "Users");
                assert_eq!(values.len(), 2);
                assert!(values[0].0.is_none());
                assert!(values[1].0.is_none());
            }
            _ => panic!("Expected Insert"),
        }
    }

    #[test]
    fn test_parse_insert_named() {
        let stmt = parse_statement("insert into Users (name: \"Alice\", age: 30)").unwrap();
        match stmt {
            Statement::Insert { bucket, values } => {
                assert_eq!(bucket, "Users");
                assert_eq!(values.len(), 2);
                assert_eq!(values[0].0.as_ref().unwrap(), "name");
                assert_eq!(values[1].0.as_ref().unwrap(), "age");
            }
            _ => panic!("Expected Insert"),
        }
    }

    #[test]
    fn test_parse_batch_insert() {
        let stmt = parse_statement(
            "insert into Users ([name: \"Alice\", age: 30], [name: \"Bob\", age: 25])",
        )
        .unwrap();
        match stmt {
            Statement::BatchInsert { bucket, rows } => {
                assert_eq!(bucket, "Users");
                assert_eq!(rows.len(), 2);
                assert_eq!(rows[0].len(), 2);
                assert_eq!(rows[1].len(), 2);
            }
            _ => panic!("Expected BatchInsert"),
        }
    }

    #[test]
    fn test_parse_bulk() {
        let stmt = parse_statement(
            "bulk { insert into Users (\"Alice\", 30) insert into Users (\"Bob\", 25) }",
        )
        .unwrap();
        match stmt {
            Statement::Bulk { statements } => {
                assert_eq!(statements.len(), 2);
            }
            _ => panic!("Expected Bulk"),
        }
    }

    #[test]
    fn test_parse_get_star() {
        let stmt = parse_statement("get * from Users").unwrap();
        match stmt {
            Statement::Get {
                bucket,
                projection,
                filter,
                order_by,
            } => {
                assert_eq!(bucket, "Users");
                assert!(matches!(projection, Projection::Star));
                assert!(filter.is_none());
                assert!(order_by.is_none());
            }
            _ => panic!("Expected Get"),
        }
    }

    #[test]
    fn test_parse_get_with_filter() {
        let stmt = parse_statement("get * from Users where name = \"Alice\"").unwrap();
        match stmt {
            Statement::Get {
                bucket,
                projection,
                filter,
                order_by,
            } => {
                assert_eq!(bucket, "Users");
                assert!(matches!(projection, Projection::Star));
                assert!(filter.is_some());
                assert!(order_by.is_none());
                let f = filter.unwrap();
                match f {
                    Filter::Simple { field, op, value } => {
                        assert_eq!(field, "name");
                        assert_eq!(op, CompareOp::Eq);
                        assert_eq!(value, Value::String("Alice".to_string()));
                    }
                    _ => panic!("Expected Simple filter"),
                }
            }
            _ => panic!("Expected Get"),
        }
    }

    #[test]
    fn test_parse_get_head() {
        match parse_statement("get head(10) from Users").unwrap() {
            Statement::Get { projection, .. } => {
                assert!(matches!(projection, Projection::Head(10)));
            }
            _ => panic!("Expected Get"),
        }
    }

    #[test]
    fn test_parse_get_tail() {
        match parse_statement("get tail(5) from Users").unwrap() {
            Statement::Get { projection, .. } => {
                assert!(matches!(projection, Projection::Tail(5)));
            }
            _ => panic!("Expected Get"),
        }
    }

    #[test]
    fn test_parse_get_hash() {
        match parse_statement("get __hash__ from Users").unwrap() {
            Statement::Get { projection, .. } => {
                assert!(matches!(projection, Projection::Hash));
            }
            _ => panic!("Expected Get"),
        }
    }

    #[test]
    fn test_parse_with_semicolon() {
        match parse_statement("create bucket Test (id: int);").unwrap() {
            Statement::CreateBucket { name, .. } => {
                assert_eq!(name, "Test");
            }
            _ => panic!("Expected CreateBucket"),
        }
    }

    #[test]
    fn test_parse_script_multiline() {
        let script = r#"
            create bucket Users (name: string);
            insert into Users ("Alice");
            get * from Users;
        "#;
        let stmts = parse_script(script);
        assert_eq!(stmts.len(), 3);
        assert!(stmts.iter().all(|r| r.is_ok()));
    }

    #[test]
    fn test_parse_script_with_comments() {
        let script = r#"
            # This is a comment
            create bucket Users (name: string);
            # Another comment
            insert into Users ("Alice");
        "#;
        let stmts = parse_script(script);
        assert_eq!(stmts.len(), 2);
    }

    #[test]
    fn test_lexer_string_escapes() {
        let tokens = Lexer::new(r#""hello\nworld""#).tokenize().unwrap();
        match &tokens[0] {
            Token::StringLit(s) => assert_eq!(s, "hello\nworld"),
            _ => panic!("Expected string literal"),
        }
    }

    #[test]
    fn test_lexer_numbers() {
        let tokens = Lexer::new("42 3.14 -10").tokenize().unwrap();
        assert!(matches!(tokens[0], Token::IntLit(42)));
        assert!(matches!(tokens[1], Token::FloatLit(_)));
        assert!(matches!(tokens[2], Token::IntLit(-10)));
    }

    #[test]
    fn test_lexer_keywords() {
        let tokens = Lexer::new("true false null").tokenize().unwrap();
        assert!(matches!(tokens[0], Token::Bool(true)));
        assert!(matches!(tokens[1], Token::Bool(false)));
        assert!(matches!(tokens[2], Token::Null));
    }

    #[test]
    fn test_parse_set_named() {
        let stmt =
            parse_statement("set Users (name: \"Bob\", age: 35) where name = \"Alice\"").unwrap();
        match stmt {
            Statement::Set {
                bucket,
                values,
                filter,
            } => {
                assert_eq!(bucket, "Users");
                assert_eq!(values.len(), 2);
                assert_eq!(values[0].0.as_ref().unwrap(), "name");
                assert_eq!(values[1].0.as_ref().unwrap(), "age");
                match filter {
                    Filter::Simple { field, .. } => {
                        assert_eq!(field, "name");
                    }
                    _ => panic!("Expected Simple filter"),
                }
            }
            _ => panic!("Expected Set"),
        }
    }

    #[test]
    fn test_parse_set_positional() {
        let stmt = parse_statement("set Users (\"Bob\", 35) where id = 1").unwrap();
        match stmt {
            Statement::Set {
                bucket,
                values,
                filter,
            } => {
                assert_eq!(bucket, "Users");
                assert_eq!(values.len(), 2);
                assert!(values[0].0.is_none());
                assert!(values[1].0.is_none());
                match filter {
                    Filter::Simple { field, .. } => {
                        assert_eq!(field, "id");
                    }
                    _ => panic!("Expected Simple filter"),
                }
            }
            _ => panic!("Expected Set"),
        }
    }

    #[test]
    fn test_parse_commit() {
        let stmt = parse_statement("commit!").unwrap();
        assert!(matches!(stmt, Statement::Commit));
    }

    #[test]
    fn test_lexer_exclamation_in_ident() {
        let tokens = Lexer::new("commit!").tokenize().unwrap();
        match &tokens[0] {
            Token::Ident(s) => assert_eq!(s, "commit!"),
            _ => panic!("Expected identifier with !"),
        }
    }

    #[test]
    fn test_parse_get_single_field() {
        let stmt = parse_statement("get name from Users").unwrap();
        match stmt {
            Statement::Get {
                bucket,
                projection,
                filter,
                order_by,
            } => {
                assert_eq!(bucket, "Users");
                assert!(
                    matches!(projection, Projection::Fields(ref fields) if fields.len() == 1 && fields[0] == "name")
                );
                assert!(filter.is_none());
                assert!(order_by.is_none());
            }
            _ => panic!("Expected Get"),
        }
    }

    #[test]
    fn test_parse_get_multiple_fields() {
        let stmt = parse_statement("get name, age, city from Users").unwrap();
        match stmt {
            Statement::Get {
                bucket,
                projection,
                filter,
                order_by,
            } => {
                assert_eq!(bucket, "Users");
                match projection {
                    Projection::Fields(fields) => {
                        assert_eq!(fields.len(), 3);
                        assert_eq!(fields[0], "name");
                        assert_eq!(fields[1], "age");
                        assert_eq!(fields[2], "city");
                    }
                    _ => panic!("Expected Fields projection"),
                }
                assert!(filter.is_none());
                assert!(order_by.is_none());
            }
            _ => panic!("Expected Get"),
        }
    }

    #[test]
    fn test_parse_get_field_with_filter() {
        let stmt = parse_statement("get name from Users where age > 20").unwrap();
        match stmt {
            Statement::Get {
                bucket,
                projection,
                filter,
                order_by,
            } => {
                assert_eq!(bucket, "Users");
                assert!(matches!(projection, Projection::Fields(ref fields) if fields.len() == 1));
                assert!(filter.is_some());
                assert!(order_by.is_none());
            }
            _ => panic!("Expected Get"),
        }
    }

    #[test]
    fn test_parse_get_with_order_by_asc() {
        let stmt = parse_statement("get * from Users order by age asc").unwrap();
        match stmt {
            Statement::Get {
                bucket,
                projection,
                filter,
                order_by,
            } => {
                assert_eq!(bucket, "Users");
                assert!(matches!(projection, Projection::Star));
                assert!(filter.is_none());
                assert!(order_by.is_some());
                let order = order_by.unwrap();
                assert_eq!(order.field, "age");
                assert_eq!(order.order, SortOrder::Asc);
            }
            _ => panic!("Expected Get"),
        }
    }

    #[test]
    fn test_parse_get_with_order_by_desc() {
        let stmt = parse_statement("get * from Users order by name desc").unwrap();
        match stmt {
            Statement::Get {
                bucket,
                projection,
                filter,
                order_by,
            } => {
                assert_eq!(bucket, "Users");
                assert!(matches!(projection, Projection::Star));
                assert!(filter.is_none());
                assert!(order_by.is_some());
                let order = order_by.unwrap();
                assert_eq!(order.field, "name");
                assert_eq!(order.order, SortOrder::Desc);
            }
            _ => panic!("Expected Get"),
        }
    }

    #[test]
    fn test_parse_get_with_filter_and_order_by() {
        let stmt = parse_statement("get * from Users where age > 20 order by name asc").unwrap();
        match stmt {
            Statement::Get {
                bucket,
                projection,
                filter,
                order_by,
            } => {
                assert_eq!(bucket, "Users");
                assert!(matches!(projection, Projection::Star));
                assert!(filter.is_some());
                assert!(order_by.is_some());
                let order = order_by.unwrap();
                assert_eq!(order.field, "name");
                assert_eq!(order.order, SortOrder::Asc);
            }
            _ => panic!("Expected Get"),
        }
    }

    #[test]
    fn test_parse_get_order_by_default_asc() {
        let stmt = parse_statement("get * from Users order by age").unwrap();
        match stmt {
            Statement::Get { order_by, .. } => {
                assert!(order_by.is_some());
                let order = order_by.unwrap();
                assert_eq!(order.order, SortOrder::Asc);
            }
            _ => panic!("Expected Get"),
        }
    }

    #[test]
    fn test_parse_complex_filter_and() {
        let stmt =
            parse_statement("get * from Products where price > 3 and in_stock = true").unwrap();
        match stmt {
            Statement::Get { filter, .. } => {
                assert!(filter.is_some());
                match filter.unwrap() {
                    Filter::And(left, right) => {
                        match *left {
                            Filter::Simple { field, op, .. } => {
                                assert_eq!(field, "price");
                                assert_eq!(op, CompareOp::Gt);
                            }
                            _ => panic!("Expected Simple filter on left"),
                        }
                        match *right {
                            Filter::Simple { field, op, .. } => {
                                assert_eq!(field, "in_stock");
                                assert_eq!(op, CompareOp::Eq);
                            }
                            _ => panic!("Expected Simple filter on right"),
                        }
                    }
                    _ => panic!("Expected And filter"),
                }
            }
            _ => panic!("Expected Get"),
        }
    }

    #[test]
    fn test_parse_complex_filter_or() {
        let stmt = parse_statement("get * from Products where price = 2.5 or price > 10").unwrap();
        match stmt {
            Statement::Get { filter, .. } => {
                assert!(filter.is_some());
                match filter.unwrap() {
                    Filter::Or(left, right) => {
                        match *left {
                            Filter::Simple { field, op, .. } => {
                                assert_eq!(field, "price");
                                assert_eq!(op, CompareOp::Eq);
                            }
                            _ => panic!("Expected Simple filter on left"),
                        }
                        match *right {
                            Filter::Simple { field, op, .. } => {
                                assert_eq!(field, "price");
                                assert_eq!(op, CompareOp::Gt);
                            }
                            _ => panic!("Expected Simple filter on right"),
                        }
                    }
                    _ => panic!("Expected Or filter"),
                }
            }
            _ => panic!("Expected Get"),
        }
    }

    #[test]
    fn test_parse_complex_filter_with_parens() {
        let stmt = parse_statement(
            "get * from Products where (price > 3 and in_stock = true) or price = 2.5",
        )
        .unwrap();
        match stmt {
            Statement::Get { filter, .. } => {
                assert!(filter.is_some());
                match filter.unwrap() {
                    Filter::Or(left, right) => {
                        match *left {
                            Filter::And(_, _) => {}
                            _ => panic!("Expected And filter on left"),
                        }
                        match *right {
                            Filter::Simple { field, op, .. } => {
                                assert_eq!(field, "price");
                                assert_eq!(op, CompareOp::Eq);
                            }
                            _ => panic!("Expected Simple filter on right"),
                        }
                    }
                    _ => panic!("Expected Or filter"),
                }
            }
            _ => panic!("Expected Get"),
        }
    }

    #[test]
    fn test_parse_like_operator() {
        let stmt = parse_statement("get * from Products where name like \"D%\"").unwrap();
        match stmt {
            Statement::Get { filter, .. } => {
                assert!(filter.is_some());
                match filter.unwrap() {
                    Filter::Simple { field, op, value } => {
                        assert_eq!(field, "name");
                        assert_eq!(op, CompareOp::Like);
                        assert_eq!(value, Value::String("D%".to_string()));
                    }
                    _ => panic!("Expected Simple filter"),
                }
            }
            _ => panic!("Expected Get"),
        }
    }

    #[test]
    fn test_parse_like_with_complex_filter() {
        let stmt = parse_statement(
            "get name from Products where name like \"D%\" and price > 4 and in_stock = true",
        )
        .unwrap();
        match stmt {
            Statement::Get { filter, .. } => {
                assert!(filter.is_some());

                match filter.unwrap() {
                    Filter::And(left, right) => {
                        match *left {
                            Filter::And(inner_left, inner_right) => {
                                match *inner_left {
                                    Filter::Simple { op, .. } => {
                                        assert_eq!(op, CompareOp::Like);
                                    }
                                    _ => panic!("Expected Like filter"),
                                }
                                match *inner_right {
                                    Filter::Simple { op, .. } => {
                                        assert_eq!(op, CompareOp::Gt);
                                    }
                                    _ => panic!("Expected Gt filter"),
                                }
                            }
                            _ => panic!("Expected And filter on left"),
                        }
                        match *right {
                            Filter::Simple { op, .. } => {
                                assert_eq!(op, CompareOp::Eq);
                            }
                            _ => panic!("Expected Eq filter on right"),
                        }
                    }
                    _ => panic!("Expected And filter"),
                }
            }
            _ => panic!("Expected Get"),
        }
    }

    #[test]
    fn test_parse_describe() {
        let stmt = parse_statement("describe Products").unwrap();
        match stmt {
            Statement::Describe { bucket } => {
                assert_eq!(bucket, "Products");
            }
            _ => panic!("Expected Describe"),
        }
    }

    #[test]
    fn test_parse_describe_with_semicolon() {
        let stmt = parse_statement("describe Users;").unwrap();
        match stmt {
            Statement::Describe { bucket } => {
                assert_eq!(bucket, "Users");
            }
            _ => panic!("Expected Describe"),
        }
    }
}
