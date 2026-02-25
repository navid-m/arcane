//! AQL — Arcane Query Language Parser.
//!
//! statement     ::= create_bucket | insert | get
//! create_bucket ::= "create" "bucket" IDENT "(" field_def ("," field_def)* ")"
//! field_def     ::= IDENT ":" type
//! type          ::= "string" | "int" | "float" | "bool" | "bytes"
//! insert        ::= "insert" "into" IDENT "(" value_list ")"
//! value_list    ::= named_value_list | positional_value_list
//! named_value   ::= IDENT ":" literal
//! positional    ::= literal
//! get           ::= "get" projection "from" IDENT filter?
//! projection    ::= "*" | "__hash__" | "head" "(" INT ")" | "tail" "(" INT ")"
//! filter        ::= "where" IDENT "=" literal
//! literal       ::= STRING | INT | FLOAT | "true" | "false" | "null"
//! STRING        ::= '"' ... '"'

use crate::error::{ArcaneError, Result};
use crate::storage::{FieldDef, FieldType, Value};

#[derive(Debug, Clone)]
pub enum Statement {
    CreateBucket {
        name: String,
        fields: Vec<FieldDef>,
    },
    Insert {
        bucket: String,
        values: Vec<(Option<String>, Value)>,
    },
    Get {
        bucket: String,
        projection: Projection,
        filter: Option<Filter>,
    },
}

#[derive(Debug, Clone)]
pub enum Projection {
    Star,
    Hash,
    Head(usize),
    Tail(usize),
}

#[derive(Debug, Clone)]
pub struct Filter {
    pub field: String,
    pub value: Value,
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
    LParen,
    RParen,
    Eq,
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
            if c.is_alphanumeric() || c == '_' {
                s.push(c);
                self.advance();
            } else {
                break;
            }
        }
        match s.to_lowercase().as_str() {
            "true" => Token::Bool(true),
            "false" => Token::Bool(false),
            "null" => Token::Null,
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
            Some('(') => Ok(Token::LParen),
            Some(')') => Ok(Token::RParen),
            Some('=') => Ok(Token::Eq),
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

    fn parse_create_bucket(&mut self) -> Result<Statement> {
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
        Ok(Statement::CreateBucket { name, fields })
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
        let filter = if let Token::Ident(ref kw) = self.peek().clone() {
            if kw.to_lowercase() == "where" {
                self.advance();
                let field = self.expect_ident()?;
                self.expect_token(&Token::Eq)?;
                let value = self.parse_literal()?;
                Some(Filter { field, value })
            } else {
                None
            }
        } else {
            None
        };

        Ok(Statement::Get {
            bucket,
            projection,
            filter,
        })
    }

    fn parse_statement(&mut self) -> Result<Statement> {
        let kw = self.expect_ident()?;
        match kw.to_lowercase().as_str() {
            "create" => {
                let next = self.expect_ident()?;
                if next.to_lowercase() != "bucket" {
                    return Err(ArcaneError::ParseError {
                        pos: self.pos,
                        msg: format!("Expected 'bucket' after 'create', got '{}'", next),
                    });
                }
                self.parse_create_bucket()
            }
            "insert" => self.parse_insert(),
            "get" => self.parse_get(),
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
    Ok(stmt)
}

pub fn parse_script(src: &str) -> Vec<Result<Statement>> {
    src.lines()
        .map(|l| l.trim())
        .filter(|l| !l.is_empty() && !l.starts_with('#'))
        .map(parse_statement)
        .collect()
}
