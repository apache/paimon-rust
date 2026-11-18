// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Paimon's vector and geospatial SQL types.

use std::sync::Arc;

use datafusion::arrow::datatypes::FieldRef;
use datafusion::common::{plan_datafusion_err, Result};
use datafusion::logical_expr::planner::TypePlanner;
use datafusion::sql::sqlparser::ast::{
    visit_expressions_mut, ArrayElemTypeDef, DataType, Expr, Statement,
};
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::keywords::Keyword;
use datafusion::sql::sqlparser::parser::{Parser, ParserError};
use datafusion::sql::sqlparser::tokenizer::{Token, TokenWithSpan};
use paimon::spec::{DataType as PaimonDataType, VectorType};

use crate::error::to_datafusion_error;

#[derive(Debug)]
pub(crate) struct PaimonTypePlanner;

impl TypePlanner for PaimonTypePlanner {
    fn plan_type_field(&self, sql_type: &DataType) -> Result<Option<FieldRef>> {
        let paimon_type = if let Some(vector) = sql_type_to_vector(sql_type, true)? {
            Some(PaimonDataType::Vector(vector))
        } else if contains_sql_type(sql_type, is_geo_type)
            || (matches!(sql_type, DataType::Map(_, _)) && contains_vector(sql_type))
        {
            if contains_sql_type(sql_type, |ty| {
                matches!(
                    ty,
                    DataType::Array(ArrayElemTypeDef::SquareBracket(_, Some(_)))
                )
            }) {
                return Err(datafusion::error::DataFusionError::NotImplemented(
                    "Fixed-size SQL arrays inside vector MAP types or geospatial types are not supported; use ARRAY<T> for variable-length collections".to_string(),
                ));
            }
            // DataFusion 55.2 cannot plan MAP itself, including a MAP reached
            // recursively from ARRAY/STRUCT. Use Paimon's mapping for vector maps.
            Some(crate::sql_context::sql_data_type_to_paimon_type(
                sql_type, true,
            )?)
        } else {
            None
        };
        if let Some(paimon_type) = paimon_type {
            let field = paimon::arrow::paimon_type_to_arrow_field("", &paimon_type)
                .map_err(to_datafusion_error)?;
            return Ok(Some(Arc::new(field)));
        }
        crate::binary_cast::BinaryTypePlanner.plan_type_field(sql_type)
    }
}

fn is_geo_type(ty: &DataType) -> bool {
    let DataType::Custom(name, _) = ty else {
        return false;
    };
    let [part] = name.0.as_slice() else {
        return false;
    };
    part.as_ident().is_some_and(|ident| {
        ident.quote_style.is_none()
            && (ident.value.eq_ignore_ascii_case("GEOMETRY")
                || ident.value.eq_ignore_ascii_case("GEOGRAPHY"))
    })
}

// sqlparser represents custom type modifiers as bare strings and formats them
// verbatim. Restore quoting in CASTs before storing or re-planning their SQL.
fn quote_geo_cast_modifiers(ty: &mut DataType) {
    if is_geo_type(ty) {
        if let DataType::Custom(_, args) = ty {
            for arg in args {
                *arg = format!("'{}'", arg.replace('\'', "''"));
            }
        }
        return;
    }
    match ty {
        DataType::Array(
            ArrayElemTypeDef::AngleBracket(t)
            | ArrayElemTypeDef::Parenthesis(t)
            | ArrayElemTypeDef::SquareBracket(t, _),
        ) => quote_geo_cast_modifiers(t),
        DataType::Map(k, v) => {
            quote_geo_cast_modifiers(k);
            quote_geo_cast_modifiers(v);
        }
        DataType::Struct(fields, _) => {
            for f in fields {
                quote_geo_cast_modifiers(&mut f.field_type);
            }
        }
        _ => {}
    }
}

fn contains_vector(sql_type: &DataType) -> bool {
    contains_sql_type(sql_type, |ty| match ty {
        DataType::Custom(name, _) => name
            .0
            .first()
            .and_then(|part| part.as_ident())
            .and_then(|ident| ident.value.get(..7))
            .is_some_and(|prefix| name.0.len() == 1 && prefix.eq_ignore_ascii_case("VECTOR<")),
        _ => false,
    })
}

fn contains_sql_type(sql_type: &DataType, predicate: fn(&DataType) -> bool) -> bool {
    if predicate(sql_type) {
        return true;
    }
    match sql_type {
        DataType::Array(
            ArrayElemTypeDef::AngleBracket(element)
            | ArrayElemTypeDef::SquareBracket(element, _)
            | ArrayElemTypeDef::Parenthesis(element),
        ) => contains_sql_type(element, predicate),
        DataType::Map(key, value) => {
            contains_sql_type(key, predicate) || contains_sql_type(value, predicate)
        }
        DataType::Struct(fields, _) => fields
            .iter()
            .any(|field| contains_sql_type(&field.field_type, predicate)),
        _ => false,
    }
}

/// sqlparser has no hook for parsing a data type with custom angle brackets.
/// When it stops at VECTOR's '<' in a type position, encode that type as one
/// quoted custom-type name and retry. The name survives AST formatting and
/// DataFusion reparsing without losing the element type or its nullability.
/// Waiting for the parser to reject '<' leaves comparisons involving a column
/// named vector untouched, even in statements that also contain VECTOR types.
pub(crate) fn parse_sql(sql: &str) -> Result<Vec<Statement>, ParserError> {
    let tokens = Parser::new(&GenericDialect {})
        .try_with_sql(sql)?
        .into_tokens();
    parse_statements(tokens).map(|(statements, _)| statements)
}

pub(crate) fn parse_expr(sql: &str) -> Result<Expr, ParserError> {
    let mut tokens = Parser::new(&GenericDialect {})
        .try_with_sql(sql)?
        .into_tokens();
    encode_explicit_types(&mut tokens);
    let mut expr = parse_with_vectors(&mut tokens, |parser| parser.parse_expr(), true)?;
    let _ = visit_expressions_mut(&mut expr, quote_geo_cast);
    Ok(expr)
}

pub(crate) fn parse_statements(
    mut tokens: Vec<TokenWithSpan>,
) -> Result<(Vec<Statement>, bool), ParserError> {
    let original_len = tokens.len();
    encode_explicit_types(&mut tokens);
    let mut statements = parse_with_vectors(&mut tokens, |parser| parser.parse_statements(), true)?;
    let _ = visit_expressions_mut(&mut statements, quote_geo_cast);
    Ok((statements, tokens.len() != original_len))
}

fn quote_geo_cast(expr: &mut Expr) -> std::ops::ControlFlow<()> {
    if let Expr::Cast { data_type, .. } = expr {
        quote_geo_cast_modifiers(data_type);
    }
    std::ops::ControlFlow::Continue(())
}

fn parse_with_vectors<T>(
    tokens: &mut Vec<TokenWithSpan>,
    parse: impl Fn(&mut Parser<'_>) -> Result<T, ParserError>,
    retry_composites: bool,
) -> Result<T, ParserError> {
    loop {
        let mut parser = Parser::new(&GenericDialect {}).with_tokens_with_locations(tokens.clone());
        match parse(&mut parser) {
            Ok(statements) => return Ok(statements),
            Err(error) => {
                let next = (parser.index()..tokens.len())
                    .find(|index| !matches!(tokens[*index].token, Token::Whitespace(_)));
                let changed = [Some(parser.get_current_index()), next]
                    .into_iter()
                    .flatten()
                    .any(|index| {
                        encode_vector_type(tokens, index)
                            || encode_vector_nullability(tokens, index)
                            || (retry_composites && encode_composite_type(tokens, index))
                    });
                if !changed {
                    return Err(error);
                }
            }
        }
    }
}

/// Named function parameters can rewind to their composite type on failure.
/// Parse just that type to recover the inner VECTOR location. Disabling this
/// fallback in the nested attempt prevents recursion on malformed composites.
fn encode_composite_type(tokens: &mut Vec<TokenWithSpan>, root: usize) -> bool {
    if !matches!(tokens.get(root).map(|token| &token.token), Some(Token::Word(w))
        if w.quote_style.is_none() && ["ARRAY", "MAP", "STRUCT"].iter().any(|name| w.value.eq_ignore_ascii_case(name)))
    {
        return false;
    }
    let mut suffix = tokens.split_off(root);
    let original_len = suffix.len();
    let _ = parse_with_vectors(&mut suffix, |parser| parser.parse_data_type(), false);
    let changed = suffix.len() != original_len;
    tokens.extend(suffix);
    changed
}

/// CAST parsing may rewind to the CAST keyword on failure. Parse explicit type
/// positions separately so the retry can still locate a nested VECTOR's '<'.
fn encode_explicit_types(tokens: &mut Vec<TokenWithSpan>) {
    let mut significant = tokens
        .iter()
        .filter(|t| !matches!(t.token, Token::Whitespace(_)))
        .peekable();
    let has_vector_type = std::iter::from_fn(|| {
        let token = significant.next()?;
        Some(matches!(&token.token, Token::Word(w) if w.quote_style.is_none() && w.value.eq_ignore_ascii_case("VECTOR"))
            && significant.peek().is_some_and(|next| next.token == Token::Lt))
    }).any(|is_vector| is_vector);
    if !has_vector_type {
        return;
    }
    let mut index = 0;
    while index < tokens.len() {
        let introduces_type = matches!(tokens[index].token, Token::DoubleColon)
            || matches!(&tokens[index].token, Token::Word(w) if matches!(w.keyword, Keyword::AS | Keyword::TYPE | Keyword::RETURNS));
        index += 1;
        if !introduces_type {
            continue;
        }
        let Some(root) =
            (index..tokens.len()).find(|i| !matches!(tokens[*i].token, Token::Whitespace(_)))
        else {
            continue;
        };
        if !matches!(&tokens[root].token, Token::Word(w) if w.quote_style.is_none() && ["VECTOR", "ARRAY", "MAP", "STRUCT"].iter().any(|name| w.value.eq_ignore_ascii_case(name)))
        {
            continue;
        }
        let mut suffix = tokens.split_off(root);
        let _ = parse_with_vectors(
            &mut suffix,
            |parser| {
                let ty = parser.parse_data_type()?;
                if matches!(&ty, DataType::Custom(name, _) if name.to_string().eq_ignore_ascii_case("VECTOR"))
                    && parser.peek_token().token == Token::Lt
                {
                    // A bare custom type otherwise succeeds without reading '<'.
                    parser.expect_token(&Token::EOF)?;
                }
                Ok(ty)
            },
            false,
        );
        tokens.extend(suffix);
    }
}

/// Generic nested types reject a child's NOT NULL suffix. Fold it only after
/// that rejection, leaving accepted top-level column constraints untouched.
fn encode_vector_nullability(tokens: &mut Vec<TokenWithSpan>, not: usize) -> bool {
    if !matches!(tokens.get(not).map(|t| &t.token), Some(Token::Word(w)) if w.keyword == Keyword::NOT)
    {
        return false;
    }
    let Some(start) = tokens[..not]
        .iter()
        .rposition(|t| !matches!(t.token, Token::Whitespace(_)))
    else {
        return false;
    };
    let Token::Word(word) = &tokens[start].token else {
        return false;
    };
    if word.quote_style.is_none()
        || !word.value.starts_with("VECTOR<")
        || !word.value.ends_with('>')
    {
        return false;
    }
    let Some(null) =
        (not + 1..tokens.len()).find(|i| !matches!(tokens[*i].token, Token::Whitespace(_)))
    else {
        return false;
    };
    if !matches!(&tokens[null].token, Token::Word(w) if w.keyword == Keyword::NULL) {
        return false;
    }
    let name = format!("{} NOT NULL", word.value);
    let replacement = TokenWithSpan::new(
        Token::make_word(&name, Some('"')),
        tokens[start].span.union(&tokens[null].span),
    );
    tokens.splice(start..=null, [replacement]);
    true
}

fn encode_vector_type(tokens: &mut Vec<TokenWithSpan>, open: usize) -> bool {
    if tokens.get(open).map(|t| &t.token) != Some(&Token::Lt) {
        return false;
    }
    let Some(start) = tokens[..open]
        .iter()
        .rposition(|t| !matches!(t.token, Token::Whitespace(_)))
    else {
        return false;
    };
    if !matches!(&tokens[start].token, Token::Word(w) if w.quote_style.is_none() && w.value.eq_ignore_ascii_case("VECTOR"))
    {
        return false;
    }

    let mut depth = 1;
    let mut inner = Vec::new();
    for end in open + 1..tokens.len() {
        let closings = match tokens[end].token {
            Token::Lt => {
                depth += 1;
                0
            }
            Token::Gt => 1,
            Token::ShiftRight => 2,
            _ => 0,
        };
        if closings >= depth && closings > 0 {
            inner.extend(std::iter::repeat_n(">".to_string(), (depth - 1) as usize));
            let name = format!("VECTOR<{}>", inner.join(" "));
            let mut replacement = vec![TokenWithSpan::new(
                Token::make_word(&name, Some('"')),
                tokens[start].span.union(&tokens[end].span),
            )];
            if closings > depth {
                replacement.push(TokenWithSpan::new(Token::Gt, tokens[end].span));
            }
            tokens.splice(start..=end, replacement);
            return true;
        }
        depth -= closings;
        if !matches!(tokens[end].token, Token::Whitespace(_)) {
            inner.push(tokens[end].token.to_string());
        }
    }
    false
}

/// Decode the custom-type name used by the SQL parser and share Paimon's type
/// validation between DDL and DataFusion expressions such as CAST.
pub(crate) fn sql_type_to_vector(
    sql_type: &DataType,
    nullable: bool,
) -> Result<Option<VectorType>> {
    let DataType::Custom(name, modifiers) = sql_type else {
        return Ok(None);
    };
    let [part] = name.0.as_slice() else {
        return Ok(None);
    };
    let Some(ident) = part.as_ident() else {
        return Ok(None);
    };
    if !ident
        .value
        .get(..7)
        .is_some_and(|s| s.eq_ignore_ascii_case("VECTOR<"))
    {
        return Ok(None);
    }
    if !modifiers.is_empty() {
        return Err(plan_datafusion_err!(
            "VECTOR does not accept additional type modifiers"
        ));
    }
    let parse = || -> Result<_, ParserError> {
        let mut parser = Parser::new(&GenericDialect {}).try_with_sql(&ident.value[7..])?;
        let element = parser.parse_data_type()?;
        let element_nullable = if parser.parse_keyword(Keyword::NOT) {
            parser.expect_keyword(Keyword::NULL)?;
            false
        } else {
            let _ = parser.parse_keyword(Keyword::NULL);
            true
        };
        parser.expect_token(&Token::Comma)?;
        let length = parser.parse_literal_uint()?;
        parser.expect_token(&Token::Gt)?;
        let vector_not_null = parser.parse_keyword(Keyword::NOT);
        if vector_not_null {
            parser.expect_keyword(Keyword::NULL)?;
        }
        parser.expect_token(&Token::EOF)?;
        Ok((element, element_nullable, length, vector_not_null))
    };
    let (element, element_nullable, length, vector_not_null) =
        parse().map_err(|error| plan_datafusion_err!("Invalid VECTOR type: {error}"))?;
    let length = u32::try_from(length).map_err(|_| {
        plan_datafusion_err!(
            "Vector length must be between 1 and {}",
            VectorType::MAX_LENGTH
        )
    })?;
    let element = crate::sql_context::sql_data_type_to_paimon_type(&element, element_nullable)?;
    VectorType::try_new(nullable && !vector_not_null, length, element)
        .map(Some)
        .map_err(to_datafusion_error)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn comparisons_literals_and_quoted_identifiers_keep_their_meaning() {
        for sql in [
            "SELECT vector < f, 3 > x + 1 FROM t",
            "SELECT 'VECTOR<FLOAT, 2>', \"vector\" < f, 3 > x FROM t",
            "SELECT vector /* VECTOR<INT, 2> */ < f, 3 > x FROM t",
        ] {
            assert_eq!(
                parse_sql(sql).unwrap(),
                Parser::parse_sql(&GenericDialect {}, sql).unwrap(),
                "{sql}"
            );
        }
    }

    #[test]
    fn malformed_composites_return_parse_errors() {
        for sql in [
            "CREATE FUNCTION f(x MAP<STRING, VECTOR<FLOAT, 2>>) RETURNS INT RETURN 1",
            "CREATE FUNCTION f(x ARRAY<VECTOR<FLOAT, 2>) RETURNS INT RETURN 1",
            "CREATE FUNCTION f(x STRUCT<v VECTOR<FLOAT, 2>) RETURNS INT RETURN 1",
        ] {
            assert!(parse_sql(sql).is_err(), "{sql}");
        }
    }

    #[test]
    fn vector_type_after_alter_column_type() {
        let statements = parse_sql("ALTER TABLE t ALTER COLUMN v TYPE VECTOR<DOUBLE, 4>").unwrap();
        let Statement::AlterTable(table) = &statements[0] else {
            panic!("expected ALTER TABLE");
        };
        let datafusion::sql::sqlparser::ast::AlterTableOperation::AlterColumn {
            op: datafusion::sql::sqlparser::ast::AlterColumnOperation::SetDataType { data_type, .. },
            ..
        } = &table.operations[0]
        else {
            panic!("expected ALTER COLUMN TYPE");
        };
        assert_eq!(
            sql_type_to_vector(data_type, true).unwrap().unwrap(),
            VectorType::new(4, PaimonDataType::Double(paimon::spec::DoubleType::new())).unwrap()
        );
    }
}
