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

use datafusion::common::{DataFusionError, Result};
use datafusion::sql::sqlparser::ast::{visit_expressions_mut, BinaryOperator, Expr, Statement};
use datafusion::sql::sqlparser::{
    dialect::GenericDialect,
    keywords::Keyword,
    tokenizer::{Token, Tokenizer},
};
use std::borrow::Cow;
use std::ops::ControlFlow;

/// Work around sqlparser 0.62 consuming a bare NULL regex pattern before parsing it.
pub(crate) fn parseable_sql(sql: &str) -> Result<Cow<'_, str>> {
    let tokens = Tokenizer::new(&GenericDialect {}, sql)
        .tokenize_with_location()
        .map_err(|error| DataFusionError::Plan(format!("SQL parse error: {error}")))?;
    let mut regex = false;
    let mut operand = false;
    let mut pending_not = false;
    let mut positions = Vec::new();
    for token in tokens {
        if matches!(token.token, Token::Whitespace(_)) {
            continue;
        }
        if regex && matches!(&token.token,Token::Word(w) if w.keyword==Keyword::NULL) {
            let mut offset = 0;
            for (index, line) in sql.split_inclusive('\n').enumerate() {
                if index + 1 == token.span.start.line as usize {
                    offset += line
                        .char_indices()
                        .nth(token.span.start.column as usize - 1)
                        .map(|(i, _)| i)
                        .unwrap_or(line.len());
                    break;
                }
                offset += line.len();
            }
            positions.push(offset);
        }
        if matches!(&token.token,Token::Word(w) if w.quote_style.is_none() && matches!(w.keyword,Keyword::RLIKE|Keyword::REGEXP))
        {
            regex = operand || pending_not;
            operand = !regex;
            pending_not = false;
        } else if matches!(&token.token,Token::Word(w) if w.quote_style.is_none() && w.keyword == Keyword::NOT)
        {
            pending_not = operand;
            operand = false;
            regex = false;
        } else {
            operand = expression_end(&token.token);
            pending_not = false;
            regex = false;
        }
    }
    if positions.is_empty() {
        return Ok(Cow::Borrowed(sql));
    }
    let mut output = sql.to_string();
    for offset in positions.into_iter().rev() {
        output.replace_range(offset..offset + 4, "(NULL)");
    }
    Ok(Cow::Owned(output))
}

fn expression_end(token: &Token) -> bool {
    match token {
        Token::Word(word) => {
            word.keyword == Keyword::END
                || (!datafusion::sql::sqlparser::keywords::RESERVED_FOR_COLUMN_ALIAS
                    .contains(&word.keyword)
                    && !matches!(
                        word.keyword,
                        Keyword::AND
                            | Keyword::OR
                            | Keyword::NOT
                            | Keyword::WHEN
                            | Keyword::THEN
                            | Keyword::ELSE
                            | Keyword::AS
                            | Keyword::ON
                            | Keyword::BY
                            | Keyword::CASE
                            | Keyword::IN
                            | Keyword::IS
                            | Keyword::LIKE
                            | Keyword::ILIKE
                            | Keyword::RLIKE
                            | Keyword::REGEXP
                            | Keyword::BETWEEN
                            | Keyword::DISTINCT
                            | Keyword::ALL
                    ))
        }
        Token::Number(..)
        | Token::SingleQuotedString(_)
        | Token::DoubleQuotedString(_)
        | Token::EscapedStringLiteral(_)
        | Token::NationalStringLiteral(_)
        | Token::HexStringLiteral(_)
        | Token::DollarQuotedString(_)
        | Token::Placeholder(_)
        | Token::RParen
        | Token::RBracket => true,
        _ => false,
    }
}

/// Lower regex operators to DataFusion's native regex operators after parsing.
pub(crate) fn rewrite(statement: &mut Statement) -> bool {
    let mut changed = false;
    let _: ControlFlow<()> = visit_expressions_mut(statement, |node| {
        if let Expr::RLike {
            negated,
            expr,
            pattern,
            ..
        } = node
        {
            *node = Expr::Nested(Box::new(Expr::BinaryOp {
                left: expr.clone(),
                op: if *negated {
                    BinaryOperator::PGRegexNotMatch
                } else {
                    BinaryOperator::PGRegexMatch
                },
                right: pattern.clone(),
            }));
            changed = true;
        }
        ControlFlow::Continue(())
    });
    changed
}
