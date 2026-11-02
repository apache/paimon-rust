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

// Backport sqlparser #2436 without replacing DataFusion's AST dependency.
// Correct parsing plus explicit parentheses is needed: execution reparses SQL with 0.62.
use datafusion::sql::sqlparser::{
    ast::{Expr, Statement, VisitMut, VisitorMut},
    dialect::{Dialect, GenericDialect, Precedence},
    keywords::Keyword,
    parser::{Parser, ParserError},
    tokenizer::{Token, TokenWithSpan, Tokenizer},
};
use std::{any::TypeId, ops::ControlFlow};

#[derive(Debug)]
struct FixedDialect {
    enabled: bool,
}

// Preserve every GenericDialect capability in the pinned parser version.
macro_rules! generic_features {
    ($($name:ident),* $(,)?) => { $(fn $name(&self) -> bool { GenericDialect.$name() })* };
}
impl Dialect for FixedDialect {
    fn dialect(&self) -> TypeId {
        GenericDialect.dialect()
    }
    fn is_identifier_start(&self, ch: char) -> bool {
        GenericDialect.is_identifier_start(ch)
    }
    fn is_identifier_part(&self, ch: char) -> bool {
        GenericDialect.is_identifier_part(ch)
    }
    fn is_delimited_identifier_start(&self, ch: char) -> bool {
        GenericDialect.is_delimited_identifier_start(ch)
    }
    generic_features!(
        supports_unicode_string_literal,
        supports_partition_by_after_order_by,
        supports_array_join_syntax,
        supports_group_by_expr,
        supports_group_by_with_modifier,
        supports_left_associative_joins_without_parens,
        supports_connect_by,
        supports_match_recognize,
        supports_pipe_operator,
        supports_start_transaction_modifier,
        supports_window_function_null_treatment_arg,
        supports_dictionary_syntax,
        supports_window_clause_named_window_reference,
        supports_parenthesized_set_variables,
        supports_select_wildcard_except,
        support_map_literal_syntax,
        allow_extract_custom,
        allow_extract_single_quotes,
        supports_extract_comma_syntax,
        supports_create_view_comment_syntax,
        supports_parens_around_table_factor,
        supports_values_as_table_factor,
        supports_create_index_with_clause,
        supports_explain_with_utility_options,
        supports_limit_comma,
        supports_update_order_by,
        supports_from_first_select,
        supports_projection_trailing_commas,
        supports_asc_desc_in_column_definition,
        supports_try_convert,
        supports_bitwise_shift_operators,
        supports_comment_on,
        supports_load_extension,
        supports_named_fn_args_with_assignment_operator,
        supports_struct_literal,
        supports_empty_projections,
        supports_nested_comments,
        supports_multiline_comment_hints,
        supports_user_host_grantee,
        supports_string_escape_constant,
        supports_array_typedef_with_brackets,
        supports_match_against,
        supports_set_names,
        supports_comma_separated_set_assignments,
        supports_filter_during_aggregation,
        supports_select_wildcard_exclude,
        supports_data_type_signed_suffix,
        supports_interval_options,
        supports_quote_delimited_string,
        supports_select_wildcard_replace,
        supports_select_wildcard_ilike,
        supports_select_wildcard_rename,
        supports_optimize_table,
        supports_install,
        supports_detach,
        supports_prewhere,
        supports_with_fill,
        supports_limit_by,
        supports_interpolate,
        supports_settings,
        supports_select_format,
        supports_comment_optimizer_hint,
        supports_constraint_keyword_without_name,
        supports_key_column_option,
        supports_comma_separated_trim,
        supports_cte_without_as,
        supports_select_item_multi_column_alias,
        supports_xml_expressions,
    );
    fn prec_value(&self, precedence: Precedence) -> u8 {
        // JSON/custom operators bind above IS and comparisons, as in the upstream fix.
        if self.enabled && matches!(precedence, Precedence::PgOther) {
            21
        } else {
            GenericDialect.prec_value(precedence)
        }
    }
    fn parse_infix(
        &self,
        parser: &mut Parser,
        left: &Expr,
        precedence: u8,
    ) -> Option<Result<Expr, ParserError>> {
        if !self.enabled {
            return None;
        }
        if parser.parse_keywords(&[Keyword::IS, Keyword::DISTINCT, Keyword::FROM]) {
            Some(
                parser
                    .parse_subexpr(precedence)
                    .map(|right| Expr::IsDistinctFrom(Box::new(left.clone()), Box::new(right))),
            )
        } else if parser.parse_keywords(&[
            Keyword::IS,
            Keyword::NOT,
            Keyword::DISTINCT,
            Keyword::FROM,
        ]) {
            Some(
                parser
                    .parse_subexpr(precedence)
                    .map(|right| Expr::IsNotDistinctFrom(Box::new(left.clone()), Box::new(right))),
            )
        } else {
            None
        }
    }
}

fn dialect(tokens: &[TokenWithSpan]) -> FixedDialect {
    let keywords = tokens
        .iter()
        .filter_map(|token| match &token.token {
            Token::Whitespace(_) => None,
            Token::Word(word) => Some(word.keyword),
            _ => Some(Keyword::NoKeyword),
        })
        .collect::<Vec<_>>();
    FixedDialect {
        enabled: keywords
            .windows(3)
            .any(|w| w == [Keyword::IS, Keyword::DISTINCT, Keyword::FROM])
            || keywords
                .windows(4)
                .any(|w| w == [Keyword::IS, Keyword::NOT, Keyword::DISTINCT, Keyword::FROM]),
    }
}
fn tokens(sql: &str) -> Result<Vec<TokenWithSpan>, ParserError> {
    Tokenizer::new(&GenericDialect, sql)
        .tokenize_with_location()
        .map_err(|e| ParserError::TokenizerError(e.to_string()))
}

pub(crate) fn parse_tokens(tokens: Vec<TokenWithSpan>) -> Result<Vec<Statement>, ParserError> {
    let dialect = dialect(&tokens);
    let mut statements = Parser::new(&dialect)
        .with_tokens_with_locations(tokens)
        .parse_statements()?;
    if dialect.enabled {
        for statement in &mut statements {
            let _ = statement.visit(&mut Parenthesize);
        }
    }
    Ok(statements)
}
pub(crate) fn parse_sql(sql: &str) -> Result<Vec<Statement>, ParserError> {
    parse_tokens(tokens(sql)?)
}
pub(crate) fn has_distinct(statement: &Statement) -> bool {
    let mut found = false;
    let _: ControlFlow<()> =
        datafusion::sql::sqlparser::ast::visit_expressions(statement, |expr| {
            found |= matches!(expr, Expr::IsDistinctFrom(..) | Expr::IsNotDistinctFrom(..));
            ControlFlow::Continue(())
        });
    found
}
pub(crate) fn parse_expr(sql: &str) -> Result<Expr, ParserError> {
    let tokens = tokens(sql)?;
    let dialect = dialect(&tokens);
    let mut expr = Parser::new(&dialect)
        .with_tokens_with_locations(tokens)
        .parse_expr()?;
    if dialect.enabled {
        let _ = expr.visit(&mut Parenthesize);
    }
    Ok(expr)
}
struct Parenthesize;
impl VisitorMut for Parenthesize {
    type Break = ();
    fn post_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<()> {
        // Protect binding on both sides, including JSON operators, NOT and chained IS tests.
        if matches!(
            expr,
            Expr::BinaryOp { .. }
                | Expr::UnaryOp { .. }
                | Expr::IsDistinctFrom(..)
                | Expr::IsNotDistinctFrom(..)
                | Expr::IsNull(..)
                | Expr::IsNotNull(..)
                | Expr::IsTrue(..)
                | Expr::IsNotTrue(..)
                | Expr::IsFalse(..)
                | Expr::IsNotFalse(..)
                | Expr::IsUnknown(..)
                | Expr::IsNotUnknown(..)
        ) {
            let inner = std::mem::replace(expr, Expr::Tuple(Vec::new()));
            *expr = Expr::Nested(Box::new(inner));
        }
        ControlFlow::Continue(())
    }
}
