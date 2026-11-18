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

use std::collections::{HashMap, HashSet};
use std::sync::LazyLock;

use regex::Regex;

use crate::spec::{CoreOptions, DataField, DataType};
use crate::{Error, Result};

pub(super) fn validate_schema(
    fields: &[DataField],
    options: &HashMap<String, String>,
) -> Result<()> {
    let core_options = CoreOptions::new(options);
    for field in fields {
        validate_no_nested_generated_columns(field.data_type())?;
        if field.generation_expression().is_none() {
            continue;
        }
        if field.default_value().is_some() {
            return Err(Error::ConfigInvalid {
                message: format!(
                    "Generated column '{}' cannot have a default value.",
                    field.name()
                ),
            });
        }
        if core_options.rowkind_field() == Some(field.name()) {
            return Err(Error::ConfigInvalid {
                message: format!(
                    "Generated column '{}' cannot be used as rowkind.field.",
                    field.name()
                ),
            });
        }
    }
    Ok(())
}

pub(super) fn validate_no_nested_generated_columns(data_type: &DataType) -> Result<()> {
    match data_type {
        DataType::Row(row) => {
            for field in row.fields() {
                if field.generation_expression().is_some() {
                    return Err(Error::ConfigInvalid {
                        message: "Generated columns must be top-level columns.".into(),
                    });
                }
                validate_no_nested_generated_columns(field.data_type())?;
            }
        }
        DataType::Array(array) => validate_no_nested_generated_columns(array.element_type())?,
        DataType::Multiset(multiset) => {
            validate_no_nested_generated_columns(multiset.element_type())?;
        }
        DataType::Map(map) => {
            validate_no_nested_generated_columns(map.key_type())?;
            validate_no_nested_generated_columns(map.value_type())?;
        }
        _ => {}
    }
    Ok(())
}

/// Compare stable IDs after normalization so a drop and re-add cannot replace a protected field.
pub(super) fn validate_evolution(previous: &[DataField], next: &[DataField]) -> Result<()> {
    if !previous
        .iter()
        .chain(next)
        .any(|field| field.generation_expression().is_some())
    {
        return Ok(());
    }

    let mut next_by_id: HashMap<i32, &DataField> =
        next.iter().map(|field| (field.id(), field)).collect();
    let changed_fields: Vec<&DataField> = previous
        .iter()
        .filter(|field| {
            next_by_id.get(&field.id()).is_none_or(|target| {
                field.name() != target.name() || field.data_type() != target.data_type()
            })
        })
        .collect();

    for field in previous {
        let Some(expression) = field.generation_expression() else {
            continue;
        };
        if next_by_id.remove(&field.id()).is_none_or(|target| {
            field.name() != target.name()
                || field.data_type() != target.data_type()
                || field.generation_expression() != target.generation_expression()
        }) {
            return Err(Error::Unsupported {
                message: format!("Cannot change generated column '{}'.", field.name()),
            });
        }

        // Opaque expressions still allow additions, reordering and metadata edits.
        if changed_fields.is_empty() {
            continue;
        }
        let references = referenced_field_ids(expression, previous).ok_or_else(|| {
            Error::Unsupported {
                message: format!(
                    "Cannot rename, drop or change column types: dependencies of generated column '{}' cannot be determined from expression '{expression}'.",
                    field.name()
                ),
            }
        })?;
        for source in &changed_fields {
            if references.contains(&source.id()) {
                return Err(Error::Unsupported {
                    message: format!(
                        "Cannot rename, drop or change the type of column '{}' referenced by generated column '{}'.",
                        source.name(),
                        field.name()
                    ),
                });
            }
        }
    }

    if next_by_id
        .values()
        .any(|field| field.generation_expression().is_some())
    {
        return Err(Error::Unsupported {
            message: "Generated columns can only be defined when creating a table.".to_string(),
        });
    }
    Ok(())
}

/// Extract references using Java's generated-expression syntax. Unrecognized
/// syntax leaves every existing field as a potential dependency.
fn referenced_field_ids(expression: &str, fields: &[DataField]) -> Option<HashSet<i32>> {
    static TOKEN: LazyLock<Regex> = LazyLock::new(|| {
        Regex::new(r"^(`(?:``|[^`])+`|'(?:''|[^'])*'|[+-]?[0-9]+|[a-zA-Z_][a-zA-Z_0-9]*|[(),])")
            .expect("valid generated expression token pattern")
    });

    let mut remaining = expression.trim_start();
    let mut tokens = Vec::new();
    while !remaining.is_empty() {
        let token = TOKEN.find(remaining)?;
        tokens.push(token.as_str());
        remaining = remaining[token.end()..].trim_start();
    }
    let mut parser = DependencyParser {
        tokens,
        position: 0,
        fields,
        references: HashSet::new(),
    };
    parser.value(0)?;
    if parser.peek().is_some() {
        return None;
    }
    Some(parser.references)
}

struct DependencyParser<'a> {
    tokens: Vec<&'a str>,
    position: usize,
    fields: &'a [DataField],
    references: HashSet<i32>,
}

impl<'a> DependencyParser<'a> {
    fn peek(&self) -> Option<&'a str> {
        self.tokens.get(self.position).copied()
    }

    fn take(&mut self) -> Option<&'a str> {
        let token = self.peek()?;
        self.position += 1;
        Some(token)
    }

    fn expect(&mut self, expected: &str) -> Option<()> {
        self.take()?.eq_ignore_ascii_case(expected).then_some(())
    }

    fn take_if(&mut self, expected: &str) -> bool {
        if self
            .peek()
            .is_some_and(|token| token.eq_ignore_ascii_case(expected))
        {
            self.position += 1;
            true
        } else {
            false
        }
    }

    fn value(&mut self, depth: usize) -> Option<()> {
        if depth >= 128 {
            return None;
        }
        let token = self.take()?;
        if token.starts_with('\'')
            || token.eq_ignore_ascii_case("NULL")
            || token.starts_with(['+', '-'])
            || token.as_bytes()[0].is_ascii_digit()
        {
            return Some(());
        }
        if matches!(token, "(" | ")" | ",") {
            return None;
        }
        if self.peek() == Some("(") && !token.starts_with('`') {
            self.take()?;
            if token.eq_ignore_ascii_case("CAST") {
                self.value(depth + 1)?;
                self.expect("AS")?;
                self.cast_type()?;
            } else if self.peek() != Some(")") {
                self.value(depth + 1)?;
                while self.peek() == Some(",") {
                    self.take()?;
                    self.value(depth + 1)?;
                }
            }
            return self.expect(")");
        }
        if token.eq_ignore_ascii_case("DATE")
            && self.peek().is_some_and(|next| next.starts_with('\''))
        {
            self.take()?;
            return Some(());
        }
        let name = if token.starts_with('`') {
            token[1..token.len() - 1].replace("``", "`")
        } else {
            token.to_string()
        };
        let source = self.fields.iter().find(|field| field.name() == name)?;
        self.references.insert(source.id());
        Some(())
    }

    fn cast_type(&mut self) -> Option<()> {
        let name = self.take()?;
        if !name.starts_with(|ch: char| ch.is_ascii_alphabetic() || ch == '_') {
            return None;
        }
        if name.eq_ignore_ascii_case("DOUBLE") {
            self.take_if("PRECISION");
        }
        if self.take_if("(") {
            if name.eq_ignore_ascii_case("GEOMETRY") || name.eq_ignore_ascii_case("GEOGRAPHY") {
                self.geospatial_parameter()?;
                if name.eq_ignore_ascii_case("GEOGRAPHY") && self.take_if(",") {
                    self.geospatial_parameter()?;
                }
            } else {
                self.take()?.parse::<u32>().ok()?;
                if self.take_if(",") {
                    self.take()?.parse::<u32>().ok()?;
                }
            }
            self.expect(")")?;
        }
        if self.take_if("WITHOUT") {
            self.expect("TIME")?;
            self.expect("ZONE")?;
        } else if self.take_if("WITH") {
            self.expect("LOCAL")?;
            self.expect("TIME")?;
            self.expect("ZONE")?;
        }
        self.type_nullability()?;
        if self.take_if("ARRAY") || self.take_if("MULTISET") {
            self.type_nullability()?;
        }
        Some(())
    }

    fn geospatial_parameter(&mut self) -> Option<()> {
        self.take()?
            .starts_with(|ch: char| ch.is_ascii_alphabetic() || matches!(ch, '_' | '\'' | '`'))
            .then_some(())
    }

    fn type_nullability(&mut self) -> Option<()> {
        if self.take_if("NOT") {
            self.expect("NULL")?;
        } else {
            self.take_if("NULL");
        }
        Some(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::{ColumnMove, DataType, IntType, Schema, SchemaChange, TableSchema};

    fn fields() -> Vec<DataField> {
        ["source", "a`b", "DATE", "CAST", "DECIMAL", "NULL", "YEAR"]
            .into_iter()
            .enumerate()
            .map(|(id, name)| DataField::new(id as i32, name.into(), DataType::Int(IntType::new())))
            .collect()
    }

    #[test]
    fn generated_expression_dependencies() {
        let fields = fields();
        for (expression, expected) in [
            ("source", vec![0]),
            ("YEAR(source)", vec![0]),
            (
                " CONCAT('source isn''t a reference', `a``b`, LOWER(source), NULL, +1, -2) ",
                vec![0, 1],
            ),
            ("CAST(source AS DECIMAL(10, 2))", vec![0]),
            ("CAST(source AS DECIMAL(10, 2) NOT NULL)", vec![0]),
            ("CAST(source AS TIMESTAMP_NTZ(6))", vec![0]),
            ("CAST(source AS TIMESTAMP WITHOUT TIME ZONE NULL)", vec![0]),
            ("CAST(source AS NUMERIC(10, 2))", vec![0]),
            ("CAST(source AS GEOMETRY('EPSG:3857'))", vec![0]),
            ("CAST(source AS GEOGRAPHY(`OGC:CRS84`, KARNEY))", vec![0]),
            ("CAST(source AS DOUBLE PRECISION)", vec![0]),
            ("CAST(source AS INT NOT NULL ARRAY NOT NULL)", vec![0]),
            ("CAST(source AS FUTURE_TYPE)", vec![0]),
            (
                "cast(YEAR(source) as TIMESTAMP(6) WITHOUT TIME ZONE)",
                vec![0],
            ),
            ("DATEDIFF(DATE '2026-01-01', DATE)", vec![2]),
            ("CONCAT(`NULL`, `CAST`, DECIMAL, YEAR)", vec![3, 4, 5, 6]),
            ("FUTURE_FUNCTION(source, source)", vec![0]),
        ] {
            assert_eq!(
                referenced_field_ids(expression, &fields),
                Some(expected.into_iter().collect()),
                "{expression}"
            );
        }
        for expression in [
            "",
            "unknown",
            "source + 1",
            "source.child",
            "source[0]",
            "YEAR(source",
            "YEAR(source))",
            "CONCAT(source,)",
            "YEAR(source) extra",
            "CAST(source AS)",
            "CAST(source AS DECIMAL(10, 2)",
            "CAST(source AS DECIMAL(10, 2), YEAR)",
            "CAST(source AS DECIMAL(10, 2) YEAR)",
            "CAST(source AS 'YEAR')",
            "CAST(source AS DECIMAL(source, 2))",
            "((source)",
            "'unterminated",
            "`unterminated",
        ] {
            assert_eq!(
                referenced_field_ids(expression, &fields),
                None,
                "{expression}"
            );
        }
        let deep = format!("{}source{}", "YEAR(".repeat(128), ")".repeat(128));
        assert_eq!(referenced_field_ids(&deep, &fields), None);
    }

    #[test]
    fn generated_column_evolution_protects_each_definition_and_its_sources() {
        let ordinary = fields();
        let mut previous = ordinary.clone();
        previous.push(
            DataField::new(10, "generated".into(), DataType::Int(IntType::new()))
                .with_generation_expression(Some("source".into())),
        );
        previous.push(
            DataField::new(11, "second".into(), DataType::Int(IntType::new()))
                .with_generation_expression(Some("CAST(`a``b` AS INT)".into())),
        );
        let mut next = previous.clone();
        next[1] = next[1].clone().with_name("renamed".into());
        assert!(
            matches!(validate_evolution(&previous, &next), Err(Error::Unsupported { message }) if message.contains("column 'a`b' referenced by generated column 'second'"))
        );
        for expression in [None, Some("MONTH(source)".into())] {
            let mut next = previous.clone();
            let generated = next.last_mut().unwrap();
            *generated = generated.clone().with_generation_expression(expression);
            assert!(
                matches!(validate_evolution(&previous, &next), Err(Error::Unsupported { message }) if message.contains("Cannot change generated column"))
            );
        }
        for previous in [ordinary.as_slice(), previous.as_slice()] {
            for introduce in [true, false] {
                let mut next = previous.to_vec();
                if introduce {
                    next.push(
                        DataField::new(12, "new_generated".into(), DataType::Int(IntType::new()))
                            .with_generation_expression(Some("source".into())),
                    );
                } else {
                    next[1] = next[1]
                        .clone()
                        .with_generation_expression(Some("source".into()));
                }
                assert!(
                    matches!(validate_evolution(previous, &next), Err(Error::Unsupported { message }) if message.contains("only be defined when creating a table"))
                );
            }
        }
    }

    #[test]
    fn unknown_generated_expressions_reject_existing_column_changes() {
        for expression in ["source + 1", "source.child", ""] {
            let schema = Schema::builder()
                .column("source", DataType::Int(IntType::new()))
                .field(
                    DataField::new(1, "generated".into(), DataType::Int(IntType::new()))
                        .with_generation_expression(Some(expression.into())),
                )
                .column("unrelated", DataType::Int(IntType::new()))
                .option("alter-column-null-to-not-null.disabled", "false")
                .build()
                .unwrap();
            let stored = TableSchema::new(0, &schema);
            let changed = stored
                .apply_changes(vec![
                    SchemaChange::update_comment(Some("updated".into())),
                    SchemaChange::update_column_comment("generated".into(), "stored value".into()),
                    SchemaChange::set_option("test.option".into(), "value".into()),
                    SchemaChange::update_column_position(ColumnMove::move_first(
                        "generated".into(),
                    )),
                    SchemaChange::add_column("added".into(), DataType::Int(IntType::new())),
                ])
                .unwrap();
            assert_eq!(
                changed.fields()[0].generation_expression(),
                Some(expression)
            );
            for name in ["source", "unrelated"] {
                for change in [
                    SchemaChange::drop_column(name.into()),
                    SchemaChange::rename_column(name.into(), "renamed".into()),
                    SchemaChange::update_column_type(
                        name.into(),
                        DataType::BigInt(crate::spec::BigIntType::new()),
                    ),
                    SchemaChange::update_column_nullability(name.into(), false),
                ] {
                    let error = stored.apply_changes(vec![change.clone()]).unwrap_err();
                    assert!(
                        matches!(error, Error::Unsupported { ref message } if message.contains("cannot be determined")),
                        "{expression}: {change:?}: {error}"
                    );
                }
            }
        }
    }

    #[test]
    fn generated_columns_validate_final_normalized_schema() {
        for (key, nullable) in [
            ("source", false),
            ("source", true),
            ("generated", false),
            ("generated", true),
        ] {
            let schema = Schema::builder()
                .column("source", DataType::Int(IntType::new()))
                .field(
                    DataField::new(1, "generated".into(), DataType::Int(IntType::new()))
                        .with_generation_expression(Some("CAST(source AS INT)".into())),
                )
                .primary_key([key])
                .option("primary-key.nullable", nullable.to_string())
                .build()
                .unwrap();
            let stored = TableSchema::new(0, &schema);
            let mut changes = vec![SchemaChange::set_option(
                "primary-key.nullable".into(),
                (!nullable).to_string(),
            )];
            if nullable {
                changes.push(SchemaChange::remove_option("primary-key.nullable".into()));
            }
            for change in changes {
                let error = stored.apply_changes(vec![change.clone()]).unwrap_err();
                assert!(
                    matches!(error, Error::Unsupported { ref message } if message.contains("generated column")),
                    "{key}, nullable={nullable}: {change:?}: {error}"
                );
            }
        }
    }
}
