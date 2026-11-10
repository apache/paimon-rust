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

use std::ops::ControlFlow;

use datafusion::sql::sqlparser::ast::{
    Expr, Ident, Merge, MergeAction, TableFactor, VisitMut, VisitorMut,
};

use crate::sql_context::normalize_schema_identifier;

fn qualifier(table: &TableFactor) -> Vec<Ident> {
    match table {
        TableFactor::Table {
            alias: Some(alias), ..
        }
        | TableFactor::Derived {
            alias: Some(alias), ..
        } => vec![alias.name.clone()],
        TableFactor::Table { name, .. } => name
            .0
            .iter()
            .filter_map(|p| p.as_ident().cloned())
            .collect(),
        _ => vec![],
    }
}

/// Restore source-only resolution for NOT MATCHED without changing variables,
/// lambda bindings, or already-qualified target/source references.
pub(crate) fn qualify_not_matched_exprs(merge: &mut Merge, normalize: bool) {
    let source = qualifier(&merge.source);
    if source.is_empty() {
        return;
    }
    let mut scope = SourceScope {
        source,
        target: qualifier(&merge.table),
        normalize,
        locals: vec![],
    };
    for clause in &mut merge.clauses {
        if let MergeAction::Insert(insert) = &mut clause.action {
            let _ = insert.kind.visit(&mut scope);
            let _ = clause.predicate.visit(&mut scope);
        }
    }
}

struct SourceScope {
    source: Vec<Ident>,
    target: Vec<Ident>,
    normalize: bool,
    locals: Vec<Vec<String>>,
}

impl SourceScope {
    fn name(&self, ident: &Ident) -> String {
        normalize_schema_identifier(ident, self.normalize)
    }

    fn is_variable(&self, ident: &Ident) -> bool {
        (ident.quote_style.is_none() && ident.value.starts_with('@'))
            || self
                .locals
                .iter()
                .any(|scope| scope.contains(&self.name(ident)))
    }

    fn is_qualified(&self, parts: &[Ident]) -> bool {
        [&self.source, &self.target].into_iter().any(|qualifier| {
            (0..qualifier.len()).any(|start| {
                let qualifier = &qualifier[start..];
                parts.len() > qualifier.len()
                    && parts
                        .iter()
                        .zip(qualifier)
                        .all(|(part, name)| self.name(part) == self.name(name))
            })
        })
    }
}

impl VisitorMut for SourceScope {
    type Break = ();

    fn pre_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<Self::Break> {
        match expr {
            Expr::Lambda(lambda) => {
                self.locals
                    .push(lambda.params.iter().map(|p| self.name(&p.name)).collect());
            }
            Expr::Identifier(ident) if !self.is_variable(ident) => {
                let mut parts = self.source.clone();
                parts.push(ident.clone());
                *expr = Expr::CompoundIdentifier(parts);
            }
            Expr::CompoundIdentifier(parts)
                if !self.is_variable(&parts[0]) && !self.is_qualified(parts) =>
            {
                let mut qualified = self.source.clone();
                qualified.extend(parts.iter().cloned());
                *parts = qualified;
            }
            _ => {}
        }
        ControlFlow::Continue(())
    }

    fn post_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<Self::Break> {
        if matches!(expr, Expr::Lambda(_)) {
            self.locals.pop();
        }
        ControlFlow::Continue(())
    }
}
