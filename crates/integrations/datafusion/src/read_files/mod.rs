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

mod glob;
mod options;
mod parquet;
mod projection;
mod raw;
mod scoped;
pub(crate) mod store;

use std::sync::Arc;

use self::glob::PathGlob;
use datafusion::arrow::array::StructArray;
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::catalog::{TableFunctionArgs, TableFunctionImpl};
use datafusion::common::{DFSchema, ScalarValue};
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::datasource::{TableProvider, ViewTable};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::session_state::SessionState;
use datafusion::logical_expr::planner::{
    PlannedRelation, RelationPlanner, RelationPlannerContext, RelationPlanning,
};
use datafusion::logical_expr::{lit, LogicalPlan, LogicalPlanBuilder, Union};
use datafusion::object_store::{path::Path, ObjectMeta, ObjectStore, ObjectStoreExt};
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::ast::{
    FunctionArg, FunctionArgExpr, ObjectName, TableAliasColumnDef, TableFactor,
};
use futures::TryStreamExt;
use url::Url;

use options::{Format, Options};
pub(crate) use projection::sql;

const GENERATED_METADATA: &str = "paimon.read_files.generated_metadata";

fn metadata_expr(file: &File) -> Result<datafusion::logical_expr::Expr> {
    Ok(lit(metadata(file)?).alias_with_metadata(
        "_metadata",
        Some(datafusion::common::metadata::FieldMetadata::from(
            std::collections::HashMap::from([(GENERATED_METADATA.to_string(), "true".to_string())]),
        )),
    ))
}

fn error(message: impl std::fmt::Display) -> DataFusionError {
    DataFusionError::Plan(format!("read_files: {message}"))
}

fn is_read_files(name: &ObjectName) -> bool {
    name.0.len() == 1
        && name.0[0]
            .as_ident()
            .is_some_and(|i| i.value.eq_ignore_ascii_case("read_files"))
}

fn parse_url(path: &str) -> Result<Url> {
    let url = ListingTableUrl::parse(path)?;
    let url: &Url = url.as_ref();
    if matches!(url.scheme(), "http" | "https") {
        return Err(error("HTTP and HTTPS file URLs are not supported"));
    }
    if url.query().is_some()
        || url.fragment().is_some()
        || (!url.username().is_empty() && !matches!(url.scheme(), "abfs" | "abfss"))
        || url.password().is_some()
    {
        return Err(error(
            "paths must not contain credentials, a query, or a fragment",
        ));
    }
    if url.scheme() == "file" && url.host_str().is_some_and(|h| h != "localhost") {
        return Err(error("file URLs must refer to the local host"));
    }
    let mut url = url.clone();
    if url.scheme() == "file" {
        let path = url
            .to_file_path()
            .map_err(|_| error("invalid local path"))?;
        let mut ancestor = path.as_path();
        let mut suffix = Vec::new();
        while !ancestor.exists() {
            let Some(name) = ancestor.file_name() else {
                break;
            };
            suffix.push(name.to_owned());
            let Some(parent) = ancestor.parent() else {
                break;
            };
            ancestor = parent;
        }
        let mut normalized = std::fs::canonicalize(ancestor)?;
        for name in suffix.into_iter().rev() {
            normalized.push(name);
        }
        url = Url::from_file_path(normalized).map_err(|_| error("invalid local path"))?;
        if path.is_dir() && !url.path().ends_with('/') {
            url.set_path(&format!("{}/", url.path()));
        }
    }
    Ok(url)
}

fn file_url(base: &Url, path: &Path) -> Result<Url> {
    let mut url = base.clone();
    url.set_path("/");
    url.path_segments_mut()
        .map_err(|_| error("invalid object-store URL"))?
        .clear()
        .extend(path.parts().map(|part| part.as_ref().to_string()));
    Ok(url)
}

/// Resolve once per query using the same session FileIO routes as blob reads.
fn source(
    state: &SessionState,
    readers: &crate::BlobReaderRegistry,
    url: &Url,
) -> Result<Arc<dyn ObjectStore>> {
    let path = Path::from_url_path(url.path())?;
    let literal_uri = format!("{}/{path}", &url[..url::Position::BeforePath]);
    let directory_uri = format!("{}/", literal_uri.trim_end_matches('/'));
    let mut paths = vec![literal_uri, directory_uri];
    if let Ok(path) = url.to_file_path() {
        let path = path.to_string_lossy().to_string();
        let file_uri = format!("file:/{}", path.trim_start_matches('/'));
        for path in [path, file_uri] {
            paths.push(format!("{path}{}", std::path::MAIN_SEPARATOR));
            paths.push(path);
        }
    }
    let file_io = readers.resolve_paths(&paths);
    let mut directory = url.clone();
    if !directory.path().ends_with('/') {
        directory
            .path_segments_mut()
            .map_err(|_| error("invalid file URL"))?
            .pop();
    }
    if let Some(file_io) = file_io {
        return Ok(Arc::new(store::FileStore::file_io(directory, file_io)?));
    }
    if url.scheme() == "file" {
        // Pin the query's directory and reject special files without requiring
        // a separate application registration.
        return Ok(Arc::new(store::FileStore::local(directory)?));
    }
    if let Ok(store) = state.runtime_env().object_store_registry.get_store(url) {
        if !url.username().is_empty() {
            let mut unqualified = url.clone();
            unqualified
                .set_username("")
                .map_err(|_| error("invalid container URL"))?;
            if state
                .runtime_env()
                .object_store_registry
                .get_store(&unqualified)
                .is_ok_and(|other| Arc::ptr_eq(&store, &other))
            {
                return Err(error("ADLS container URLs require a container-aware object store registry or matching Catalog FileIO"));
            }
        }
        return Ok(store);
    }
    let file_io = paimon::io::FileIO::from_path(url.as_str())
        .and_then(|builder| builder.build())
        .map_err(error)?;
    Ok(Arc::new(store::FileStore::file_io(directory, file_io)?))
}

fn same_authority(a: &Url, b: &Url) -> bool {
    a.scheme() == b.scheme()
        && a.host_str() == b.host_str()
        && a.port() == b.port()
        && a.username() == b.username()
}

pub(crate) fn register(ctx: &SessionContext, readers: crate::BlobReaderRegistry) {
    ctx.register_udtf("read_files", Arc::new(ReadFiles { readers }));
}

/// Preserve option names before DataFusion's default table-function planning.
#[derive(Debug)]
pub(crate) struct ReadFilesPlanner;

impl RelationPlanner for ReadFilesPlanner {
    fn plan_relation(
        &self,
        relation: TableFactor,
        context: &mut dyn RelationPlannerContext,
    ) -> Result<RelationPlanning> {
        let (name, args, alias) = match &relation {
            TableFactor::Table {
                name,
                args: Some(args),
                alias,
                ..
            } => (name, &args.args, alias),
            TableFactor::Function {
                name, args, alias, ..
            } => (name, args, alias),
            _ => return Ok(RelationPlanning::Original(Box::new(relation))),
        };
        if !is_read_files(name) {
            return Ok(RelationPlanning::Original(Box::new(relation)));
        }
        let mut exprs = Vec::new();
        for (index, arg) in args.iter().enumerate() {
            match arg {
                FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) if index == 0 => {
                    exprs.push(context.sql_to_expr(expr.clone(), &DFSchema::empty())?);
                }
                FunctionArg::Named {
                    name,
                    arg: FunctionArgExpr::Expr(expr),
                    ..
                } if index > 0 => {
                    exprs.push(lit(name.value.clone()));
                    exprs.push(context.sql_to_expr(expr.clone(), &DFSchema::empty())?);
                }
                _ => {
                    return Err(error(
                        "expected read_files(path, option => value, ...); options must be named",
                    ))
                }
            }
        }
        let source = context
            .context_provider()
            .get_table_function_source("read_files", exprs)?;
        let plan = LogicalPlanBuilder::scan("read_files()", source, None)?.build()?;
        let mut alias = alias.clone();
        if let Some(alias) = &mut alias {
            if !alias.columns.is_empty()
                && plan
                    .schema()
                    .fields()
                    .iter()
                    .any(|field| field.metadata().contains_key(GENERATED_METADATA))
            {
                if alias.columns.len() + 1 != plan.schema().fields().len() {
                    return Err(error(
                        "table alias columns must match the number of data columns",
                    ));
                }
                alias
                    .columns
                    .push(TableAliasColumnDef::from_name("_metadata"));
            }
        }
        Ok(RelationPlanning::Planned(Box::new(PlannedRelation::new(
            plan, alias,
        ))))
    }
}

#[derive(Debug)]
struct ReadFiles {
    readers: crate::BlobReaderRegistry,
}

impl TableFunctionImpl for ReadFiles {
    fn call_with_args(&self, args: TableFunctionArgs) -> Result<Arc<dyn TableProvider>> {
        let options = Options::parse(args.exprs())?;
        let state = args
            .session()
            .as_any()
            .downcast_ref::<SessionState>()
            .ok_or_else(|| error("requires a DataFusion SessionState"))?
            .clone();
        let readers = self.readers.clone();
        crate::runtime::block_on_with_runtime(
            async move { build(&state, options, readers).await },
            "read_files: planning thread panicked",
        )
    }
}

#[derive(Clone, Debug)]
struct File {
    url: Url,
    meta: ObjectMeta,
}

async fn discover(
    state: &SessionState,
    options: &Options,
    readers: &crate::BlobReaderRegistry,
) -> Result<(Arc<dyn ObjectStore>, Vec<File>)> {
    // Split at the first wildcard *before* parsing the URI: '?' is a glob
    // operator here, not an HTTP query delimiter.
    let (base, pattern) = if let Some(index) = options.path.find(['*', '?', '[', '{']) {
        let slash = options.path[..index]
            .rfind('/')
            .ok_or_else(|| error("glob must have a directory prefix"))?;
        (
            &options.path[..=slash],
            Some(PathGlob::new(&options.path[slash + 1..])?),
        )
    } else {
        (options.path.as_str(), None)
    };
    let base = parse_url(base)?;
    let listing = ListingTableUrl::try_new(base.clone(), None)?;
    let store = source(state, readers, &base)?;
    let prefix = listing.prefix();
    let metas = if pattern.is_none() && !base.path().ends_with('/') {
        match store.head(prefix).await {
            Ok(meta) => vec![meta],
            Err(datafusion::object_store::Error::NotFound { .. }) => {
                store.list(Some(prefix)).try_collect().await?
            }
            Err(e) => return Err(e.into()),
        }
    } else {
        store.list(Some(prefix)).try_collect().await?
    };
    let mut files = Vec::new();
    for meta in metas {
        let Some(relative) = meta.location.prefix_match(prefix) else {
            continue;
        };
        let relative = relative
            .map(|part| part.as_ref().to_string())
            .collect::<Vec<_>>();
        if relative
            .iter()
            .any(|s| s.starts_with('.') || s.starts_with('_'))
        {
            continue;
        }
        let relative_path = relative.join("/");
        if pattern.as_ref().is_some_and(|p| !p.matches(&relative_path)) {
            continue;
        }
        if options
            .file_pattern
            .as_ref()
            .is_some_and(|p| !p.matches(meta.location.filename().unwrap_or("")))
        {
            continue;
        }
        let url = file_url(&base, &meta.location)?;
        files.push(File { url, meta });
    }
    files.sort_by(|a, b| a.meta.location.cmp(&b.meta.location));
    if files.is_empty() {
        return Err(error("no files matched the path"));
    }
    Ok((store, files))
}

async fn build(
    state: &SessionState,
    mut options: Options,
    readers: crate::BlobReaderRegistry,
) -> Result<Arc<dyn TableProvider>> {
    let (store, files) = discover(state, &options, &readers).await?;
    let format = match options.format {
        Some(format) => format,
        None => Format::infer(&files[0].meta.location)?,
    };
    if options.format.is_none()
        && files
            .iter()
            .any(|file| Format::infer(&file.meta.location).ok() != Some(format))
    {
        return Err(error("files have mixed formats; specify format explicitly"));
    }
    options.validate(format)?;
    let schema = options.schema(state)?;
    if format == Format::Csv && options.header.is_none() {
        options.header = Some(schema.is_none());
    }
    let ctx = SessionContext::new_with_state(state.clone());
    if matches!(format, Format::Binary | Format::Text | Format::Json) {
        return raw::provider(&ctx, store, files, options, format, schema).await;
    }
    if format == Format::Csv {
        let schema = match schema {
            Some(schema) => schema,
            None => raw::infer_csv_schema(state, &store, &files, &options).await?,
        };
        return raw::provider(&ctx, store, files, options, format, Some(schema)).await;
    }
    let file_format = match format {
        Format::Parquet => ParquetFormat::default(),
        _ => unreachable!(),
    };
    let schema = match schema {
        Some(schema) => schema,
        None => parquet::infer_schema(state, &store, &files, &file_format).await?,
    };
    // A real field can carry metadata from an earlier export. Provenance tags
    // belong to this source binding and must never hide persisted user columns.
    let schema = Arc::new(datafusion::arrow::datatypes::Schema::new_with_metadata(
        schema
            .fields()
            .iter()
            .map(|field| {
                let mut metadata = field.metadata().clone();
                metadata.remove(GENERATED_METADATA);
                field.as_ref().clone().with_metadata(metadata)
            })
            .collect::<Vec<_>>(),
        schema.metadata().clone(),
    ));
    let listing_options = ListingOptions::new(Arc::new(file_format)).with_file_extension("");
    let has_source_metadata = schema
        .fields()
        .iter()
        .any(|field| field.name() == "_metadata");
    let mut plans = Vec::with_capacity(files.len());
    for file in files {
        let config = ListingTableConfig::new(ListingTableUrl::try_new(file.url.clone(), None)?)
            .with_listing_options(listing_options.clone())
            .with_schema(schema.clone());
        let provider = Arc::new(scoped::ScopedTable::new(
            ListingTable::try_new(config)?,
            file.url.clone(),
            store.clone(),
        ));
        let frame = ctx.read_table(provider)?;
        let frame = if has_source_metadata {
            frame
        } else {
            frame.with_column("_metadata", metadata_expr(&file)?)?
        };
        plans.push(frame.into_unoptimized_plan());
    }
    combined_provider(plans)
}

fn combined_provider(mut plans: Vec<LogicalPlan>) -> Result<Arc<dyn TableProvider>> {
    let plan = if plans.len() == 1 {
        plans.pop().unwrap()
    } else {
        LogicalPlan::Union(Union::try_new(plans.into_iter().map(Arc::new).collect())?)
    };
    Ok(Arc::new(ViewTable::new(plan, None)))
}

fn metadata(file: &File) -> Result<ScalarValue> {
    let fields = vec![
        Field::new("file_path", DataType::Utf8, false),
        Field::new("file_name", DataType::Utf8, false),
        Field::new("file_size", DataType::Int64, false),
        Field::new(
            "file_modification_time",
            DataType::Timestamp(datafusion::arrow::datatypes::TimeUnit::Microsecond, None),
            false,
        ),
        Field::new("file_block_start", DataType::Int64, false),
        Field::new("file_block_length", DataType::Int64, false),
    ];
    let values = [
        ScalarValue::Utf8(Some(file.url.to_string())),
        ScalarValue::Utf8(Some(
            file.meta.location.filename().unwrap_or("").to_string(),
        )),
        ScalarValue::Int64(Some(i64::try_from(file.meta.size).map_err(error)?)),
        ScalarValue::TimestampMicrosecond(Some(file.meta.last_modified.timestamp_micros()), None),
        ScalarValue::Int64(Some(0)),
        ScalarValue::Int64(Some(i64::try_from(file.meta.size).map_err(error)?)),
    ];
    let arrays = values
        .iter()
        .map(ScalarValue::to_array)
        .collect::<Result<Vec<_>>>()?;
    Ok(ScalarValue::Struct(Arc::new(StructArray::try_new(
        fields.into(),
        arrays,
        None,
    )?)))
}
