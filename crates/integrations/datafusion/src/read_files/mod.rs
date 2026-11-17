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
mod raw;
mod scoped;
pub(crate) mod store;

use std::sync::{Arc, RwLock};

use self::glob::PathGlob;
use datafusion::arrow::array::StructArray;
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::catalog::{TableFunctionArgs, TableFunctionImpl};
use datafusion::common::{DFSchema, ScalarValue};
use datafusion::datasource::file_format::{parquet::ParquetFormat, FileFormat};
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::datasource::{TableProvider, ViewTable};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::session_state::{SessionState, SessionStateBuilder};
use datafusion::logical_expr::planner::{
    PlannedRelation, RelationPlanner, RelationPlannerContext, RelationPlanning,
};
use datafusion::logical_expr::{lit, LogicalPlanBuilder};
use datafusion::object_store::{path::Path, ObjectMeta, ObjectStore, ObjectStoreExt};
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::ast::{FunctionArg, FunctionArgExpr, TableFactor};
use futures::TryStreamExt;
use url::Url;

use options::{Format, Options};

fn error(message: impl std::fmt::Display) -> DataFusionError {
    DataFusionError::Plan(format!("read_files: {message}"))
}

/// Explicitly authorized directory prefixes, independent of catalog table grants.
#[derive(Clone, Debug, Default)]
pub(crate) struct Locations(Arc<RwLock<Vec<Location>>>);

#[derive(Clone, Debug)]
struct Location {
    url: Url,
    store: Arc<dyn ObjectStore>,
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

impl Locations {
    pub(crate) fn register(
        &self,
        ctx: &SessionContext,
        prefix: &str,
        file_io: Option<paimon::io::FileIO>,
    ) -> Result<()> {
        if prefix.contains(['*', '?', '[', '{']) {
            return Err(error("registered locations must not contain glob patterns"));
        }
        let mut url = parse_url(prefix)?;
        let store: Arc<dyn ObjectStore> = if url.scheme() == "file" {
            let path = url
                .to_file_path()
                .map_err(|_| error("invalid local location"))?;
            let path = std::fs::canonicalize(path)?;
            if !path.is_dir() {
                return Err(error("a registered location must be a directory"));
            }
            url = Url::from_directory_path(path).map_err(|_| error("invalid local location"))?;
            // Local paths always use a directory capability. A general FileIO
            // provider cannot safely enforce local path resolution at open time.
            Arc::new(store::FileStore::local(url.clone())?)
        } else {
            if !url.path().ends_with('/') {
                url.set_path(&format!("{}/", url.path()));
            }
            if let Some(file_io) = file_io {
                Arc::new(store::FileStore::file_io(url.clone(), file_io)?)
            } else {
                ctx.runtime_env().object_store_registry.get_store(&url)?
            }
        };
        let mut locations = self
            .0
            .write()
            .map_err(|_| error("location registry is poisoned"))?;
        locations.retain(|location| location.url != url);
        locations.push(Location { url, store });
        Ok(())
    }

    fn resolve(&self, url: &Url) -> Result<Arc<dyn ObjectStore>> {
        self.authorize(url)?;
        let target = Path::from_url_path(url.path())?;
        self.0
            .read()
            .map_err(|_| error("location registry is poisoned"))?
            .iter()
            .filter(|location| {
                same_authority(&location.url, url)
                    && Path::from_url_path(location.url.path()).is_ok_and(|prefix| {
                        target == prefix || target.prefix_match(&prefix).is_some()
                    })
            })
            .max_by_key(|location| location.url.path().len())
            .map(|location| location.store.clone())
            .ok_or_else(|| error("location is not registered"))
    }

    fn authorize(&self, url: &Url) -> Result<()> {
        let mut url = url.clone();
        if url.scheme() == "file" {
            let path = url
                .to_file_path()
                .map_err(|_| error("invalid local file path"))?;
            // Resolve symlinks as well as '..'; a registered directory must not
            // grant access to an unrelated local file through a symlink.
            if path.exists() {
                url = Url::from_file_path(std::fs::canonicalize(path)?)
                    .map_err(|_| error("invalid local file path"))?;
            }
        }
        let target = Path::from_url_path(url.path())?;
        let locations = self
            .0
            .read()
            .map_err(|_| error("location registry is poisoned"))?;
        let allowed = locations.iter().any(|location| {
            if !same_authority(&location.url, &url) {
                return false;
            }
            let Ok(prefix) = Path::from_url_path(location.url.path()) else {
                return false;
            };
            target == prefix || target.prefix_match(&prefix).is_some()
        });
        if allowed {
            Ok(())
        } else {
            Err(error("location is not registered; register an authorized directory with SQLContext::register_read_files_location"))
        }
    }
}

fn same_authority(a: &Url, b: &Url) -> bool {
    a.scheme() == b.scheme()
        && a.host_str() == b.host_str()
        && a.port() == b.port()
        && a.username() == b.username()
}

pub(crate) fn register(ctx: &SessionContext, locations: Locations) {
    ctx.register_udtf("read_files", Arc::new(ReadFiles { locations }));
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
        if name.0.len() != 1
            || !name.0[0]
                .as_ident()
                .is_some_and(|i| i.value.eq_ignore_ascii_case("read_files"))
        {
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
        Ok(RelationPlanning::Planned(Box::new(PlannedRelation::new(
            plan,
            alias.clone(),
        ))))
    }
}

#[derive(Debug)]
struct ReadFiles {
    locations: Locations,
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
        let locations = self.locations.clone();
        crate::runtime::block_on_with_runtime(
            async move { build(&state, options, locations).await },
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
    _state: &SessionState,
    options: &Options,
    locations: &Locations,
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
    locations.authorize(&base)?;
    let listing = ListingTableUrl::try_new(base.clone(), None)?;
    let store = locations.resolve(&base)?;
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
        locations.authorize(&url)?;
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
    locations: Locations,
) -> Result<Arc<dyn TableProvider>> {
    let (store, files) = discover(state, &options, &locations).await?;
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
    let file_format: Arc<dyn FileFormat> = match format {
        Format::Parquet => Arc::new(ParquetFormat::default()),
        _ => unreachable!(),
    };
    let schema = match schema {
        Some(schema) => schema,
        None => {
            let runtime =
                scoped::runtime(state.runtime_env(), files[0].url.clone(), store.clone())?;
            let state = SessionStateBuilder::new_from_existing(state.clone())
                .with_runtime_env(runtime)
                .build();
            file_format
                .infer_schema(
                    &state,
                    &store,
                    &files
                        .iter()
                        .map(|file| file.meta.clone())
                        .collect::<Vec<_>>(),
                )
                .await?
        }
    };
    let listing_options = ListingOptions::new(file_format).with_file_extension("");
    let has_source_metadata = schema
        .fields()
        .iter()
        .any(|field| field.name() == "_metadata");
    let mut combined = None;
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
            frame.with_column("_metadata", lit(metadata(&file)?))?
        };
        combined = Some(match combined {
            None => frame,
            Some(previous) => frame.union(previous)?,
        });
    }
    Ok(Arc::new(ViewTable::new(
        combined.unwrap().into_unoptimized_plan(),
        None,
    )))
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
    ];
    let values = [
        ScalarValue::Utf8(Some(file.url.to_string())),
        ScalarValue::Utf8(Some(
            file.meta.location.filename().unwrap_or("").to_string(),
        )),
        ScalarValue::Int64(Some(i64::try_from(file.meta.size).map_err(error)?)),
        ScalarValue::TimestampMicrosecond(Some(file.meta.last_modified.timestamp_micros()), None),
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
