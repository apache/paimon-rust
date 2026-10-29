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

//! Bounded sort buffers and pairwise disk merges. Disk work runs on blocking workers.

use super::extraction::sort_index_rows;
use super::SortedIndexKeyRow;
use crate::btree::key_serde::{DynKeyComparator, KeyComparator};
use crate::{Error, Result};
use std::cmp::Ordering;
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use tempfile::NamedTempFile;
use tokio::io::AsyncReadExt;

/// One run per binary level bounds open files during merging to two inputs.
/// Temporary files are private scratch state and are removed on failure or cancellation.
pub(super) struct IndexSorter {
    rows: Vec<SortedIndexKeyRow>,
    bytes: usize,
    budget: usize,
    cmp: KeyComparator,
    runs: Vec<Option<NamedTempFile>>,
}

impl IndexSorter {
    pub(super) fn new(budget: usize, cmp: KeyComparator) -> Self {
        Self {
            rows: Vec::new(),
            bytes: 0,
            budget,
            cmp,
            runs: Vec::new(),
        }
    }

    pub(super) fn push(&mut self, row: SortedIndexKeyRow) -> Result<()> {
        // Charge spare Vec slots as well as the actual serialized-key allocation.
        // The unstable sort uses no additional row-sized scratch allocation.
        let bytes = row
            .0
            .as_ref()
            .map_or(0, Vec::capacity)
            .checked_add(2 * std::mem::size_of::<SortedIndexKeyRow>())
            .ok_or_else(|| io_error(std::io::Error::other("index key size overflow")))?;
        if bytes > self.budget {
            return Err(Error::ResourceExhausted {
                message: format!(
                    "Sorted index key requires {bytes} bytes, exceeding write-buffer-size={}",
                    self.budget
                ),
            });
        }
        if bytes > self.budget - self.bytes {
            self.spill()?;
        }
        self.bytes += bytes;
        // Vec's first amortized allocation otherwise reserves four slots,
        // exceeding the two-slots-per-row charge for a one-row buffer.
        if self.rows.capacity() == 0 {
            self.rows.reserve_exact(1);
        }
        self.rows.push(row);
        Ok(())
    }

    fn spill(&mut self) -> Result<()> {
        if self.rows.is_empty() {
            return Ok(());
        }
        sort_index_rows(&mut self.rows, &self.cmp)?;
        let mut run = NamedTempFile::new().map_err(io_error)?;
        {
            let mut output = BufWriter::new(run.as_file_mut());
            for row in self.rows.drain(..) {
                write_row(&mut output, &row)?;
            }
            output.flush().map_err(io_error)?;
        }
        // Release the buffer before merging so old capacity cannot escape the budget.
        self.rows = Vec::new();
        self.bytes = 0;
        let mut level = 0;
        loop {
            if level == self.runs.len() {
                self.runs.push(None);
            }
            match self.runs[level].take() {
                Some(previous) => run = merge(previous, run, &self.cmp)?,
                None => {
                    self.runs[level] = Some(run);
                    return Ok(());
                }
            }
            level += 1;
        }
    }

    pub(super) fn finish(mut self) -> Result<SortedRows> {
        if self.runs.is_empty() {
            sort_index_rows(&mut self.rows, &self.cmp)?;
            return Ok(SortedRows::Memory(self.rows.into_iter()));
        }
        self.spill()?;
        let mut runs = self.runs.into_iter().flatten();
        let mut run = runs
            .next()
            .ok_or_else(|| io_error(std::io::Error::other("missing index sort run")))?;
        for other in runs {
            run = merge(run, other, &self.cmp)?;
        }
        run.as_file_mut()
            .seek(SeekFrom::Start(0))
            .map_err(io_error)?;
        let reader =
            tokio::io::BufReader::new(tokio::fs::File::from_std(run.reopen().map_err(io_error)?));
        Ok(SortedRows::Disk { _run: run, reader })
    }
}

pub(super) enum SortedRows {
    Memory(std::vec::IntoIter<SortedIndexKeyRow>),
    Disk {
        _run: NamedTempFile,
        reader: tokio::io::BufReader<tokio::fs::File>,
    },
}

impl SortedRows {
    pub(super) async fn next(&mut self) -> Result<Option<SortedIndexKeyRow>> {
        match self {
            Self::Memory(rows) => Ok(rows.next()),
            Self::Disk { reader, .. } => {
                let mut tag = [0];
                if reader.read(&mut tag).await.map_err(io_error)? == 0 {
                    return Ok(None);
                }
                let row_id = reader.read_i64_le().await.map_err(io_error)?;
                let key = if tag[0] == 0 {
                    None
                } else {
                    let length = reader.read_u64_le().await.map_err(io_error)?;
                    let length = usize::try_from(length).map_err(|_| {
                        io_error(std::io::Error::other("index sort key length overflow"))
                    })?;
                    let mut key = vec![0; length];
                    reader.read_exact(&mut key).await.map_err(io_error)?;
                    Some(key)
                };
                Ok(Some((key, row_id)))
            }
        }
    }
}

fn io_error(error: std::io::Error) -> Error {
    Error::UnexpectedError {
        message: format!("Sorted index temporary file: {error}"),
        source: Some(Box::new(error)),
    }
}

fn write_row(output: &mut impl Write, row: &SortedIndexKeyRow) -> Result<()> {
    output
        .write_all(&[u8::from(row.0.is_some())])
        .map_err(io_error)?;
    output.write_all(&row.1.to_le_bytes()).map_err(io_error)?;
    if let Some(key) = &row.0 {
        output
            .write_all(&(key.len() as u64).to_le_bytes())
            .map_err(io_error)?;
        output.write_all(key).map_err(io_error)?;
    }
    Ok(())
}

fn read_row(input: &mut impl Read) -> Result<Option<SortedIndexKeyRow>> {
    let mut tag = [0];
    if input.read(&mut tag).map_err(io_error)? == 0 {
        return Ok(None);
    }
    let mut row_id = [0; 8];
    input.read_exact(&mut row_id).map_err(io_error)?;
    let key = if tag[0] == 0 {
        None
    } else {
        let mut length = [0; 8];
        input.read_exact(&mut length).map_err(io_error)?;
        let length = usize::try_from(u64::from_le_bytes(length))
            .map_err(|_| io_error(std::io::Error::other("index sort key length overflow")))?;
        let mut key = vec![0; length];
        input.read_exact(&mut key).map_err(io_error)?;
        Some(key)
    };
    Ok(Some((key, i64::from_le_bytes(row_id))))
}

pub(super) fn compare_rows(
    left: &SortedIndexKeyRow,
    right: &SortedIndexKeyRow,
    cmp: &DynKeyComparator<'_>,
) -> Result<Ordering> {
    Ok(match (&left.0, &right.0) {
        (None, None) => left.1.cmp(&right.1),
        (None, Some(_)) => Ordering::Less,
        (Some(_), None) => Ordering::Greater,
        (Some(left_key), Some(right_key)) => {
            cmp(left_key, right_key)?.then_with(|| left.1.cmp(&right.1))
        }
    })
}

fn merge(
    mut left: NamedTempFile,
    mut right: NamedTempFile,
    cmp: &DynKeyComparator<'_>,
) -> Result<NamedTempFile> {
    left.as_file_mut()
        .seek(SeekFrom::Start(0))
        .map_err(io_error)?;
    right
        .as_file_mut()
        .seek(SeekFrom::Start(0))
        .map_err(io_error)?;
    let mut output = NamedTempFile::new().map_err(io_error)?;
    {
        let mut output = BufWriter::new(output.as_file_mut());
        let mut left = BufReader::new(left.as_file_mut());
        let mut right = BufReader::new(right.as_file_mut());
        let mut a = read_row(&mut left)?;
        let mut b = read_row(&mut right)?;
        while a.is_some() || b.is_some() {
            let take_left = match (&a, &b) {
                (Some(a), Some(b)) => compare_rows(a, b, cmp)? != Ordering::Greater,
                (Some(_), None) => true,
                _ => false,
            };
            if take_left {
                write_row(&mut output, a.as_ref().expect("left row present"))?;
                a = read_row(&mut left)?;
            } else {
                write_row(&mut output, b.as_ref().expect("right row present"))?;
                b = read_row(&mut right)?;
            }
        }
        output.flush().map_err(io_error)?;
    }
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn comparator() -> KeyComparator {
        Box::new(|a, b| Ok(a.cmp(b)))
    }

    #[tokio::test]
    async fn spilling_preserves_nulls_duplicates_and_row_id_ties() {
        let mut input = (0..100)
            .rev()
            .map(|id| (Some(vec![(id % 7) as u8]), id))
            .collect::<Vec<_>>();
        input.extend([(None, 101), (Some(vec![]), 102), (None, 100)]);
        let mut expected = input.clone();
        sort_index_rows(&mut expected, &comparator()).unwrap();
        let mut sorter = IndexSorter::new(256, comparator());
        for row in input {
            sorter.push(row).unwrap();
        }
        let mut rows = sorter.finish().unwrap();
        let SortedRows::Disk { _run, .. } = &rows else {
            panic!("test must spill")
        };
        let path = _run.path().to_owned();
        let mut actual = Vec::new();
        while let Some(row) = rows.next().await.unwrap() {
            actual.push(row);
        }
        assert_eq!(actual, expected);
        drop(rows);
        assert!(!path.exists());
    }

    #[tokio::test]
    async fn disk_merge_uses_numeric_comparison_instead_of_serialized_bytes() {
        use crate::btree::{make_key_comparator, serialize_datum};
        use crate::spec::{DataType, Datum, IntType};
        let data_type = DataType::Int(IntType::new());
        let values = [256, -1, 1, -256, 0, 255, i32::MAX, i32::MIN];
        let mut sorter = IndexSorter::new(128, make_key_comparator(&data_type));
        for (id, value) in values.into_iter().enumerate() {
            sorter
                .push((
                    Some(serialize_datum(&Datum::Int(value), &data_type)),
                    id as i64,
                ))
                .unwrap();
        }
        let mut rows = sorter.finish().unwrap();
        assert!(matches!(rows, SortedRows::Disk { .. }));
        let mut ids = Vec::new();
        while let Some((_, id)) = rows.next().await.unwrap() {
            ids.push(id);
        }
        assert_eq!(ids, [7, 3, 1, 4, 2, 5, 0, 6]);
    }

    #[test]
    fn oversized_keys_fail_and_dropping_sorter_removes_runs() {
        let mut sorter = IndexSorter::new(128, comparator());
        for id in 0..10 {
            sorter.push((Some(vec![1; 16]), id)).unwrap();
        }
        let paths = sorter
            .runs
            .iter()
            .flatten()
            .map(|run| run.path().to_owned())
            .collect::<Vec<_>>();
        assert!(!paths.is_empty());
        assert!(matches!(
            sorter.push((Some(vec![0; 129]), 11)),
            Err(Error::ResourceExhausted { .. })
        ));
        drop(sorter);
        assert!(paths.iter().all(|path| !path.exists()));
    }
}
