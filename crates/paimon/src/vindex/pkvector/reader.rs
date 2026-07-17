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

/// Sequential exact-scan source of vectors for one data file. Mirrors Java
/// `org.apache.paimon.index.pkvector.PkVectorReader`.
///
/// `Send` so a boxed reader can be held across the `.await` points of the async
/// search path (the returned future is spawned on a `Send` runtime by callers
/// such as the DataFusion integration).
pub(crate) trait PkVectorReader: Send {
    fn dimension(&self) -> usize;

    fn row_count(&self) -> i64;

    /// Read the next row's vector into `reuse` (`reuse.len() == dimension()`).
    /// Returns `false` when the physical row is a NULL vector: it is not scored,
    /// but the physical position still advances by one. Each call advances
    /// exactly one physical row.
    fn read_next_vector(&mut self, reuse: &mut [f32]) -> crate::Result<bool>;
}

#[cfg(test)]
pub(crate) mod test_support {
    use super::PkVectorReader;

    /// In-memory `PkVectorReader` for tests. `None` entries are NULL rows.
    /// Mirrors Java's test `ArrayReader`.
    pub(crate) struct ArrayReader {
        dimension: usize,
        vectors: Vec<Option<Vec<f32>>>,
        position: usize,
    }

    impl ArrayReader {
        pub(crate) fn new(dimension: usize, vectors: Vec<Option<Vec<f32>>>) -> Self {
            Self {
                dimension,
                vectors,
                position: 0,
            }
        }
    }

    impl PkVectorReader for ArrayReader {
        fn dimension(&self) -> usize {
            self.dimension
        }

        fn row_count(&self) -> i64 {
            self.vectors.len() as i64
        }

        fn read_next_vector(&mut self, reuse: &mut [f32]) -> crate::Result<bool> {
            assert_eq!(
                reuse.len(),
                self.dimension,
                "reuse buffer must equal dimension"
            );
            let entry = &self.vectors[self.position];
            self.position += 1;
            match entry {
                Some(vector) => {
                    reuse.copy_from_slice(vector);
                    Ok(true)
                }
                None => Ok(false),
            }
        }
    }
}
