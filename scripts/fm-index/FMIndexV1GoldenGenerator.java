/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.globalindex.fmindex;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.globalindex.ResultEntry;
import org.apache.paimon.globalindex.io.GlobalIndexFileWriter;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.VarCharType;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

/** Generates the canonical uncompressed FM-index V1 fixture with the Java writer. */
public final class FMIndexV1GoldenGenerator {

    private FMIndexV1GoldenGenerator() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            throw new IllegalArgumentException("Expected one output file argument.");
        }

        java.nio.file.Path output = Paths.get(args[0]).toAbsolutePath();
        java.nio.file.Path parent = output.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }

        LocalFileIO fileIO = LocalFileIO.create();
        org.apache.paimon.fs.Path paimonOutput =
                new org.apache.paimon.fs.Path(output.toUri());
        GlobalIndexFileWriter fileWriter =
                new GlobalIndexFileWriter() {
                    @Override
                    public String newFileName(String prefix) {
                        return output.getFileName().toString();
                    }

                    @Override
                    public PositionOutputStream newOutputStream(String fileName)
                            throws IOException {
                        return fileIO.newOutputStream(paimonOutput, true);
                    }
                };

        Options options = new Options();
        options.set(FMGlobalIndexOptions.PARTITION_ROW_COUNT, 100);
        options.set(FMGlobalIndexOptions.SA_SAMPLE_RATE, 4);
        options.set(FMGlobalIndexOptions.COMPRESSION, "none");
        options.set(FMGlobalIndexOptions.LOCATE_COST_RATIO, 1d);
        DataField field =
                new DataField(1, "text", new VarCharType(VarCharType.MAX_LENGTH));
        FMGlobalIndexWriter writer = new FMGlobalIndexer(field, options).createWriter(fileWriter);
        writer.write(BinaryString.fromString("banana"), 0);
        writer.write(null, 1);
        writer.write(
                BinaryString.fromBytes(
                        new byte[] {0, (byte) 0xFF, 'b', 'a', 'n', 'a', 'n', 'a'}),
                2);
        writer.write(BinaryString.fromString(""), 3);

        List<ResultEntry> entries = writer.finish();
        if (entries.size() != 1 || !Files.isRegularFile(output)) {
            throw new IllegalStateException("Java FM-index writer did not produce one file.");
        }
    }
}
