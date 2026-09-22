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

package org.apache.paimon.fixture;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import static org.apache.paimon.options.CatalogOptions.WAREHOUSE;

/** Generates committed Java tables used by paimon-rust FileIndex compatibility tests. */
public class JavaFileIndexTableFixtureGeneratorTest {

    private static final String DATABASE = "default";

    @Test
    public void generate() throws Exception {
        String output = System.getProperty("generate.file-index.fixture.output");
        if (output == null || output.trim().isEmpty()) {
            throw new IllegalArgumentException(
                    "Set -Dgenerate.file-index.fixture.output=<empty-output-directory>");
        }

        Path warehouse = Paths.get(output).toAbsolutePath().normalize();
        Options catalogOptions = new Options();
        catalogOptions.set(WAREHOUSE, warehouse.toUri().toString());
        Catalog catalog = CatalogFactory.createCatalog(CatalogContext.create(catalogOptions));
        catalog.createDatabase(DATABASE, false);

        writeTable(catalog, "bitmap_embedded", "bitmap", "1 MB");
        writeTable(catalog, "bitmap_sidecar", "bitmap", "1 B");
        writeTable(catalog, "bloom_filter_embedded", "bloom-filter", "1 MB");
        writeTable(catalog, "bloom_filter_sidecar", "bloom-filter", "1 B");
    }

    private static void writeTable(
            Catalog catalog, String tableName, String indexType, String manifestThreshold)
            throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put("bucket", "1");
        options.put("bucket-key", "id");
        options.put("file.format", "parquet");
        options.put("file-index.read.enabled", "true");
        options.put("file-index.in-manifest-threshold", manifestThreshold);
        options.put("file-index." + indexType + ".columns", "id");
        if ("bloom-filter".equals(indexType)) {
            options.put("file-index.bloom-filter.id.items", "16");
            options.put("file-index.bloom-filter.id.fpp", "0.01");
        }

        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("payload", DataTypes.STRING())
                        .options(options)
                        .build();
        Identifier identifier = Identifier.create(DATABASE, tableName);
        catalog.createTable(identifier, schema, false);
        Table table = catalog.getTable(identifier);

        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = builder.newWrite();
                BatchTableCommit commit = builder.newCommit()) {
            write.write(row(1, "keep"));
            write.write(row(1, "drop"));
            write.write(row(null, "null-id"));
            write.write(row(3, "three"));
            commit.commit(write.prepareCommit());
        }
    }

    private static GenericRow row(Integer id, String payload) {
        return GenericRow.of(id, BinaryString.fromString(payload));
    }
}
