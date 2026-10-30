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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import org.apache.paimon.KeyValue;
import org.apache.paimon.KeyValueSerializer;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.row.RowFormatWriter;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;

public final class PostponeRowJavaFixture {
    private static final class MemoryOutput extends PositionOutputStream {
        private final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        public long getPos() { return bytes.size(); }
        public void write(int value) { bytes.write(value); }
        public void write(byte[] data) throws IOException { bytes.write(data); }
        public void write(byte[] data, int offset, int length) { bytes.write(data, offset, length); }
        public void flush() {}
        public void close() {}
    }

    private static void write(String path, RowType schema, InternalRow row) throws Exception {
        MemoryOutput output = new MemoryOutput();
        RowFormatWriter writer = new RowFormatWriter(output, schema, 65536, 1);
        writer.addElement(row);
        writer.close();
        Files.write(Paths.get(path), output.bytes.toByteArray());
        System.out.println(path + " bytes=" + output.getPos() + " schema=" + schema.asSQLString());
    }

    public static void main(String[] args) throws Exception {
        RowType key = RowType.of(new DataField(SpecialFields.KEY_FIELD_ID_START, "_KEY_id", new IntType(false)));
        RowType value = RowType.of(new DataField(0, "id", new IntType(false)), new DataField(1, "value", new IntType(true)));
        KeyValueSerializer serializer = new KeyValueSerializer(key, value);
        InternalRow physical = serializer.toRow(GenericRow.of(1), -1L, RowKind.INSERT, GenericRow.of(1, null));
        write(args[0] + "/java_postpone_kv_null.row", KeyValue.schema(key, value), physical);
        RowType thin = RowType.of(SpecialFields.SEQUENCE_NUMBER, SpecialFields.VALUE_KIND, value.getFields().get(0), value.getFields().get(1));
        write(args[0] + "/java_postpone_thin_null.row", thin, GenericRow.of(-1L, (byte) 0, 1, null));
    }
}
