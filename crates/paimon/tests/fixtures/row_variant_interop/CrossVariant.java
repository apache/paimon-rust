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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.StringJoiner;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.DataGetters;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericMap;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.variant.GenericVariant;
import org.apache.paimon.data.variant.Variant;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.FormatReaderContext;
import org.apache.paimon.format.FormatReaderFactory;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.FileRecordReader;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.TinyIntType;
import org.apache.paimon.types.VarCharType;
import org.apache.paimon.types.VariantType;

public class CrossVariant {
    static RowType type() {
        return new RowType(
                Arrays.asList(
                        new DataField(SpecialFields.KEY_FIELD_ID_START, "_KEY_id", new IntType()),
                        new DataField(2147483646, "_SEQUENCE_NUMBER", new BigIntType()),
                        new DataField(2147483645, "_VALUE_KIND", new TinyIntType()),
                        new DataField(0, "id", new IntType()),
                        new DataField(1, "payload", new VariantType()),
                        new DataField(
                                2,
                                "nested",
                                new RowType(
                                        Arrays.asList(
                                                new DataField(10, "payload", new VariantType()),
                                                new DataField(11, "note", new VarCharType(100))))),
                        new DataField(3, "items", new ArrayType(new VariantType())),
                        new DataField(
                                4, "attrs", new MapType(new VarCharType(100), new VariantType())),
                        new DataField(5, "extra", new VariantType())));
    }

    static String[] jsons = {
        "{\"x\":1,\"n\":{\"items\":[true,null,\"unicode\\u4e2d\"]}}",
        "[1,{\"x\":\"value\"},null]",
        "null",
        null,
        "\"scalar\"",
        "{}",
        "[]",
        "true",
        "1234567890123456789"
    };

    static String hex(byte[] b) {
        StringBuilder s = new StringBuilder();
        for (byte v : b) s.append(String.format("%02x", v & 255));
        return s.toString();
    }

    static String hex(java.nio.ByteBuffer buffer) {
        java.nio.ByteBuffer c = buffer.duplicate();
        byte[] b = new byte[c.remaining()];
        c.get(b);
        return hex(b);
    }

    static String variant(Variant v) {
        return hex(v.valueBuffer()) + ":" + hex(v.metadataBuffer());
    }

    static void dump(InternalRow row, RowType t) {
        StringBuilder s = new StringBuilder();
        for (int i = 0; i < t.getFieldCount(); i++) {
            if (i > 0) s.append('|');
            s.append(field(row, i, t.getTypeAt(i)));
        }
        System.out.println(s);
    }

    static String field(DataGetters d, int i, DataType t) {
        if (d.isNullAt(i)) return "SQLNULL";
        switch (t.getTypeRoot()) {
            case INTEGER:
                return "I" + d.getInt(i);
            case BIGINT:
                return "L" + d.getLong(i);
            case TINYINT:
                return "B" + d.getByte(i);
            case VARCHAR:
                return "S" + d.getString(i);
            case VARIANT:
                return variant(d.getVariant(i));
            case ROW:
                {
                    RowType r = (RowType) t;
                    InternalRow a = d.getRow(i, r.getFieldCount());
                    StringJoiner s = new StringJoiner(",", "R[", "]");
                    for (int j = 0; j < r.getFieldCount(); j++) s.add(field(a, j, r.getTypeAt(j)));
                    return s.toString();
                }
            case ARRAY:
                {
                    InternalArray a = d.getArray(i);
                    DataType e = ((ArrayType) t).getElementType();
                    StringJoiner s = new StringJoiner(",", "A[", "]");
                    for (int j = 0; j < a.size(); j++) s.add(field(a, j, e));
                    return s.toString();
                }
            case MAP:
                {
                    InternalMap a = d.getMap(i);
                    MapType m = (MapType) t;
                    StringJoiner s = new StringJoiner(",", "M[", "]");
                    for (int j = 0; j < a.size(); j++)
                        s.add(
                                field(a.keyArray(), j, m.getKeyType())
                                        + "="
                                        + field(a.valueArray(), j, m.getValueType()));
                    return s.toString();
                }
            default:
                throw new RuntimeException(t.toString());
        }
    }

    public static void main(String[] args) throws Exception {
        RowType t = type();
        Path path = new Path(args[1]);
        LocalFileIO io = new LocalFileIO();
        FileFormat f = FileFormat.fromIdentifier("row", new Options());
        if (args[0].equals("write")) {
            PositionOutputStream out = io.newOutputStream(path, false);
            FormatWriter w = f.createWriterFactory(t).create(out, "zstd");
            for (int i = 0; i < jsons.length; i++) {
                Variant v = jsons[i] == null ? null : GenericVariant.fromJson(jsons[i]);
                Variant n = GenericVariant.fromJson("null");
                Map<BinaryString, Variant> m = new LinkedHashMap<>();
                m.put(BinaryString.fromString("a"), v);
                m.put(BinaryString.fromString("b"), null);
                m.put(BinaryString.fromString("c"), n);
                InternalRow row =
                        GenericRow.of(
                                i,
                                (long) i,
                                (byte) 0,
                                i,
                                v,
                                i == 3 ? null : GenericRow.of(v, BinaryString.fromString("ok")),
                                i == 5 ? null : new GenericArray(new Object[] {v, null, n}),
                                i == 6 ? null : new GenericMap(m),
                                v);
                w.addElement(row);
                dump(row, t);
            }
            w.close();
            out.close();
        } else {
            FormatReaderFactory factory = f.createReaderFactory(t, t, new ArrayList<>());
            FileRecordReader<InternalRow> reader =
                    factory.createReader(
                            new FormatReaderContext(io, path, io.getFileSize(path), null, null));
            reader.forEachRemaining(row -> dump(row, t));
            reader.close();
        }
    }
}
