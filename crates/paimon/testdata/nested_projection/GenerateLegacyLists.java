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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

// Run with parquet-hadoop/example and Hadoop common/mapreduce on the classpath.
// Generates fixtures using Parquet's Java writer, including legacy LIST layouts.
public class GenerateLegacyLists {
    public static void main(String[] args) throws Exception {
        String[] names = {"standard", "array", "tuple", "repeated"};
        String[] layouts = {
            "optional group items (LIST) { repeated group list { optional group element { optional int32 unused; optional int32 wanted; } } }",
            "optional group items (LIST) { repeated group array { optional int32 unused; optional int32 wanted; } }",
            "optional group items (LIST) { repeated group items_tuple { optional int32 unused; optional int32 wanted; } }",
            "repeated group items { optional int32 unused; optional int32 wanted; }"
        };
        for (int i = 0; i < names.length; i++) {
            MessageType schema = MessageTypeParser.parseMessageType("message schema {" + layouts[i] + "}");
            SimpleGroupFactory factory = new SimpleGroupFactory(schema);
            try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(new Path(args[0] + "/" + names[i] + ".parquet"))
                    .withConf(new Configuration()).withType(schema)
                    .withCompressionCodec(CompressionCodecName.UNCOMPRESSED).build()) {
                Group record = factory.newGroup();
                Group root = i == 3 ? record : record.addGroup("items");
                for (int value = 7; value <= 8; value++) {
                    Group element = i == 0 ? root.addGroup("list").addGroup("element")
                        : root.addGroup(i == 1 ? "array" : i == 2 ? "items_tuple" : "items");
                    element.add("unused", 100 + value);
                    element.add("wanted", value);
                }
                writer.write(record);
                Group empty = factory.newGroup();
                if (i != 3) empty.addGroup("items");
                writer.write(empty);
                writer.write(factory.newGroup());
            }
        }
    }
}
