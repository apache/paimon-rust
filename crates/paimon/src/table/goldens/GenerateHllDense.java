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

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.datasketches.hll.HllSketch;
import org.apache.datasketches.hll.TgtHllType;

/** Generate compact, dense Java DataSketches 4.2.0 HLL fixtures for Rust union tests. */
public class GenerateHllDense {

    public static void main(String[] args) throws Exception {
        Path output = Paths.get(args[0]);
        for (TgtHllType type : TgtHllType.values()) {
            HllSketch a = new HllSketch(12, type);
            HllSketch b = new HllSketch(12, type);
            for (int value = 0; value < 10000; value++) {
                a.update(value);
            }
            for (int value = 5000; value < 15000; value++) {
                b.update(value);
            }
            String suffix = type == TgtHllType.HLL_4 ? "" : type == TgtHllType.HLL_6 ? "6" : "8";
            Files.write(output.resolve("hll_java_dense" + suffix + "_a.bin"), a.toCompactByteArray());
            Files.write(output.resolve("hll_java_dense" + suffix + "_b.bin"), b.toCompactByteArray());
        }
    }
}
