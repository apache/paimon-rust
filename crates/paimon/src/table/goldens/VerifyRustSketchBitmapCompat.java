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

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.TreeSet;

import org.apache.datasketches.hll.HllSketch;
import org.apache.datasketches.hll.TgtHllType;
import org.apache.datasketches.hll.Union;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.theta.Sketches;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.longlong.Roaring64Bitmap;

/**
 * JVM-side verifier for Rust-written sketch/bitmap aggregator payloads.
 *
 * Uses the same production libraries and call patterns as Paimon utils:
 * {@code HllSketchUtil}, {@code ThetaSketch}, {@code RoaringBitmap32},
 * {@code RoaringBitmap64} (DataSketches 4.2.0 / RoaringBitmap 1.2.1).
 *
 * Classpath: resolve via {@code pom.xml} in this directory
 * ({@code mvn dependency:build-classpath}), same versions as paimon-common.
 *
 * Args: &lt;goldens-dir&gt; [&lt;rust-written-temp-dir&gt;]
 */
public class VerifyRustSketchBitmapCompat {

    private static int failures = 0;

    public static void main(String[] args) throws Exception {
        Path goldens = Path.of(args[0]);
        Path rustOut = Path.of(args.length > 1 ? args[1] : goldens.resolve("rust_written").toString());

        verifyRbm32(goldens, rustOut);
        verifyRbm64(goldens, rustOut);
        verifyHll(goldens, rustOut);
        verifyTheta(goldens, rustOut);

        if (failures != 0) {
            System.err.println("FAILED checks=" + failures);
            System.exit(1);
        }
        System.out.println("OK all JVM compatibility checks passed");
    }

    /** Mirrors FieldRoaringBitmap32Agg / RoaringBitmap32.deserialize + or. */
    private static void verifyRbm32(Path goldens, Path rustOut) throws Exception {
        RoaringBitmap javaA = readRbm32(goldens.resolve("compat_rbm32_a.bin"));
        RoaringBitmap javaB = readRbm32(goldens.resolve("compat_rbm32_b.bin"));
        RoaringBitmap javaUnion = readRbm32(goldens.resolve("compat_rbm32_union.bin"));

        RoaringBitmap merged = javaA.clone();
        merged.or(javaB);
        check("rbm32 java∪java set", values32(merged).equals(values32(javaUnion)));

        // Rust-written operands must deserialize with Java RoaringBitmap.
        RoaringBitmap rustA = readRbm32(rustOut.resolve("rust_rbm32_a.bin"));
        RoaringBitmap rustB = readRbm32(rustOut.resolve("rust_rbm32_b.bin"));
        RoaringBitmap rustUnion = readRbm32(rustOut.resolve("rust_rbm32_union.bin"));
        check("rbm32 rust_a set", values32(rustA).equals(values32(javaA)));
        check("rbm32 rust_b set", values32(rustB).equals(values32(javaB)));
        check("rbm32 rust_union set", values32(rustUnion).equals(values32(javaUnion)));

        RoaringBitmap cross = rustA.clone();
        cross.or(javaB);
        check("rbm32 rust∪java set", values32(cross).equals(values32(javaUnion)));

        RoaringBitmap cross2 = javaA.clone();
        cross2.or(rustB);
        check("rbm32 java∪rust set", values32(cross2).equals(values32(javaUnion)));

        RoaringBitmap empty = readRbm32(goldens.resolve("compat_rbm32_empty.bin"));
        RoaringBitmap dups = readRbm32(goldens.resolve("compat_rbm32_dups.bin"));
        RoaringBitmap bounds = readRbm32(goldens.resolve("compat_rbm32_bounds.bin"));
        check("rbm32 empty", empty.getCardinality() == 0);
        check("rbm32 dups", values32(dups).equals(set32(7, 8)));
        check(
                "rbm32 bounds",
                values32(bounds)
                        .equals(set32(0, Integer.MAX_VALUE, 0xFFFFFFFF)));
    }

    /** Mirrors FieldRoaringBitmap64Agg / RoaringBitmap64.deserialize + or. */
    private static void verifyRbm64(Path goldens, Path rustOut) throws Exception {
        Roaring64Bitmap javaA = readRbm64(goldens.resolve("compat_rbm64_a.bin"));
        Roaring64Bitmap javaB = readRbm64(goldens.resolve("compat_rbm64_b.bin"));
        Roaring64Bitmap javaUnion = readRbm64(goldens.resolve("compat_rbm64_union.bin"));

        Roaring64Bitmap merged = javaA.clone();
        merged.or(javaB);
        check("rbm64 java∪java card", merged.getLongCardinality() == javaUnion.getLongCardinality());
        check("rbm64 java∪java set", values64(merged).equals(values64(javaUnion)));

        Roaring64Bitmap rustA = readRbm64(rustOut.resolve("rust_rbm64_a.bin"));
        Roaring64Bitmap rustB = readRbm64(rustOut.resolve("rust_rbm64_b.bin"));
        Roaring64Bitmap rustUnion = readRbm64(rustOut.resolve("rust_rbm64_union.bin"));
        check("rbm64 rust_a set", values64(rustA).equals(values64(javaA)));
        check("rbm64 rust_b set", values64(rustB).equals(values64(javaB)));
        check("rbm64 rust_union set", values64(rustUnion).equals(values64(javaUnion)));

        Roaring64Bitmap cross = rustA.clone();
        cross.or(javaB);
        check("rbm64 rust∪java set", values64(cross).equals(values64(javaUnion)));

        Roaring64Bitmap cross2 = javaA.clone();
        cross2.or(rustB);
        check("rbm64 java∪rust set", values64(cross2).equals(values64(javaUnion)));

        Roaring64Bitmap empty = readRbm64(goldens.resolve("compat_rbm64_empty.bin"));
        check("rbm64 empty", empty.getLongCardinality() == 0);
    }

    /** Mirrors HllSketchUtil.union (heapify + Union + compact HLL_4). */
    private static void verifyHll(Path goldens, Path rustOut) throws Exception {
        byte[] javaA = Files.readAllBytes(goldens.resolve("compat_hll_a.bin"));
        byte[] javaB = Files.readAllBytes(goldens.resolve("compat_hll_b.bin"));
        byte[] javaUnion = Files.readAllBytes(goldens.resolve("compat_hll_union.bin"));
        double expected = HllSketch.heapify(javaUnion).getEstimate();

        double javaMerged = hllUnionEstimate(javaA, javaB);
        check("hll java∪java estimate", Math.abs(javaMerged - expected) < 1e-9);

        byte[] rustUnion = Files.readAllBytes(rustOut.resolve("rust_hll_union.bin"));
        // Production Java deserialize of Rust-written compact HLL.
        double rustAsJava = HllSketch.heapify(rustUnion).getEstimate();
        check("hll rust-bytes via Java heapify", Math.abs(rustAsJava - expected) < 1e-6);

        double rustThenJava = hllUnionEstimate(rustUnion, javaA);
        check("hll rust∪java estimate stable", Math.abs(rustThenJava - expected) < 1.0);

        byte[] empty = Files.readAllBytes(goldens.resolve("compat_hll_empty.bin"));
        check("hll empty readable", HllSketch.heapify(empty).getEstimate() >= 0.0);
    }

    /** Mirrors ThetaSketch.union (Sketches.setOperationBuilder + Memory.wrap). */
    private static void verifyTheta(Path goldens, Path rustOut) throws Exception {
        byte[] javaA = Files.readAllBytes(goldens.resolve("compat_theta_a.bin"));
        byte[] javaB = Files.readAllBytes(goldens.resolve("compat_theta_b.bin"));
        byte[] javaUnion = Files.readAllBytes(goldens.resolve("compat_theta_union.bin"));
        double expected = Sketches.wrapSketch(Memory.wrap(javaUnion)).getEstimate();

        double javaMerged = thetaUnionEstimate(javaA, javaB);
        check("theta java∪java estimate", Math.abs(javaMerged - expected) < 1e-9);

        byte[] rustUnion = Files.readAllBytes(rustOut.resolve("rust_theta_union.bin"));
        double rustAsJava = Sketches.wrapSketch(Memory.wrap(rustUnion)).getEstimate();
        check("theta rust-bytes via Java wrapSketch", Math.abs(rustAsJava - expected) < 1e-9);

        double rustThenJava = thetaUnionEstimate(rustUnion, javaA);
        check("theta rust∪java estimate", Math.abs(rustThenJava - expected) < 1e-9);

        byte[] empty = Files.readAllBytes(goldens.resolve("compat_theta_empty.bin"));
        check(
                "theta empty readable",
                Sketches.wrapSketch(Memory.wrap(empty)).isEmpty()
                        || Sketches.wrapSketch(Memory.wrap(empty)).getEstimate() == 0.0);
    }

    private static double hllUnionEstimate(byte[] left, byte[] right) {
        // Same sequence as org.apache.paimon.utils.HllSketchUtil.union.
        HllSketch heapify = HllSketch.heapify(left);
        Union union = Union.heapify(right);
        union.update(heapify);
        return union.getResult(TgtHllType.HLL_4).getEstimate();
    }

    private static double thetaUnionEstimate(byte[] left, byte[] right) {
        // Same sequence as org.apache.paimon.utils.ThetaSketch.union.
        org.apache.datasketches.theta.Union union =
                Sketches.setOperationBuilder().buildUnion();
        union.union(Memory.wrap(left));
        union.union(Memory.wrap(right));
        return union.getResult().getEstimate();
    }

    private static RoaringBitmap readRbm32(Path path) throws Exception {
        RoaringBitmap bitmap = new RoaringBitmap();
        bitmap.deserialize(ByteBuffer.wrap(Files.readAllBytes(path)));
        return bitmap;
    }

    private static Roaring64Bitmap readRbm64(Path path) throws Exception {
        Roaring64Bitmap bitmap = new Roaring64Bitmap();
        bitmap.deserialize(
                new DataInputStream(new ByteArrayInputStream(Files.readAllBytes(path))));
        return bitmap;
    }

    private static TreeSet<Integer> values32(RoaringBitmap bitmap) {
        TreeSet<Integer> values = new TreeSet<>();
        Iterator<Integer> it = bitmap.iterator();
        while (it.hasNext()) {
            values.add(it.next());
        }
        return values;
    }

    private static TreeSet<Long> values64(Roaring64Bitmap bitmap) {
        TreeSet<Long> values = new TreeSet<>();
        Iterator<Long> it = bitmap.iterator();
        while (it.hasNext()) {
            values.add(it.next());
        }
        return values;
    }

    private static TreeSet<Integer> set32(int... values) {
        TreeSet<Integer> set = new TreeSet<>();
        for (int value : values) {
            set.add(value);
        }
        return set;
    }

    private static void check(String name, boolean ok) {
        if (ok) {
            System.out.println("PASS " + name);
        } else {
            System.err.println("FAIL " + name);
            failures++;
        }
    }
}
