/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.schema.marshaller;

import java.util.Arrays;
import java.util.BitSet;
import java.util.Objects;
import java.util.Random;
import java.util.UUID;
import org.apache.ignite.internal.schema.Bitmask;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.Columns;
import org.apache.ignite.internal.schema.NativeType;
import org.apache.ignite.internal.schema.NativeTypeSpec;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.TestUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.apache.ignite.internal.schema.NativeType.BYTE;
import static org.apache.ignite.internal.schema.NativeType.BYTES;
import static org.apache.ignite.internal.schema.NativeType.DOUBLE;
import static org.apache.ignite.internal.schema.NativeType.FLOAT;
import static org.apache.ignite.internal.schema.NativeType.INTEGER;
import static org.apache.ignite.internal.schema.NativeType.LONG;
import static org.apache.ignite.internal.schema.NativeType.SHORT;
import static org.apache.ignite.internal.schema.NativeType.STRING;
import static org.apache.ignite.internal.schema.NativeType.UUID;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Serializer test.
 */
public class JavaSerializerTest {
    /** Random. */
    private Random rnd;

    /**
     *
     */
    @BeforeEach
    public void initRandom() {
        long seed = System.currentTimeMillis();

        System.out.println("Using seed: " + seed + "L;");

        rnd = new Random(seed);
    }

    /**
     * @throws SerializationException If serialization failed.
     */
    @Test
    public void testBasicTypes() throws SerializationException {
        // Fixed types:
        checkBasicType(BYTE, BYTE);
        checkBasicType(SHORT, SHORT);
        checkBasicType(INTEGER, INTEGER);
        checkBasicType(LONG, LONG);
        checkBasicType(FLOAT, FLOAT);
        checkBasicType(DOUBLE, DOUBLE);
        checkBasicType(UUID, UUID);
        checkBasicType(Bitmask.of(4), Bitmask.of(5));

        // Varlen types:
        checkBasicType(BYTES, BYTES);
        checkBasicType(STRING, STRING);

        // Mixed:
        checkBasicType(LONG, INTEGER);
        checkBasicType(INTEGER, BYTES);
        checkBasicType(STRING, LONG);
        checkBasicType(Bitmask.of(9), BYTES);
    }

    /**
     * @throws SerializationException If serialization failed.
     */
    @Test
    public void testComplexType() throws SerializationException {
        Column[] cols = new Column[] {
            new Column("pByteCol", BYTE, false),
            new Column("pShortCol", SHORT, false),
            new Column("pIntCol", INTEGER, false),
            new Column("pLongCol", LONG, false),
            new Column("pFloatCol", FLOAT, false),
            new Column("pDoubleCol", DOUBLE, false),

            new Column("byteCol", BYTE, true),
            new Column("shortCol", SHORT, true),
            new Column("intCol", INTEGER, true),
            new Column("longCol", LONG, true),
            new Column("floatCol", FLOAT, true),
            new Column("doubleCol", DOUBLE, true),

            new Column("uuidCol", UUID, true),
            new Column("bitmaskCol", Bitmask.of(42), true),
            new Column("stringCol", STRING, true),
            new Column("bytesCol", BYTES, true),
        };

        SchemaDescriptor schema = new SchemaDescriptor(1, new Columns(cols), new Columns(cols.clone()));

        final Object key = TestObject.randomObject(rnd);
        final Object val = TestObject.randomObject(rnd);

        JavaSerializer serializer = new JavaSerializer(schema, key.getClass(), val.getClass());

        byte[] bytes = serializer.serialize(key, val);

        Object key1 = serializer.deserializeKey(bytes);
        Object val1 = serializer.deserializeValue(bytes);

        assertTrue(key.getClass().isInstance(key1));
        assertTrue(val.getClass().isInstance(val1));

        assertEquals(key, key);
        assertEquals(val, val1);
    }

    /**
     *
     */
    @Test
    public void testClassWithIncorrectBitmaskSize() {
        Column[] cols = new Column[] {
            new Column("pLongCol", LONG, false),
            new Column("bitmaskCol", Bitmask.of(9), true),
        };

        SchemaDescriptor schema = new SchemaDescriptor(1, new Columns(cols), new Columns(cols.clone()));

        final Object key = TestObject.randomObject(rnd);
        final Object val = TestObject.randomObject(rnd);

        JavaSerializer serializer = new JavaSerializer(schema, key.getClass(), val.getClass());

        assertThrows(
            SerializationException.class,
            () -> serializer.serialize(key, val),
            "Failed to write field [name=bitmaskCol]"
        );
    }

    /**
     *
     */
    @Test
    public void testClassWithWrongFieldType() {
        Column[] cols = new Column[] {
            new Column("bitmaskCol", Bitmask.of(42), true),
            new Column("shortCol", UUID, true)
        };

        SchemaDescriptor schema = new SchemaDescriptor(1, new Columns(cols), new Columns(cols.clone()));

        final Object key = TestObject.randomObject(rnd);
        final Object val = TestObject.randomObject(rnd);

        JavaSerializer serializer = new JavaSerializer(schema, key.getClass(), val.getClass());

        assertThrows(
            SerializationException.class,
            () -> serializer.serialize(key, val),
            "Failed to write field [name=shortCol]"
        );
    }

    /**
     *
     */
    @Test
    public void testClassWithPrivateConstructor() throws SerializationException {
        Column[] cols = new Column[] {
            new Column("pLongCol", LONG, false),
        };

        SchemaDescriptor schema = new SchemaDescriptor(1, new Columns(cols), new Columns(cols.clone()));

        final Object key = PrivateTestObject.randomObject(rnd);
        final Object val = PrivateTestObject.randomObject(rnd);

        JavaSerializer serializer = new JavaSerializer(schema, key.getClass(), val.getClass());

        byte[] bytes = serializer.serialize(key, val);

        Object key1 = serializer.deserializeKey(bytes);
        Object val1 = serializer.deserializeValue(bytes);

        assertTrue(key.getClass().isInstance(key1));
        assertTrue(val.getClass().isInstance(val1));

        assertEquals(key, key);
        assertEquals(val, val1);
    }

    /**
     *
     */
    @Test
    public void testClassWithNoDefaultConstructor() throws SerializationException {
        Column[] cols = new Column[] {
            new Column("pLongCol", LONG, false),
        };

        SchemaDescriptor schema = new SchemaDescriptor(1, new Columns(cols), new Columns(cols.clone()));

        final Object key = WrongTestObject.randomObject(rnd);
        final Object val = WrongTestObject.randomObject(rnd);

        final JavaSerializer serializer = new JavaSerializer(schema, key.getClass(), val.getClass());

        final byte[] bytes = serializer.serialize(key, val);

        Object key1 = serializer.deserializeKey(bytes);
        Object val1 = serializer.deserializeValue(bytes);

        assertTrue(key.getClass().isInstance(key1));
        assertTrue(val.getClass().isInstance(val1));

        assertEquals(key, key);
        assertEquals(val, val1);
    }

    /**
     * Generate random key-value pair of given types and
     * check serialization and deserialization works fine.
     *
     * @param keyType Key type.
     * @param valType Value type.
     * @throws SerializationException If (de)serialization failed.
     */
    private void checkBasicType(NativeType keyType, NativeType valType) throws SerializationException {
        final Object key = generateRandomValue(keyType);
        final Object val = generateRandomValue(valType);

        Column[] keyCols = new Column[] {new Column("key", keyType, false)};
        Column[] valCols = new Column[] {new Column("val", valType, false)};

        SchemaDescriptor schema = new SchemaDescriptor(1, new Columns(keyCols), new Columns(valCols));

        JavaSerializer serializer = new JavaSerializer(schema, key.getClass(), val.getClass());

        byte[] bytes = serializer.serialize(key, val);

        Object key1 = serializer.deserializeKey(bytes);
        Object val1 = serializer.deserializeValue(bytes);

        assertTrue(key.getClass().isInstance(key1));
        assertTrue(val.getClass().isInstance(val1));

        compareObjects(keyType, key, key);
        compareObjects(valType, val, val1);
    }

    /**
     * Compare object regarding NativeType.
     *
     * @param type Native type.
     * @param exp Expected value.
     * @param act Actual value.
     */
    private void compareObjects(NativeType type, Object exp, Object act) {
        if (type.spec() == NativeTypeSpec.BYTES)
            assertArrayEquals((byte[])exp, (byte[])act);
        else
            assertEquals(exp, act);
    }

    /**
     * Generates randon value of given type.
     *
     * @param type Type.
     */
    private Object generateRandomValue(NativeType type) {
        return TestUtils.generateRandomValue(rnd, type);
    }

    /**
     * Test object.
     */
    public static class TestObject {
        /**
         * @return Random TestObject.
         */
        public static TestObject randomObject(Random rnd) {
            final TestObject obj = new TestObject();

            obj.pByteCol = (byte)rnd.nextInt(255);
            obj.pShortCol = (short)rnd.nextInt(65535);
            obj.pIntCol = rnd.nextInt();
            obj.pLongCol = rnd.nextLong();
            obj.pFloatCol = rnd.nextFloat();
            obj.pDoubleCol = rnd.nextDouble();

            obj.byteCol = (byte)rnd.nextInt(255);
            obj.shortCol = (short)rnd.nextInt(65535);
            obj.intCol = rnd.nextInt();
            obj.longCol = rnd.nextLong();
            obj.floatCol = rnd.nextFloat();
            obj.doubleCol = rnd.nextDouble();

            obj.uuidCol = new UUID(rnd.nextLong(), rnd.nextLong());
            obj.bitmaskCol = TestUtils.randomBitSet(rnd, 42);
            obj.stringCol = TestUtils.randomString(rnd, rnd.nextInt(255));
            obj.bytesCol = TestUtils.randomBytes(rnd, rnd.nextInt(255));

            return obj;
        }

        // Primitive typed
        private byte pByteCol;
        private short pShortCol;
        private int pIntCol;
        private long pLongCol;
        private float pFloatCol;
        private double pDoubleCol;

        // Reference typed
        private Byte byteCol;
        private Short shortCol;
        private Integer intCol;
        private Long longCol;
        private Float floatCol;
        private Double doubleCol;

        private UUID uuidCol;
        private BitSet bitmaskCol;
        private String stringCol;
        private byte[] bytesCol;

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            TestObject object = (TestObject)o;

            return pByteCol == object.pByteCol &&
                pShortCol == object.pShortCol &&
                pIntCol == object.pIntCol &&
                pLongCol == object.pLongCol &&
                Float.compare(object.pFloatCol, pFloatCol) == 0 &&
                Double.compare(object.pDoubleCol, pDoubleCol) == 0 &&
                Objects.equals(byteCol, object.byteCol) &&
                Objects.equals(shortCol, object.shortCol) &&
                Objects.equals(intCol, object.intCol) &&
                Objects.equals(longCol, object.longCol) &&
                Objects.equals(floatCol, object.floatCol) &&
                Objects.equals(doubleCol, object.doubleCol) &&
                Objects.equals(uuidCol, object.uuidCol) &&
                Objects.equals(bitmaskCol, object.bitmaskCol) &&
                Objects.equals(stringCol, object.stringCol) &&
                Arrays.equals(bytesCol, object.bytesCol);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return 73;
        }
    }

    /**
     * Test object with private constructor.
     */
    private static class PrivateTestObject {
        /**
         * @return Random TestObject.
         */
        static PrivateTestObject randomObject(Random rnd) {
            final PrivateTestObject obj = new PrivateTestObject();

            obj.pLongCol = rnd.nextLong();

            return obj;
        }

        /** Value. */
        private long pLongCol;

        /**
         * Private constructor.
         */
        @SuppressWarnings("RedundantNoArgConstructor")
        private PrivateTestObject() {
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            PrivateTestObject object = (PrivateTestObject)o;

            return pLongCol == object.pLongCol;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(pLongCol);
        }
    }

    /**
     * Test object without default constructor.
     */
    private static class WrongTestObject {
        /**
         * @return Random TestObject.
         */
        static WrongTestObject randomObject(Random rnd) {
            return new WrongTestObject(rnd.nextLong());
        }

        /** Value. */
        private final long pLongCol;

        /**
         * Private constructor.
         */
        private WrongTestObject(long val) {
            pLongCol = val;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            WrongTestObject object = (WrongTestObject)o;

            return pLongCol == object.pLongCol;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(pLongCol);
        }
    }
}
