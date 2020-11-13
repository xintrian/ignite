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

import java.util.Random;
import org.apache.ignite.internal.schema.Bitmask;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.Columns;
import org.apache.ignite.internal.schema.NativeType;
import org.apache.ignite.internal.schema.NativeTypeSpec;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.SchemaTestUtils;
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

        System.out.println("Using seed: " + seed + "L; //");

        rnd = new Random(seed);
    }

    /**
     * Generates randon value of given type.
     *
     * @param type Type.
     */
    private Object generateRandomValue(NativeType type) {
        return SchemaTestUtils.generateRandomValue(rnd, type);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testBasicTypes() throws Exception {
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
     * @throws Exception If failed.
     */
    private void checkBasicType(NativeType keyType, NativeType valType) throws Exception {
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
}
