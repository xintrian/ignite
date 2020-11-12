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

package org.apache.ignite.internal.schema;

import java.util.BitSet;
import java.util.Random;

/**
 *  Test utility class.
 */
public class SchemaTestUtils {
    /**
     * Generates randon value of given type.
     *
     * @param rnd Random generator.
     * @param type Type.
     */
    public static Object generateRandomValue(Random rnd, NativeType type) {
        switch (type.spec()) {
            case BYTE:
                return (byte)rnd.nextInt(255);

            case SHORT:
                return (short)rnd.nextInt(65535);

            case INTEGER:
                return rnd.nextInt();

            case LONG:
                return rnd.nextLong();

            case FLOAT:
                return rnd.nextFloat();

            case DOUBLE:
                return rnd.nextDouble();

            case UUID:
                return new java.util.UUID(rnd.nextLong(), rnd.nextLong());

            case STRING: {
                int size = rnd.nextInt(255);

                StringBuilder sb = new StringBuilder();

                while (sb.length() < size) {
                    char pt = (char)rnd.nextInt(Character.MAX_VALUE + 1);

                    if (Character.isDefined(pt) &&
                        Character.getType(pt) != Character.PRIVATE_USE &&
                        !Character.isSurrogate(pt))
                        sb.append(pt);
                }

                return sb.toString();
            }

            case BYTES: {
                int size = rnd.nextInt(255);
                byte[] data = new byte[size];
                rnd.nextBytes(data);

                return data;
            }

            case BITMASK: {
                Bitmask maskType = (Bitmask)type;

                BitSet set = new BitSet();

                for (int i = 0; i < maskType.bits(); i++) {
                    if (rnd.nextBoolean())
                        set.set(i);
                }

                return set;
            }

            default:
                throw new IllegalStateException("Unsupported type: " + type);
        }
    }
}
