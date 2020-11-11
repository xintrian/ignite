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

import java.util.BitSet;
import java.util.UUID;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.Tuple;
import org.apache.ignite.internal.schema.TupleAssembler;
import org.jetbrains.annotations.NotNull;

/**
 * Cache objects (de)serializer.
 * <p>
 * TODO: Extract interface.
 */
public class JavaSerializer {

    /**
     * Gets binary read/write mode for given class.
     *
     * @param cls Type.
     * @return Binary mode.
     */
    public static BinaryMode mode(Class<?> cls) {
        assert cls != null;

        // Primitives.
        if (cls == byte.class)
            return BinaryMode.P_BYTE;
        else if (cls == short.class)
            return BinaryMode.P_SHORT;
        else if (cls == int.class)
            return BinaryMode.P_INT;
        else if (cls == long.class)
            return BinaryMode.P_LONG;
        else if (cls == float.class)
            return BinaryMode.P_FLOAT;
        else if (cls == double.class)
            return BinaryMode.P_DOUBLE;

            // Boxed primitives.
        else if (cls == Byte.class)
            return BinaryMode.BYTE;
        else if (cls == Short.class)
            return BinaryMode.SHORT;
        else if (cls == Integer.class)
            return BinaryMode.INT;
        else if (cls == Long.class)
            return BinaryMode.LONG;
        else if (cls == Float.class)
            return BinaryMode.FLOAT;
        else if (cls == Double.class)
            return BinaryMode.DOUBLE;

            // Other types
        else if (cls == byte[].class)
            return BinaryMode.BYTE_ARR;
        else if (cls == String.class)
            return BinaryMode.STRING;
        else if (cls == UUID.class)
            return BinaryMode.UUID;
        else if (cls == BitSet.class)
            return BinaryMode.BITSET;

        return null;
    }

    /**
     * Reads value object from tuple.
     *
     * @param reader Reader.
     * @param colIdx Column index.
     * @param mode Binary read mode.
     * @return Read value object.
     */
    static Object readRefValue(Tuple reader, int colIdx, BinaryMode mode) {
        assert reader != null;
        assert colIdx >= 0;

        Object val = null;

        switch (mode) {
            case BYTE:
                val = reader.byteValueBoxed(colIdx);

                break;

            case SHORT:
                val = reader.shortValueBoxed(colIdx);

                break;

            case INT:
                val = reader.intValueBoxed(colIdx);

                break;

            case LONG:
                val = reader.longValueBoxed(colIdx);

                break;

            case FLOAT:
                val = reader.floatValueBoxed(colIdx);

                break;

            case DOUBLE:
                val = reader.doubleValueBoxed(colIdx);

                break;

            case STRING:
                val = reader.stringValue(colIdx);

                break;

            case UUID:
                val = reader.uuidValue(colIdx);

                break;

            case BYTE_ARR:
                val = reader.bytesValue(colIdx);

                break;

            case BITSET:
                val = reader.bitmaskValue(colIdx);

                break;

            default:
                assert false : "Invalid mode: " + mode;
        }

        return val;
    }

    /**
     * Writes reference value to tuple.
     *
     * @param val Value object.
     * @param writer Writer.
     * @param mode Write binary mode.
     */
    static void writeRefObject(Object val, TupleAssembler writer, BinaryMode mode) {
        assert writer != null;

        if (val == null) {
            writer.appendNull();

            return;
        }

        switch (mode) {
            case BYTE:
                writer.appendByte((Byte)val);

                break;

            case SHORT:
                writer.appendShort((Short)val);

                break;

            case INT:
                writer.appendInt((Integer)val);

                break;

            case LONG:
                writer.appendLong((Long)val);

                break;

            case FLOAT:
                writer.appendFloat((Float)val);

                break;

            case DOUBLE:
                writer.appendDouble((Double)val);

                break;

            case STRING:
                writer.appendString((String)val);

                break;

            case UUID:
                writer.appendUuid((UUID)val);

                break;

            case BYTE_ARR:
                writer.appendBytes((byte[])val);

                break;

            case BITSET:
                writer.appendBitmask((BitSet)val);

                break;

            default:
                assert false : "Invalid mode: " + mode;
        }
    }

    /** Schema. */
    private final SchemaDescriptor schema;

    private final Class<?> keyClass;
    private final Class<?> valClass;

    /** Key marshaller. */
    private final Marshaller keyMarsh;

    /** Value marshaller. */
    private final Marshaller valMarsh;

    /**
     * Constructor.
     *
     * @param schema Schema.
     * @param keyClass Key type.
     * @param valClass Value type.
     */
    public JavaSerializer(SchemaDescriptor schema, Class<?> keyClass, Class<?> valClass) {
        this.schema = schema;
        this.keyClass = keyClass;
        this.valClass = valClass;

        keyMarsh = createMarshaller(schema, keyClass);
        valMarsh = createMarshaller(schema, valClass);
    }

    /**
     * Creates marshaller for class.
     *
     * @param schema Schema.
     * @param aClass Type.
     * @return Marshaller.
     */
    @NotNull private <T> Marshaller createMarshaller(SchemaDescriptor schema, Class<T> aClass) {
        final BinaryMode mode = mode(aClass);

        if (mode != null) {
            final Column col = schema.keyColumns().column(0);

            assert mode.typeSpec() == col.type().spec() : "Target type is not compatible.";
            assert aClass.isPrimitive() && col.nullable() : "Non-nullable types are not allowed.";

            return new BaseTypeMarshaller(mode);
        }

        // TODO: Build accessors

        return new ClassMarshaller<>(schema.keyColumns(), ObjectFactory.classFactory(aClass), null);
    }

    /**
     * Writes key-value pair to tuple.
     *
     * @param key Key object.
     * @param val Value object.
     * @return Serialized key-value pair.
     */
    public byte[] serialize(Object key, Object val) throws SerializationException {
        assert keyClass.isInstance(key);
        assert val == null || valClass.isInstance(val);

        int size = 0;

        int nonNullVarLenKeyCols = keyMarsh.nonNullVarLenCols(key);
        int nonNullVarLenValCols = valMarsh.nonNullVarLenCols(val);

        final TupleAssembler asm = new TupleAssembler(schema, size, nonNullVarLenKeyCols, nonNullVarLenValCols);

        keyMarsh.writeObject(key, asm);
        valMarsh.writeObject(val, asm); // TODO: support tomstones.

        return asm.build();
    }
}
