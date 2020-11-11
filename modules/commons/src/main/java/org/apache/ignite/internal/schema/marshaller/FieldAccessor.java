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

import java.lang.reflect.Field;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.Tuple;
import org.apache.ignite.internal.schema.TupleAssembler;
import org.jetbrains.annotations.Nullable;

/**
 * Field accessor to speedup access.
 */
//TODO: Rewrite to direct field access via Unsafe to bypass security checks.
//TODO: Extract interface, move to java-8 profile and add implementation based on VarHandle for java9+.
public abstract class FieldAccessor {
    /**
     * TODO: implement sesitive information filtering.
     *
     * @return {@code False} if sensitive information exoising is prohibited, {@code false} otherwise.
     */
    private static boolean includeSensitive() {
        return true;
    }

    /** Field name */
    protected final Field field;

    /** Mode. */
    protected final BinaryMode mode;

    /** Column index. */
    protected final int colIdx;

    /**
     * Create accessor for the field.
     *
     * @param field Field.
     * @param col Mapped column.
     * @param colIdx Column index.
     * @return Accessor.
     */
    //TODO: Extract a provider for this factory-method.
    public static FieldAccessor create(Field field, Column col, int colIdx) {
        if (field.getType().isPrimitive() && col.nullable()) //TODO: convert to assert?
            throw new IllegalArgumentException("Failed to map non-nullable field to nullable column [name=" + field.getName() + ']');

        BinaryMode mode = JavaSerializer.mode(field.getType());

        switch (mode) {
            case P_BYTE:
                return new BytePrimitiveAccessor(field, colIdx);

            case P_SHORT:
                return new ShortPrimitiveAccessor(field, colIdx);

            case P_INT:
                return new IntPrimitiveAccessor(field, colIdx);

            case P_LONG:
                return new LongPrimitiveAccessor(field, colIdx);

            case P_FLOAT:
                return new FloatPrimitiveAccessor(field, colIdx);

            case P_DOUBLE:
                return new DoublePrimitiveAccessor(field, colIdx);

            case BYTE:
            case SHORT:
            case INT:
            case LONG:
            case FLOAT:
            case DOUBLE:
            case STRING:
            case UUID:
            case BYTE_ARR:
            case BITSET:
                return new ReferenceFieldAccessor(field, colIdx, mode);

            default:
                assert false : "Invalid mode " + mode;
        }

        throw new IllegalArgumentException("Failed to create accessor for field [name=" + field.getName() + ']');
    }

    /**
     * Protected constructor.
     *
     * @param field Field.
     * @param colIdx Column index.
     * @param mode Binary mode;
     */
    protected FieldAccessor(Field field, int colIdx, BinaryMode mode) {
        assert field != null;
        assert colIdx >= 0;
        assert mode != null;

        this.field = field;
        this.colIdx = colIdx;
        this.mode = mode;

        field.setAccessible(true);
    }

    /**
     * Get binary read/write mode.
     *
     * @return Binary mode.
     */
    public BinaryMode mode() {
        return mode;
    }

    /**
     * Write object field value to tuple.
     *
     * @param obj Source object.
     * @param writer Tuple writer.
     * @throws SerializationException If failed.
     */
    public void write(Object obj, TupleAssembler writer) throws SerializationException {
        try {
            write0(obj, writer);
        }
        catch (Exception ex) {
            if (includeSensitive())
                throw new SerializationException("Failed to write field [name=" + field.getName() + ']', ex);
            else
                throw new SerializationException("Failed to write field [id=" + colIdx + ']', ex);
        }
    }

    /**
     * Write object field value to tuple.
     *
     * @param obj Source object.
     * @param writer Tuple writer.
     * @throws IllegalAccessException If failed.
     */
    protected abstract void write0(Object obj, TupleAssembler writer) throws IllegalAccessException;

    /**
     * Reads value fom tuple to object field.
     *
     * @param obj Target object.
     * @param reader Tuple reader.
     * @throws SerializationException If failed.
     */
    public void read(Object obj, Tuple reader) throws SerializationException {
        try {
            read0(obj, reader);
        }
        catch (Exception ex) {
            if (includeSensitive())
                throw new SerializationException("Failed to read field [name=" + field.getName() + ']', ex);
            else
                throw new SerializationException("Failed to read field [id=" + colIdx + ']', ex);
        }
    }

    /**
     * Reads value fom tuple to object field.
     *
     * @param obj Target object.
     * @param reader Tuple reader.
     * @throws IllegalAccessException If failed.
     */
    protected abstract void read0(Object obj, Tuple reader) throws IllegalAccessException;

    /**
     * Reads object field value.
     *
     * @param obj Object.
     * @return Field value of given object.
     * @throws SerializationException If failed.
     */
    @Nullable Object value(Object obj) throws SerializationException {
        try {
            return field.get(obj);
        }
        catch (IllegalAccessException ex) {
            if (includeSensitive())
                throw new SerializationException("Failed to read field [name=" + field.getName() + ']', ex);
            else
                throw new SerializationException("Failed to read field [id=" + colIdx + ']', ex);
        }
    }

    /**
     * Accessor for field of primitive {@code byte} type.
     */
    private static class BytePrimitiveAccessor extends FieldAccessor {
        /**
         * Constructor.
         *
         * @param field Field.
         * @param colIdx Column index.
         */
        public BytePrimitiveAccessor(Field field, int colIdx) {
            super(field, colIdx, BinaryMode.P_BYTE);
        }

        /** {@inheritDoc} */
        @Override protected void write0(Object obj, TupleAssembler writer) throws IllegalAccessException {
            writer.appendByte(field.getByte(obj));
        }

        /** {@inheritDoc} */
        @Override protected void read0(Object obj, Tuple reader) throws IllegalAccessException {
            field.setByte(obj, reader.byteValue(colIdx));
        }
    }

    /**
     * Accessor for field of primitive {@code short} type.
     */
    private static class ShortPrimitiveAccessor extends FieldAccessor {
        /**
         * Constructor.
         *
         * @param field Field.
         * @param colIdx Column index.
         */
        public ShortPrimitiveAccessor(Field field, int colIdx) {
            super(field, colIdx, BinaryMode.P_SHORT);
        }

        /** {@inheritDoc} */
        @Override protected void write0(Object obj, TupleAssembler writer) throws IllegalAccessException {
            writer.appendShort(field.getShort(obj));
        }

        /** {@inheritDoc} */
        @Override protected void read0(Object obj, Tuple reader) throws IllegalAccessException {
            field.setShort(obj, reader.shortValue(colIdx));
        }
    }

    /**
     * Accessor for field of primitive {@code int} type.
     */
    private static class IntPrimitiveAccessor extends FieldAccessor {
        /**
         * Constructor.
         *
         * @param field Field.
         * @param colIdx Column index.
         */
        public IntPrimitiveAccessor(Field field, int colIdx) {
            super(field, colIdx, BinaryMode.P_INT);
        }

        /** {@inheritDoc} */
        @Override protected void write0(Object obj, TupleAssembler writer) throws IllegalAccessException {
            writer.appendInt(field.getInt(obj));
        }

        /** {@inheritDoc} */
        @Override protected void read0(Object obj, Tuple reader) throws IllegalAccessException {
            field.setInt(obj, reader.intValue(colIdx));
        }
    }

    /**
     * Accessor for field of primitive {@code long} type.
     */
    private static class LongPrimitiveAccessor extends FieldAccessor {
        /**
         * Constructor.
         *
         * @param field Field.
         * @param colIdx Column index.
         */
        public LongPrimitiveAccessor(Field field, int colIdx) {
            super(field, colIdx, BinaryMode.P_LONG);
        }

        /** {@inheritDoc} */
        @Override protected void write0(Object obj, TupleAssembler writer) throws IllegalAccessException {
            writer.appendLong(field.getLong(obj));
        }

        /** {@inheritDoc} */
        @Override protected void read0(Object obj, Tuple reader) throws IllegalAccessException {
            field.setLong(obj, reader.longValue(colIdx));
        }
    }

    /**
     * Accessor for field of primitive {@code float} type.
     */
    private static class FloatPrimitiveAccessor extends FieldAccessor {
        /**
         * Constructor.
         *
         * @param field Field.
         * @param colIdx Column index.
         */
        public FloatPrimitiveAccessor(Field field, int colIdx) {
            super(field, colIdx, BinaryMode.P_FLOAT);
        }

        /** {@inheritDoc} */
        @Override protected void write0(Object obj, TupleAssembler writer) throws IllegalAccessException {
            writer.appendFloat(field.getFloat(obj));
        }

        /** {@inheritDoc} */
        @Override protected void read0(Object obj, Tuple reader) throws IllegalAccessException {
            field.setFloat(obj, reader.floatValue(colIdx));
        }
    }

    /**
     * Accessor for field of primitive {@code double} type.
     */
    private static class DoublePrimitiveAccessor extends FieldAccessor {
        /**
         * Constructor.
         *
         * @param field Field.
         * @param colIdx Column index.
         */
        public DoublePrimitiveAccessor(Field field, int colIdx) {
            super(field, colIdx, BinaryMode.P_DOUBLE);
        }

        /** {@inheritDoc} */
        @Override protected void write0(Object obj, TupleAssembler writer) throws IllegalAccessException {
            writer.appendDouble(field.getDouble(obj));
        }

        /** {@inheritDoc} */
        @Override protected void read0(Object obj, Tuple reader) throws IllegalAccessException {
            field.setDouble(obj, reader.doubleValue(colIdx));
        }
    }

    /**
     * Accessor for field of reference type.
     */
    private static class ReferenceFieldAccessor extends FieldAccessor {
        /**
         * Constructor.
         *
         * @param field Field.
         * @param colIdx Column index.
         * @param mode Binary mode.
         */
        ReferenceFieldAccessor(Field field, int colIdx, BinaryMode mode) {
            super(field, colIdx, mode);
        }

        /** {@inheritDoc} */
        @Override protected void write0(Object obj, TupleAssembler writer) throws IllegalAccessException {
            assert obj != null;
            assert writer != null;

            Object val;

            val = field.get(obj);

            if (val == null) {
                writer.appendNull();

                return;
            }

            JavaSerializer.writeRefObject(val, writer, mode);
        }

        /** {@inheritDoc} */
        @Override public void read0(Object obj, Tuple reader) throws IllegalAccessException {
            Object val = JavaSerializer.readRefValue(reader, colIdx, mode);

            assert !field.getType().isPrimitive();

            field.set(obj, val);
        }
    }
}
