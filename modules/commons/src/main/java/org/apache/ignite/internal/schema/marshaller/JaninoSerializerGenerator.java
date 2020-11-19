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

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import org.apache.ignite.internal.schema.Columns;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.util.IgniteUnsafeUtils;
import org.codehaus.commons.compiler.CompilerFactoryFactory;
import org.codehaus.commons.compiler.IClassBodyEvaluator;

/**
 *
 */
public class JaninoSerializerGenerator implements SerializerFactory {
    /** Tabulate. */
    static final String TAB = "    ";

    /** Line feed. */
    static final char LF = '\n';

    /** Debug flag. */
    private static final boolean enabledDebug = false;

    @Override public Serializer create(
        SchemaDescriptor schema,
        Class<?> keyClass,
        Class<?> valClass
    ) {
        try {
            final String packageName = "org.apache.ignite.internal.schema.marshaller.";
            final String className = "JaninoSerializerForSchema_" + schema.version();

            final IClassBodyEvaluator ce = CompilerFactoryFactory.getDefaultCompilerFactory().newClassBodyEvaluator();

            ce.setClassName(packageName + className);
            ce.setImplementedInterfaces(new Class[] {Serializer.class});
            ce.setDefaultImports(
                "java.util.UUID",
                "java.util.BitSet",

                "org.apache.ignite.internal.schema.ByteBufferTuple",
                "org.apache.ignite.internal.schema.Columns",
                "org.apache.ignite.internal.schema.SchemaDescriptor",
                "org.apache.ignite.internal.schema.Tuple",
                "org.apache.ignite.internal.schema.TupleAssembler",
                "org.apache.ignite.internal.util.IgniteUnsafeUtils"
            );

            final StringBuilder sb = new StringBuilder(8 * 1024);

            // Create class fields and constructor.
            sb.append("private final SchemaDescriptor schema;" + LF);
            sb.append("private final Class kClass;" + LF);
            sb.append("private final Class vClass;" + LF);
            // Constructor.
            sb.append(LF + "public ").append(className).append("(SchemaDescriptor schema, Class kClass, Class vClass) {" + LF);
            sb.append(TAB + "this.kClass = kClass;" + LF);
            sb.append(TAB + "this.vClass = vClass;" + LF);
            sb.append(TAB + "this.schema = schema; " + LF);
            sb.append("}" + LF);

            // Build field accessor generators.
            final ObjectMarshallerExprGenerator keyMarsh = createObjectMarshaller(keyClass, "kClass", schema.keyColumns(), 0);
            final ObjectMarshallerExprGenerator valMarsh = createObjectMarshaller(valClass, "vClass", schema.valueColumns(), schema.keyColumns().length());

            generateTupleFactoryMethod(sb, schema, keyMarsh, valMarsh);

            writeSerializeMethod(sb, keyMarsh, valMarsh);
            writeDeserializeMethods(sb, keyMarsh, valMarsh);

            final String code = sb.toString();

            if (enabledDebug) {
                ce.setDebuggingInformation(true, true, true);
                //TODO: pass to logger.
                System.out.println(code);
            }

            ce.setParentClassLoader(getClass().getClassLoader());
            ce.cook(code);

            try {
                final Constructor<Serializer> ctor = (Constructor<Serializer>)ce.getClazz()
                    .getDeclaredConstructor(schema.getClass(), Class.class, Class.class);

                return ctor.newInstance(schema, keyClass, valClass);
            }
            catch (Exception ex) {
                System.err.println(code);

                throw ex;
            }
        }
        catch (Exception ex) {
            //TODO: fallback to java serializer?
            throw new IllegalStateException(ex);
        }
    }

    private ObjectMarshallerExprGenerator createObjectMarshaller(Class<?> aClass, String classField, Columns columns,
        int firstColIdx) {
        BinaryMode mode = MarshallerUtil.mode(aClass);

        if (mode != null)
            return new ObjectMarshallerExprGenerator.IdentityObjectMarshaller(createAccessor(mode, firstColIdx, -1L));

        FieldAccessExprGenerator[] accessors = new FieldAccessExprGenerator[columns.length()];
        try {
            for (int i = 0; i < columns.length(); i++) {
                final Field field = aClass.getDeclaredField(columns.column(i).name());

                accessors[i] = createAccessor(
                    MarshallerUtil.mode(field.getType()),
                    firstColIdx + i /* schma absolute index. */,
                    IgniteUnsafeUtils.objectFieldOffset(field));
            }
        }
        catch (NoSuchFieldException ex) {
            throw new IllegalStateException(ex);
        }

        return new ObjectMarshallerExprGenerator(classField, accessors);
    }

    private FieldAccessExprGenerator createAccessor(BinaryMode mode, int colIdx, long offset) {
        switch (mode) {
            case BYTE:
                return new FieldAccessExprGenerator(
                    colIdx,
                    "Byte",
                    "asm.appendByte",
                    "tuple.byteValueBoxed",
                    offset);

            case P_BYTE:
                return new FieldAccessExprGenerator(
                    colIdx,
                    "tuple.byteValue", "asm.appendByte",
                    offset,
                    "IgniteUnsafeUtils.getByteField",
                    "IgniteUnsafeUtils.putByteField"
                );

            case SHORT:
                return new FieldAccessExprGenerator(
                    colIdx,
                    "Short",
                    "asm.appendShort",
                    "tuple.shortValueBoxed",
                    offset);

            case P_SHORT:
                return new FieldAccessExprGenerator(
                    colIdx,
                    "tuple.shortValue", "asm.appendShort",
                    offset,
                    "IgniteUnsafeUtils.getShortField",
                    "IgniteUnsafeUtils.putShortField"
                );

            case INT:
                return new FieldAccessExprGenerator(
                    colIdx,
                    "Integer",
                    "asm.appendInt",
                    "tuple.intValueBoxed",
                    offset);

            case P_INT:
                return new FieldAccessExprGenerator(
                    colIdx,
                    "tuple.intValue", "asm.appendInt",
                    offset,
                    "IgniteUnsafeUtils.getIntField",
                    "IgniteUnsafeUtils.putIntField"
                );

            case LONG:
                return new FieldAccessExprGenerator(
                    colIdx,
                    "Long",
                    "asm.appendLong",
                    "tuple.longValueBoxed",
                    offset);

            case P_LONG:
                return new FieldAccessExprGenerator(
                    colIdx,
                    "tuple.longValue", "asm.appendLong",
                    offset,
                    "IgniteUnsafeUtils.getLongField",
                    "IgniteUnsafeUtils.putLongField"
                );

            case FLOAT:
                return new FieldAccessExprGenerator(
                    colIdx,
                    "Float",
                    "asm.appendFloat",
                    "tuple.floatValueBoxed",
                    offset);

            case P_FLOAT:
                return new FieldAccessExprGenerator(
                    colIdx,
                    "tuple.floatValue", "asm.appendFloat",
                    offset,
                    "IgniteUnsafeUtils.getFloatField",
                    "IgniteUnsafeUtils.putFloatField"
                );

            case DOUBLE:
                return new FieldAccessExprGenerator(
                    colIdx,
                    "Double",
                    "asm.appendDouble",
                    "tuple.doubleValueBoxed",
                    offset);

            case P_DOUBLE:
                return new FieldAccessExprGenerator(
                    colIdx,
                    "tuple.doubleValue", "asm.appendDouble",
                    offset,
                    "IgniteUnsafeUtils.getDoubleField",
                    "IgniteUnsafeUtils.putDoubleField"
                );

            case UUID:
                return new FieldAccessExprGenerator(
                    colIdx,
                    "UUID",
                    "asm.appendUuid",
                    "tuple.uuidValue",
                    offset);

            case BITSET:
                return new FieldAccessExprGenerator(
                    colIdx,
                    "BitSet",
                    "asm.appendBitmask",
                    "tuple.bitmaskValue",
                    offset);

            case STRING:
                return new FieldAccessExprGenerator(
                    colIdx,
                    "String",
                    "asm.appendString",
                    "tuple.stringValue",
                    offset);

            case BYTE_ARR:
                return new FieldAccessExprGenerator(
                    colIdx,
                    "byte[]",
                    "asm.appendBytes",
                    "tuple.bytesValue",
                    offset);
            default:
                throw new IllegalStateException("Unsupportd binary mode");
        }
    }

    private void writeSerializeMethod(StringBuilder sb,
        ObjectMarshallerExprGenerator keyMarsh,
        ObjectMarshallerExprGenerator valMarsh
    ) {
        sb.append(LF + "@Override public byte[] serialize(Object key, Object val) throws SerializationException {" + LF);
        sb.append(TAB + "TupleAssembler asm = createAssembler(key, val);" + LF);

        sb.append(TAB + "{" + LF);
        sb.append(TAB + TAB + "Object obj = key;" + LF);
        keyMarsh.marshallObject(sb, TAB + TAB);
        sb.append(TAB + "} {" + LF);
        sb.append(TAB + TAB + "Object obj = val;" + LF);
        valMarsh.marshallObject(sb, TAB + TAB);
        sb.append(TAB + "}" + LF);

        sb.append(TAB + "return asm.build();" + LF);
        sb.append("}" + LF);
    }

    private void writeDeserializeMethods(StringBuilder sb,
        ObjectMarshallerExprGenerator keyMarsh,
        ObjectMarshallerExprGenerator valMarsh
    ) {
        sb.append(LF + "@Override public Object deserializeKey(byte[] data) throws SerializationException {" + LF);
        sb.append(TAB + "Tuple tuple = new ByteBufferTuple(schema, data);" + LF);

        keyMarsh.unmarshallObject(sb, TAB);

        sb.append(TAB + "return obj;" + LF);
        sb.append("}" + LF);

        sb.append(LF + "@Override public Object deserializeValue(byte[] data) throws SerializationException {" + LF);
        sb.append(TAB + "Tuple tuple = new ByteBufferTuple(schema, data);" + LF);

        valMarsh.unmarshallObject(sb, TAB);

        sb.append(TAB + "return obj;" + LF);
        sb.append("}" + LF);
    }

    private void generateTupleFactoryMethod(
        StringBuilder sb,
        SchemaDescriptor schema,
        ObjectMarshallerExprGenerator keyMarsh,
        ObjectMarshallerExprGenerator valMarsh
    ) {
        sb.append(LF + "TupleAssembler createAssembler(Object key, Object val) {" + LF);
        sb.append(TAB + "int nonNullVarlenKeys = 0; int nonNullVarlenValues = 0;" + LF);
        sb.append(TAB + "int nonNullVarlenKeysSize = 0; int nonNullVarlenValuesSize = 0;" + LF);
        sb.append(LF);
        sb.append(TAB + "Columns keyCols = schema.keyColumns();" + LF);
        sb.append(TAB + "Columns valCols = schema.valueColumns();" + LF);
        sb.append(LF);

        Columns keyCols = schema.keyColumns();
        if (keyCols.firstVarlengthColumn() >= 0) {
            sb.append(TAB + "{" + LF);
            sb.append(TAB + TAB + "Object fVal, obj = key;" + LF);

            for (int i = keyCols.firstVarlengthColumn(); i < keyCols.length(); i++) {
                assert !keyCols.column(i).type().spec().fixedLength();

                sb.append(TAB + TAB + "assert !keyCols.column(").append(i).append(").type().spec().fixedLength();" + LF);
                sb.append(TAB + TAB + "fVal = ").append(keyMarsh.accessors[i].getFieldExpr()).append(";" + LF);
                sb.append(TAB + TAB + "if (fVal != null) {" + LF);
                sb.append(TAB + TAB + TAB + "nonNullVarlenKeysSize += MarshallerUtil.getValueSize(fVal, keyCols.column(").append(i).append(").type());").append(LF);
                sb.append(TAB + TAB + TAB + "nonNullVarlenKeys++;" + LF);
                sb.append(TAB + TAB + "}" + LF);
            }

            sb.append(TAB + "}" + LF);
        }

        Columns valCols = schema.valueColumns();
        if (valCols.firstVarlengthColumn() >= 0) {
            sb.append(TAB + "{" + LF);
            sb.append(TAB + TAB + "Object fVal, obj = val;" + LF);

            for (int i = valCols.firstVarlengthColumn(); i < valCols.length(); i++) {
                assert !valCols.column(i).type().spec().fixedLength();

                sb.append(TAB + TAB + "assert !valCols.column(").append(i).append(").type().spec().fixedLength();" + LF);
                sb.append(TAB + TAB + "fVal = ").append(valMarsh.accessors[i].getFieldExpr()).append(";" + LF);
                sb.append(TAB + TAB + "if (fVal != null) {" + LF);
                sb.append(TAB + TAB + TAB + "nonNullVarlenValuesSize += MarshallerUtil.getValueSize(fVal, valCols.column(").append(i).append(").type());" + LF);
                sb.append(TAB + TAB + TAB + "nonNullVarlenValues++;" + LF);
                sb.append(TAB + TAB + "}" + LF);
            }
            sb.append(TAB + "}" + LF);
        }

        sb.append(LF);
        sb.append(TAB + "int size = TupleAssembler.tupleSize(" + LF);
        sb.append(TAB + TAB + "keyCols, nonNullVarlenKeys, nonNullVarlenKeysSize, " + LF);
        sb.append(TAB + TAB + "valCols, nonNullVarlenValues, nonNullVarlenValuesSize); " + LF);
        sb.append(LF);

        sb.append(TAB + "return new TupleAssembler(schema, size, nonNullVarlenKeys, nonNullVarlenValues);" + LF);
        sb.append("}" + LF);
    }
}
