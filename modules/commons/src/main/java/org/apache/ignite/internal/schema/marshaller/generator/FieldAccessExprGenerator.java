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

package org.apache.ignite.internal.schema.marshaller.generator;

import org.jetbrains.annotations.Nullable;

/**
 * Object field access expression generators.
 */
class FieldAccessExprGenerator {
    /** Object field offset or {@code -1} for identity accessor. */
    private final long offset;

    /** Absolute schema index. */
    private final int colIdx;

    /** Class cast expression. */
    private final String castClassExpr;

    /** Write column value expression. */
    private final String writeColMethod;

    /** Read column value expression. */
    private final String readColMethod;

    /** Read object field expression. */
    private final String getFieldMethod;

    /** Write object field expression. */
    private final String putFieldMethod;

    /**
     * Constructor.
     *
     * @param colIdx Absolute schema index in schema.
     * @param castClassExpr Class cast expression
     * @param readColMethod Read column value expression.
     * @param writeColMethod Write column value expression.
     * @param offset Field offset or {@code -1} for identity accessor.
     */
    public FieldAccessExprGenerator(
        int colIdx,
        String castClassExpr,
        String readColMethod,
        String writeColMethod,
        long offset
    ) {
        this(colIdx, castClassExpr, readColMethod, writeColMethod, offset,
            "IgniteUnsafeUtils.getObjectField", "IgniteUnsafeUtils.putObjectField");
    }

    /**
     * Constructor.
     *
     * @param colIdx Absolute schema index in schema.
     * @param readColMethod Read column value expression.
     * @param writeColMethod Write column value expression.
     * @param offset Field offset or {@code -1} for identity accessor.
     * @param getFieldMethod Read object field expression.
     * @param putFieldMethod Read object field expression.
     */
    public FieldAccessExprGenerator(
        int colIdx,
        String readColMethod,
        String writeColMethod,
        long offset,
        String getFieldMethod,
        String putFieldMethod
    ) {
        this(colIdx, null /* primitive type */, readColMethod, writeColMethod, offset, getFieldMethod, putFieldMethod);
    }

    /**
     * Constructor.
     *
     * @param colIdx Absolute schema index in schema.
     * @param castClassExpr Class cast expression or {@code null} if not applicable.
     * @param readColMethod Read column value expression.
     * @param writeColMethod Write column value expression.
     * @param offset Field offset or {@code -1} for identity accessor.
     * @param getFieldMethod Read object field expression.
     * @param putFieldMethod Read object field expression.
     */
    private FieldAccessExprGenerator(
        int colIdx,
        @Nullable String castClassExpr,
        String readColMethod,
        String writeColMethod,
        long offset,
        String getFieldMethod,
        String putFieldMethod
    ) {
        this.offset = offset;
        this.colIdx = colIdx;
        this.castClassExpr = castClassExpr;
        this.putFieldMethod = putFieldMethod;
        this.getFieldMethod = getFieldMethod;
        this.writeColMethod = writeColMethod;
        this.readColMethod = readColMethod;
    }

    /**
     * @return Object field value access expression or object value expression for simple types.
     */
    public String getFieldExpr() {
        if (offset == -1)
            return "obj"; // Identity accessor.

        return getFieldMethod + "(obj, " + offset + ')';
    }

    /**
     * Appends write value to field expression.
     *
     * @param sb String bulder.
     * @param valueExpression Value expression.
     * @param indent Line indentation.
     */
    public final void appendPutFieldExpr(StringBuilder sb, String valueExpression, String indent) {
        sb.append(indent).append(putFieldMethod).append("(obj, ").append(offset).append(", ").append(valueExpression).append(')');
        sb.append(";" + JaninoSerializerGenerator.LF);
    }

    /**
     * Appends write value to column expression.
     *
     * @param sb String bulder.
     * @param valueExpr Value expression.
     * @param indent Line indentation.
     */
    public final void appendWriteColumnExpr(StringBuilder sb, String valueExpr, String indent) {
        sb.append(indent).append(writeColMethod).append('(');

        if (castClassExpr != null)
            sb.append("(").append(castClassExpr).append(")");

        sb.append(valueExpr).append(");" + JaninoSerializerGenerator.LF);
    }

    /**
     * @return Column value read expression.
     */
    public String readColumnExpr() {
        return readColMethod + "(" + colIdx + ")";
    }
}
