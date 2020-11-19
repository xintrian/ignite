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

class FieldAccessExprGenerator {
    final long offset;
    final int colIdx;

    private final String castClass;
    private final String putFieldMethod;
    private final String getFieldMethod;
    private final String writeColMethod;
    private final String readColMethod;

    public FieldAccessExprGenerator(int colIdx, String castClass, String writeColMethod,
        String readColMethod, long offset) {
        this(colIdx, castClass, readColMethod, writeColMethod, offset,
            "IgniteUnsafeUtils.getObjectField", "IgniteUnsafeUtils.putObjectField");
    }

    public FieldAccessExprGenerator(int colIdx, String readColMethod, String writeColMethod, long offset,
        String getFieldMethod, String putFieldMethod) {
        this(colIdx, null, readColMethod, writeColMethod, offset, getFieldMethod, putFieldMethod);
    }

    private FieldAccessExprGenerator(int colIdx, String castClass, String readColMethod, String writeColMethod,
        long offset, String getFieldMethod, String putFieldMethod) {
        this.offset = offset;
        this.colIdx = colIdx;
        this.castClass = castClass;
        this.putFieldMethod = putFieldMethod;
        this.getFieldMethod = getFieldMethod;
        this.writeColMethod = writeColMethod;
        this.readColMethod = readColMethod;
    }

    public String getFieldExpr() {
        if (offset == -1)
            return "obj";

        return getFieldMethod + "(obj, " + offset + ')';
    }

    public final void addPutFieldExpr(StringBuilder sb, String val, String indent) {
        sb.append(indent).append(putFieldMethod).append("(obj, ").append(offset).append(", ").append(val).append(')');
        sb.append(";" + JaninoSerializerGenerator.LF);
    }

    public final void addWriteColumnExpr(StringBuilder sb, String expr, String indent) {
        sb.append(indent).append(writeColMethod).append('(');

        if (castClass != null)
            sb.append("(").append(castClass).append(")");

        sb.append(expr).append(");" + JaninoSerializerGenerator.LF);
    }

    public String readColumnExpr() {
        return readColMethod + "(" + colIdx + ")";
    }
}
