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

class ObjectMarshallerExprGenerator {
    private final String classFieldExpr;
    protected FieldAccessExprGenerator[] accessors;

    public ObjectMarshallerExprGenerator(String classFieldExpr, FieldAccessExprGenerator[] accessors) {
        this.accessors = accessors;
        this.classFieldExpr = classFieldExpr;
    }

    public void unmarshallObject(StringBuilder sb, String indent) {
        sb.append(indent).append("Object obj;" + JaninoSerializerGenerator.LF);
        sb.append(indent).append("try {" + JaninoSerializerGenerator.LF);
        sb.append(indent).append(JaninoSerializerGenerator.TAB + "obj = IgniteUnsafeUtils.allocateInstance(").append(classFieldExpr).append(");" + JaninoSerializerGenerator.LF);

        for (int i = 0; i < accessors.length; i++)
            accessors[i].addPutFieldExpr(sb, accessors[i].readColumnExpr(), indent + JaninoSerializerGenerator.TAB);
        sb.append(indent).append("} catch (InstantiationException ex) {" + JaninoSerializerGenerator.LF);

        sb.append(indent).append(JaninoSerializerGenerator.TAB + "throw new SerializationException(\"Failed to instantiate object: \" + ")
            .append(classFieldExpr).append(".getSimpleName(), ex);").append(JaninoSerializerGenerator.LF);
        sb.append(indent).append("}" + JaninoSerializerGenerator.LF);
    }

    public void marshallObject(StringBuilder sb, String indent) {
        sb.append(indent).append("try {" + JaninoSerializerGenerator.LF);

        for (int i = 0; i < accessors.length; i++)
            accessors[i].addWriteColumnExpr(sb, accessors[i].getFieldExpr(), indent + JaninoSerializerGenerator.TAB);

        sb.append(indent).append("} catch (Exception ex) {" + JaninoSerializerGenerator.LF);
        sb.append(indent).append(JaninoSerializerGenerator.TAB + "throw new SerializationException(ex);").append(JaninoSerializerGenerator.LF);
        sb.append(indent).append("}" + JaninoSerializerGenerator.LF);
    }

    static class IdentityObjectMarshaller extends ObjectMarshallerExprGenerator {
        IdentityObjectMarshaller(FieldAccessExprGenerator accessors) {
            super(null, new FieldAccessExprGenerator[] {accessors});
        }

        @Override public void marshallObject(StringBuilder sb, String indent) {
            for (int i = 0; i < accessors.length; i++)
                accessors[i].addWriteColumnExpr(sb, "obj", indent);
        }

        @Override public void unmarshallObject(StringBuilder sb, String indent) {
            sb.append(indent).append("Object obj = ").append(accessors[0].readColumnExpr()).append(";" + JaninoSerializerGenerator.LF);
        }
    }
}
