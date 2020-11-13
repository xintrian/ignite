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

import java.util.Objects;
import org.apache.ignite.internal.schema.Tuple;
import org.apache.ignite.internal.schema.TupleAssembler;
import org.jetbrains.annotations.Nullable;

/**
 * Marshaller.
 */
public class Marshaller {
    /**
     * Field accessors for mapped columns.
     * Array has same size and order as columns.
     */
    private final FieldAccessor[] fieldAccessors;

    /**
     * Object factory for complex types or {@code null} for basic type.
     */
    private final ObjectFactory<?> factory;

    /**
     * Constructor.
     * Creates marshaller for complex types.
     *
     * @param factory Object factory.
     * @param fieldAccessors Object field accessors for mapped columns.
     */
    @SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
    public Marshaller(ObjectFactory<?> factory, FieldAccessor[] fieldAccessors) {
        this.fieldAccessors = fieldAccessors;
        this.factory = Objects.requireNonNull(factory);
    }

    /**
     * Constructor.
     * Creates marshaller for basic types.
     *
     * @param fieldAccessor Identity field accessor for object of basic type.
     */
    public Marshaller(FieldAccessor fieldAccessor) {
        fieldAccessors = new FieldAccessor[] {fieldAccessor};
        factory = null;
    }

    /**
     * Reads object field.
     *
     * @param obj Object.
     * @param fldIdx Field index.
     * @return Field value.
     * @throws SerializationException If failed.
     */
    public @Nullable Object value(Object obj, int fldIdx) throws SerializationException {
        return fieldAccessors[fldIdx].value(obj);
    }

    /**
     * Reads object from tuple.
     *
     * @param reader Tuple reader.
     * @return Object.
     * @throws SerializationException If failed.
     */
    public Object readObject(Tuple reader) throws SerializationException {
        if (isBasicTypeMarshaller())
            return fieldAccessors[0].read(reader);

        final Object obj = factory.newInstance();

        for (int fldIdx = 0; fldIdx < fieldAccessors.length; fldIdx++)
            fieldAccessors[fldIdx].read(obj, reader);

        return obj;
    }

    /**
     * Write object to tuple.
     *
     * @param obj Object.
     * @param writer Tuple writer.
     * @throws SerializationException If failed.
     */
    public void writeObject(Object obj, TupleAssembler writer) throws SerializationException {
        for (int fldIdx = 0; fldIdx < fieldAccessors.length; fldIdx++)
            fieldAccessors[fldIdx].write(obj, writer);
    }

    /**
     * @return {@code true} if it is marshaller for basic type, {@code false} otherwise.
     */
    private boolean isBasicTypeMarshaller() {
        return factory == null;
    }
}
