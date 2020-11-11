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

import org.apache.ignite.internal.schema.Columns;
import org.apache.ignite.internal.schema.Tuple;
import org.apache.ignite.internal.schema.TupleAssembler;
import org.jetbrains.annotations.Unmodifiable;

/**
 * General purpose (de)serializer handler for objects of complex types.
 */
public class ClassMarshaller<T> implements Marshaller {
    /** Mapped columns. */
    private final Columns cols;

    /**
     * Field accessors for mapped columns.
     * Array has same size and order as columns.
     */
    private final FieldAccessor[] fieldAccessors;

    /** Object factory for class. */
    private final ObjectFactory<T> factory;

    /**
     * Constructor.
     *
     * @param cols Mapped columns.
     * @param factory Object factory.
     * @param fieldAccessors Object field accessors for mapped columns.
     */
    @SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
    public ClassMarshaller(Columns cols, ObjectFactory<T> factory, FieldAccessor[] fieldAccessors) {
        this.cols = cols;
        this.fieldAccessors = fieldAccessors;
        this.factory = factory;
    }

    /** {@inheritDoc} */
    @Override public int nonNullVarLenCols(Object obj) throws SerializationException {
        if (obj == null)
            return 0;

        int cnt = 0;

        for (int fldIdx = cols.firstVarlengthColumn(); fldIdx < cols.length(); fldIdx++) {
            if (fieldAccessors[fldIdx].value(obj) == null)
                cnt++;
        }

        return cnt;
    }

    /** {@inheritDoc} */
    @Override public Object value(Object obj, int fldIdx) throws SerializationException {
        return fieldAccessors[fldIdx].value(obj);
    }

    /** {@inheritDoc} */
    @Override public Object readObject(Tuple reader) throws SerializationException {
        final T obj = factory.newInstance();

        for (int fldIdx = 0; fldIdx < fieldAccessors.length; fldIdx++)
            fieldAccessors[fldIdx].read(obj, reader);

        return obj;
    }

    /** {@inheritDoc} */
    @Override public void writeObject(Object obj, TupleAssembler writer) throws SerializationException {
        for (int fldIdx = 0; fldIdx < fieldAccessors.length; fldIdx++) {
            fieldAccessors[fldIdx].write(obj, writer);
        }
    }
}
