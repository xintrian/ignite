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

/**
 * (De)serializer handler for object of base types.
 */
class BaseTypeMarshaller implements Marshaller {
    /** Write mode. */
    private final BinaryMode mode;

    /**
     * Constructor.
     *
     * @param mode Binary mode.
     */
    public BaseTypeMarshaller(BinaryMode mode) {
        this.mode = Objects.requireNonNull(mode);
    }

    /** {@inheritDoc} */
    @Override public int nonNullVarLenCols(Object obj) {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public Object value(Object obj, int colIdx) {
        assert colIdx == 0 : "No columns expected for native types.";

        return obj;
    }

    /** {@inheritDoc} */
    @Override public Object readObject(Tuple reader) {
        return JavaSerializer.readRefValue(reader, 0, mode);
    }

    /** {@inheritDoc} */
    @Override public void writeObject(Object val, TupleAssembler writer) {
        Objects.requireNonNull(val, "Null values are not supported."); //TODO: Add 'tombstones' (null-values) support.

        JavaSerializer.writeRefObject(val, writer, mode);
    }
}
