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

import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.Columns;

public abstract class MarshallerFactory {
    /**
     * Creates marshaller for class.
     *
     * @param cols Columns.
     * @param firstColId First column position in schema.
     * @param aClass Type.
     * @return Marshaller.
     */
    public static Marshaller createMarshaller(Columns cols, int firstColId, Class<? extends Object> aClass) {
        final BinaryMode mode = JavaSerializer.mode(aClass);

        if (mode != null) {
            final Column col = cols.column(0);

            assert cols.length() == 1;
            assert mode.typeSpec() == col.type().spec() : "Target type is not compatible.";
            assert !aClass.isPrimitive() : "Non-nullable types are not allowed.";

            return new Marshaller(FieldAccessor.createIdentityAccessor(col, firstColId, mode));
        }

        FieldAccessor[] fieldAccessors = new FieldAccessor[cols.length()];

        // Build accessors
        for (int i = 0; i < cols.length(); i++) {
            final Column col = cols.column(i);

            final int colIdx = firstColId + i; /* Absolute column idx in schema. */
            fieldAccessors[i] = FieldAccessor.create(aClass, col, colIdx);
        }

        return new Marshaller(new ObjectFactory<>(aClass), fieldAccessors);
    }
}
