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

import org.apache.ignite.internal.schema.Tuple;
import org.apache.ignite.internal.schema.TupleAssembler;

/**
 * Marshaller interface.
 */
public interface Marshaller {
    /**
     * Counts number non-null fields of variable length types.
     *
     * @param obj Object to analyze.
     * @return Amount of non-null fields of variable length types.
     * @throws SerializationException If failed.
     */
    //TODO: do we really need this to be 'public'? Would package-private in abstract class be better?
    int nonNullVarLenCols(Object obj) throws SerializationException;

    /**
     * Reads object field value mapped to column of given index in schema.
     *
     * @param obj Object.
     * @param colIdx Column index.
     * @return Field value.
     * @throws SerializationException If failed.
     */
    Object value(Object obj, int colIdx) throws SerializationException;

    /**
     * Reads object from tuple.
     *
     * @param reader Tuple reader.
     * @return Object.
     * @throws SerializationException If failed.
     */
    Object readObject(Tuple reader) throws SerializationException;

    /**
     * Write object to tuple.
     *
     * @param obj Object.
     * @param writer Tuple writer.
     * @throws SerializationException If failed.
     */
    void writeObject(Object obj, TupleAssembler writer) throws SerializationException;
}
