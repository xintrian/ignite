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
import java.lang.reflect.InvocationTargetException;

/**
 * Object factory.
 */
// TODO: Rewrite to direct field access via Unsafe to bypass security checks.
// TODO: Extract interface, move to java-8 profile and add Java9+ implementation using VarHandles.
class ObjectFactory<T> {
    /**
     * Creates factory for class.
     *
     * @param tClass Class.
     * @return Object factory.
     */
    static <T> ObjectFactory<T> classFactory(Class<T> tClass) {
        try {
            return new ObjectFactory<>(tClass.getDeclaredConstructor());
        }
        catch (NoSuchMethodException e) {
            throw new IllegalStateException("No default constructor found for class: " + tClass.getSimpleName());
        }
    }

    /** Constructor for class. */
    private final Constructor<T> ctor;

    /**
     * Constructor.
     *
     * @param ctor Class constructor for factory.
     */
    private ObjectFactory(Constructor<T> ctor) {
        this.ctor = ctor;
    }

    /**
     * Creates new class instance using default constructor.
     *
     * @return New instance of class.
     * @throws IllegalStateException If failed.
     */
    public T newInstance() throws IllegalStateException {
        try {
            return ctor.newInstance();
        }
        catch (InstantiationException | InvocationTargetException | IllegalAccessException e) {
            throw new IllegalStateException("Failed to instantiate class: " + ctor.getDeclaringClass(), e);
        }
    }
}
