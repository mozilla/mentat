/* -*- Mode: Java; c-basic-offset: 4; tab-width: 20; indent-tabs-mode: nil; -*-
 * Copyright 2018 Mozilla
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License. */

package com.mozilla.mentat;

import com.sun.jna.Pointer;

import java.nio.ByteBuffer;
import java.util.Date;
import java.util.UUID;

/**
 * A wrapper around Mentat's `TypedValue` Rust object. This class wraps a raw pointer to a Rust `TypedValue`
 * struct and provides accessors to the values according to expected result type.
 * </p>
 * As the FFI functions for fetching values are consuming, this class keeps a copy of the result internally after
 * fetching so that the value can be referenced several times.
 * </p>
 * Also, due to the consuming nature of the FFI layer, this class also manages it's raw pointer, nilling it after calling the
 * FFI conversion function so that the underlying base class can manage cleanup.
 */
public class TypedValue extends RustObject {

    private Object value;

    private boolean isConsumed() {
        return this.rawPointer == null;
    }

    public TypedValue(Pointer pointer) {
        this.rawPointer = pointer;
    }

    /**
     * This value as a {@link Long}. This function will panic if the `ValueType` of this
     * {@link TypedValue} is not a `Long`
     * @return  the value of this {@link TypedValue} as a {@link Long}
     */
    public Long asLong() {
        if (!this.isConsumed()) {
            this.value = JNA.INSTANCE.typed_value_as_long(this.rawPointer);
            this.rawPointer = null;
        }
        return (Long)value;
    }

    /**
     * This value as a Entid. This function will panic if the `ValueType` of this
     * {@link TypedValue} is not a `Ref`
     * @return  the value of this {@link TypedValue} as a Entid
     */
    public Long asEntid() {
        if (!this.isConsumed()) {
            this.value = JNA.INSTANCE.typed_value_as_entid(this.rawPointer);
            this.rawPointer = null;
        }
        return (Long)value;
    }

    /**
     * This value as a keyword {@link String}. This function will panic if the `ValueType` of this
     * {@link TypedValue} is not a `Keyword`
     * @return  the value of this {@link TypedValue} as a Keyword
     */
    public String asKeyword() {
        if (!this.isConsumed()) {
            this.value = JNA.INSTANCE.typed_value_as_kw(this.rawPointer);
            this.rawPointer = null;
        }
        return (String)value;
    }

    /**
     * This value as a {@link Boolean}. This function will panic if the `ValueType` of this
     * {@link TypedValue} is not a `Boolean`
     * @return  the value of this {@link TypedValue} as a {@link Boolean}
     */
    public Boolean asBoolean() {
        if (!this.isConsumed()) {
            long value = JNA.INSTANCE.typed_value_as_boolean(this.rawPointer);
            this.value = value == 0 ? false : true;
            this.rawPointer = null;
        }
        return (Boolean) this.value;
    }

    /**
     * This value as a {@link Double}. This function will panic if the `ValueType` of this
     * {@link TypedValue} is not a `Double`
     * @return  the value of this {@link TypedValue} as a {@link Double}
     */
    public Double asDouble() {
        if (!this.isConsumed()) {
            this.value = JNA.INSTANCE.typed_value_as_double(this.rawPointer);
            this.rawPointer = null;
        }
        return (Double)value;
    }

    /**
     * This value as a {@link Date}. This function will panic if the `ValueType` of this
     * {@link TypedValue} is not a `Instant`
     * @return  the value of this {@link TypedValue} as a {@link Date}
     */
    public Date asDate() {
        if (!this.isConsumed()) {
            this.value = new Date(JNA.INSTANCE.typed_value_as_timestamp(this.rawPointer) * 1_000);
            this.rawPointer = null;
        }
        return (Date)this.value;
    }

    /**
     * This value as a {@link String}. This function will panic if the `ValueType` of this
     * {@link TypedValue} is not a `String`
     * @return  the value of this {@link TypedValue} as a {@link String}
     */
    public String asString() {
        if (!this.isConsumed()) {
            this.value = JNA.INSTANCE.typed_value_as_string(this.rawPointer);
            this.rawPointer = null;
        }
        return (String)value;
    }

    /**
     * This value as a {@link UUID}. This function will panic if the `ValueType` of this
     * {@link TypedValue} is not a `Uuid`
     * @return  the value of this {@link TypedValue} as a {@link UUID}
     */
    public UUID asUUID() {
        if (!this.isConsumed()) {
            Pointer uuidPtr = JNA.INSTANCE.typed_value_as_uuid(this.rawPointer);
            byte[] bytes = uuidPtr.getByteArray(0, 16);
            ByteBuffer bb = ByteBuffer.wrap(bytes);
            long high = bb.getLong();
            long low = bb.getLong();
            this.value = new UUID(high, low);
            this.rawPointer = null;
        }
        return (UUID)this.value;
    }

    @Override
    public void close() {
        if (this.rawPointer != null) {
            JNA.INSTANCE.typed_value_destroy(this.rawPointer);
        }
    }
}
