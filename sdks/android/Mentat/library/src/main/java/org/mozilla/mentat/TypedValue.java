/* -*- Mode: Java; c-basic-offset: 4; tab-width: 20; indent-tabs-mode: nil; -*-
 * Copyright 2018 Mozilla
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License. */

package org.mozilla.mentat;

import com.sun.jna.Pointer;

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
public class TypedValue extends RustObject<JNA.TypedValue> {

    private Object value;

    public TypedValue(JNA.TypedValue pointer) {
        super(pointer);
    }

    /**
     * This value as a {@link Long}. This function will panic if the `ValueType` of this
     * {@link TypedValue} is not a `Long`
     * @return  the value of this {@link TypedValue} as a {@link Long}
     */
    public Long asLong() {
        if (!this.isConsumed()) {
            this.value = JNA.INSTANCE.typed_value_into_long(this.consumePointer());
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
            this.value = JNA.INSTANCE.typed_value_into_entid(this.consumePointer());
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
            this.value = getAndConsumeMentatString(JNA.INSTANCE.typed_value_into_kw(this.consumePointer()));
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
            long value = JNA.INSTANCE.typed_value_into_boolean(this.consumePointer());
            this.value = value == 0 ? false : true;
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
            this.value = JNA.INSTANCE.typed_value_into_double(this.consumePointer());
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
            this.value = new Date(JNA.INSTANCE.typed_value_into_timestamp(this.consumePointer()) * 1_000);
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
            this.value = getAndConsumeMentatString(
                    JNA.INSTANCE.typed_value_into_string(this.consumePointer()));
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
            this.value = getAndConsumeUUIDPointer(JNA.INSTANCE.typed_value_into_uuid(this.consumePointer()));
        }
        return (UUID)this.value;
    }

    @Override
    protected void destroyPointer(JNA.TypedValue p) {
        JNA.INSTANCE.typed_value_destroy(p);
    }
}
