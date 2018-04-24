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

import android.util.Log;

import com.sun.jna.Pointer;

import java.util.Date;
import java.util.UUID;

/**
 * Wraps a `Coll` result from a Mentat query.
 * A `Coll` result is a list of rows of single values of type {@link TypedValue}.
 * Values for individual rows can be fetched as {@link TypedValue} or converted into a requested type.
 * <p>
 * Row values can be fetched as one of the following types:
 * <ul>
 * <li>{@link TypedValue}</li>
 * <li>long</li>
 * <li>Entid (as long)</li>
 * <li>Keyword (as String)</li>
 * <li>boolean</li>
 * <li>double</li>
 * <li>{@link Date}</li>
 * <li>{@link String}</li>
 * <li>{@link UUID}</li>
 * </ul>
 * <p>
 * To iterate over the result set use standard iteration flows.
 */
public class CollResult extends TupleResult implements Iterable<TypedValue> {

    public CollResult(Pointer pointer) {
        super(pointer);
    }

    @Override
    public void close() {
        Log.i("CollResult", "close");

        if (this.rawPointer != null) {
            JNA.INSTANCE.destroy(this.rawPointer);
        }
    }

    @Override
    public ColResultIterator iterator() {
        Pointer iterPointer = JNA.INSTANCE.values_iter(this.rawPointer);
        this.rawPointer = null;
        if (iterPointer == null) {
            return null;
        }
        return new ColResultIterator(iterPointer);
    }
}
