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

import java.util.Iterator;

/**
 * Iterator for a {@link CollResult}
 */
public class ColResultIterator extends RustObject implements Iterator {

    Pointer nextPointer;

    ColResultIterator(Pointer iterator) {
        this.rawPointer = iterator;
    }

    private Pointer getNextPointer() {
        return JNA.INSTANCE.typed_value_list_iter_next(this.rawPointer);
    }

    @Override
    public boolean hasNext() {
        this.nextPointer = getNextPointer();
        return this.nextPointer != null;
    }

    @Override
    public TypedValue next() {
        Pointer next = this.nextPointer == null ? getNextPointer() : this.nextPointer;
        if (next == null) {
            return null;
        }

        return new TypedValue(next);
    }

    @Override
    public void close() {
        if (this.rawPointer != null) {
            JNA.INSTANCE.typed_value_list_iter_destroy(this.rawPointer);
        }
    }
}
