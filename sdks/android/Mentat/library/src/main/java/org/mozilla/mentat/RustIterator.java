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
import java.util.NoSuchElementException;

// Common factored-out code shared by both RelResultIterator and CollResultIterator (and possibly
// others in the future). This code is a bit error-prone so it's worth avoiding the duplication.
abstract class RustIterator<E extends RustObject> extends RustObject implements Iterator<E> {
    // We own this if it is not null!
    private Pointer nextPointer;

    RustIterator(Pointer pointer) {
        super(pointer);
    }

    /** Implement by calling `JNI.INSTANCE.whatever_iter_next(this.validPointer());` */
    abstract protected Pointer advanceIterator();
    // Generally should be implemented as `new E(p)`.
    abstract protected E constructItem(Pointer p);

    private Pointer consumeNextPointer() {
        if (this.nextPointer == null) {
            throw new NullPointerException(this.getClass() + " nextPointer already consumed");
        }
        Pointer p = this.nextPointer;
        this.nextPointer = null;
        return p;
    }

    @Override
    public boolean hasNext() {
        if (this.nextPointer != null) {
            return true;
        }
        this.nextPointer = advanceIterator();
        return this.nextPointer != null;
    }

    @Override
    public E next() {
        Pointer next = this.nextPointer == null ? advanceIterator() : this.consumeNextPointer();
        if (next == null) {
            throw new NoSuchElementException();
        }
        return this.constructItem(next);
    }

    @Override
    public void close() {
        if (this.nextPointer != null) {
            // clean up the next pointer by delegating to the iterated item -- this simplifies
            // iterator implementations, and keeps the cleanup code in one place.
            this.constructItem(this.consumeNextPointer());
        }
        super.close();
    }
}
