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

import com.sun.jna.Memory;
import com.sun.jna.Pointer;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.UUID;

/**
 * Base class that wraps an non-optional {@link Pointer} representing a pointer to a Rust object.
 * This class implements {@link Closeable} but does not provide an implementation, forcing all
 * subclasses to implement it. This ensures that all classes that inherit from RustObject
 * will have their {@link Pointer} destroyed when the Java wrapper is destroyed.
 */
abstract class RustObject implements Closeable {
    Pointer rawPointer;

    /**
     * Throws a {@link NullPointerException} if the underlying {@link Pointer} is null.
     */
    void validate() {
        if (this.rawPointer == null) {
            throw new NullPointerException(this.getClass() + " consumed");
        }
    }

    public Pointer getPointerForUUID(UUID uuid) {
        ByteBuffer bb = ByteBuffer.wrap(new byte[16]);
        bb.putLong(uuid.getMostSignificantBits());
        bb.putLong(uuid.getLeastSignificantBits());
        byte[] bytes = bb.array();
        final Pointer bytesNativeArray = new Memory(bytes.length);
        bytesNativeArray.write(0, bytes, 0, bytes.length);
        return bytesNativeArray;
    }

    public UUID getUUIDFromPointer(Pointer uuidPtr) {
        byte[] bytes = uuidPtr.getByteArray(0, 16);
        ByteBuffer bb = ByteBuffer.wrap(bytes);
        long high = bb.getLong();
        long low = bb.getLong();
        return new UUID(high, low);
    }
}
