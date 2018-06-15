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

import android.util.Log;

import com.sun.jna.Pointer;
import com.sun.jna.Structure;

import java.util.Arrays;
import java.util.List;

/**
 * Represents a C struct containing a {@link Pointer}s and String that map to a Rust Result.
 * A RustResult will contain either an ok value, OR an err value, or neither - never both.
 */
public class RustResult extends Structure {
    public static class ByReference extends RustResult implements Structure.ByReference {
    }

    public static class ByValue extends RustResult implements Structure.ByValue {
    }
    // It's probably a mistake to touch these, but they need to be public for JNA. Use getError and takeSuccess instead!!
    public Pointer ok;
    public Pointer err;

    /**
     * Is there an value attached to this result
     * @return  true if a value is present, false otherwise
     */
    public boolean isSuccess() {
        return this.ok != null;
    }

    /**
     * Is there an error attached to this result?
     * @return  true is an error is present, false otherwise
     */
    public boolean isFailure() {
        return this.err != null;
    }

    /**
     * Get the error attached to this result, or null if there is none.
     */
    public String getError() {
        return this.err == null ? null : this.err.getString(0, "utf8");
    }

    /* package-local */
    void logIfFailure(String tag) {
        if (this.isFailure()) {
            Log.e(tag, this.getError());
        }
    }

    public Pointer consumeSuccess() {
        if (this.ok == null) {
            throw new RuntimeException("consumeSuccess called on failed Result!");
        }
        Pointer result = this.ok;
        this.ok = null;
        return result;
    }

    @Override
    protected List<String> getFieldOrder() {
        return Arrays.asList("ok", "err");
    }

    @Override
    protected void finalize() {
        if (this.err != null) {
            JNA.INSTANCE.destroy_mentat_string(this.err);
            this.err = null;
        }
        if (this.ok != null) {
            Log.w("RustResult", "RustResult.ok is still present during finalization, leaking memory!");
        }
        JNA.INSTANCE.destroy(this.getPointer());
    }
}