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
public class RustError extends Structure {
    public static class ByReference extends RustError implements Structure.ByReference {
    }

    public static class ByValue extends RustError implements Structure.ByValue {
    }
    // It's probably a mistake to touch this, but it needs to be public for JNA
    public Pointer message;

    /**
     * Does this represent success?
     */
    public boolean isSuccess() {
        return this.message == null;
    }

    /**
     * Does this represent failure?
     */
    public boolean isFailure() {
        return this.message != null;
    }

    /**
     * Get and consume the error message, or null if there is none.
     */
    public String consumeErrorMessage() {
        String result = this.getErrorMessage();
        if (this.message != null) {
            JNA.INSTANCE.destroy_mentat_string(this.message);
            this.message = null;
        }
        return result;
    }

    /**
     * Get the error message or null if there is none.
     */
    public String getErrorMessage() {
        return this.message == null ? null : this.message.getString(0, "utf8");
    }

    @Override
    protected List<String> getFieldOrder() {
        return Arrays.asList("message");
    }

    @Override
    protected void finalize() {
        if (this.message != null) {
            JNA.INSTANCE.destroy_mentat_string(this.message);
            this.message = null;
        }
    }

    /* package-local */
    void logAndConsumeError(String tag) {
        if (this.isFailure()) {
            Log.e(tag, this.consumeErrorMessage());
        }
    }
}