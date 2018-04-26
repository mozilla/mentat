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
import com.sun.jna.Structure;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class InProgressTransactionResult extends Structure implements Closeable {
    public static class ByReference extends InProgressTransactionResult implements Structure.ByReference {
    }

    public static class ByValue extends InProgressTransactionResult implements Structure.ByValue {
    }

    public Pointer inProgress;
    public RustResult.ByReference result;

    @Override
    protected List<String> getFieldOrder() {
        return Arrays.asList("inProgress", "result");
    }

    public InProgress getInProgress() {
        return new InProgress(this.inProgress);
    }
    public TxReport getReport() {
        if (this.result.isFailure()) {
            Log.e("InProgressTransactionResult", this.result.err);
            return null;
        }

        return new TxReport(this.result.ok);
    }

    @Override
    public void close() throws IOException {
        if (this.getPointer() != null) {
            JNA.INSTANCE.destroy(this.getPointer());
        }
    }
}
