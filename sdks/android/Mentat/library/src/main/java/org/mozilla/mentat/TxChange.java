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
import com.sun.jna.Structure;

import java.util.Arrays;
import java.util.List;

/**
 * Represents a C struct representing changes that occured during a transaction.
 * These changes contain the transaction identifier, a {@link Pointer} to a list of affected attribute
 * Entids and the number of items that the list contains.
 */
public class TxChange extends Structure {
    public static class ByReference extends TxChange implements Structure.ByReference {
    }

    public static class ByValue extends TxChange implements Structure.ByValue {
    }

    public long txid;
    public Pointer changes;
    public long changes_len;

    /**
     * Get the affected attributes for this transaction
     * @return  The changes as a list of Entids of affected attributes
     */
    public List<Long> getChanges() {
        final long[] array = (long[]) changes.getLongArray(0, (int)changes_len);
        Long[] longArray = new Long[(int)changes_len];
        int idx = 0;
        for (long change: array) {
            longArray[idx++] = change;
        }
        return Arrays.asList(longArray);
    }

    @Override
    protected List<String> getFieldOrder() {
        return Arrays.asList("txid", "changes", "changes_len");
    }

    // Note: Rust has ownership of this data.
}
