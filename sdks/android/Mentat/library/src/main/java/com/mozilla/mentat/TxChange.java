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
import com.sun.jna.Structure;

import java.io.Closeable;
import java.util.Arrays;
import java.util.List;

/**
 * Represents a C struct representing changes that occured during a transaction.
 * These changes contain the transaction identifier, a {@link Pointer} to a list of affected attribute
 * Entids and the number of items that the list contains.
 */
public class TxChange extends Structure implements Closeable {
    public static class ByReference extends TxChange implements Structure.ByReference {
    }

    public static class ByValue extends TxChange implements Structure.ByValue {
    }

    public int txid;
    public Pointer changes;
    public int numberOfItems;
    // Used by the Swift counterpart, JNA does this for us automagically.
    // But we still need it here so that the number of fields and their order is correct
    public int changes_len;

    /**
     * Get the affected attributes for this transaction
     * @return  The changes as a list of Entids of affected attributes
     */
    public List<Long> getChanges() {
        final long[] array = (long[]) changes.getLongArray(0, numberOfItems);
        Long[] longArray = new Long[numberOfItems];
        int idx = 0;
        for (long change: array) {
            longArray[idx++] = change;
        }
        return Arrays.asList(longArray);
    }

    @Override
    protected List<String> getFieldOrder() {
        return Arrays.asList("txid", "changes", "changes_len", "numberOfItems");
    }

    @Override
    public void close() {
        if (this.getPointer() != null) {
            JNA.INSTANCE.destroy(this.getPointer());
        }
    }
}
