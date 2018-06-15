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

/**
 * This class wraps a raw pointer than points to a Rust `TxReport` object.
 * </p>
 * The `TxReport` contains information about a successful Mentat transaction.
 * </p>
 * This information includes:
 * <ul>
 *     <li>`txId` - the identifier for the transaction.</li>
 *     <li>`txInstant` - the time that the transaction occured.</li>
 *     <li>a map of temporary identifiers provided in the transaction and the `Entid`s that they were mapped to.</li>
 * </ul>
 * </p>
 * Access an `Entid` for a temporary identifier that was provided in the transaction can be done through `entid(String:)`.
 * </p>
 * <pre>{@code
 * TxReport report = mentat.transact("[[:db/add "a" :foo/boolean true]]");
 * long aEntid = report.getEntidForTempId("a");
 *}</pre>
 */
public class TxReport extends RustObject {

    private Long txId;
    private Date txInstant;


    public TxReport(Pointer pointer) {
        super(pointer);
    }

    /**
     * Get the identifier for the transaction.
     * @return  The identifier for the transaction.
     */
    public Long getTxId() {
        if (this.txId == null) {
            this.txId = JNA.INSTANCE.tx_report_get_entid(this.rawPointer);
        }

        return this.txId;
    }

    /**
     * Get the time that the transaction occured.
     * @return  The time that the transaction occured.
     */
    public Date getTxInstant() {
        if (this.txInstant == null) {
            this.txInstant = new Date(JNA.INSTANCE.tx_report_get_tx_instant(this.rawPointer));
        }
        return this.txInstant;
    }

    /**
     * Access an `Entid` for a temporary identifier that was provided in the transaction.
     * @param tempId    A {@link String} representing the temporary identifier to fetch the `Entid` for.
     * @return  The `Entid` for the temporary identifier, if present, otherwise `null`.
     */
    public Long getEntidForTempId(String tempId) {
        Pointer longPointer =  JNA.INSTANCE.tx_report_entity_for_temp_id(this.rawPointer, tempId);
        if (longPointer == null) {
            return null;
        }
        try {
            return longPointer.getLong(0);
        } finally {
            JNA.INSTANCE.destroy(longPointer);
        }
    }

    @Override
    protected void destroyPointer(Pointer p) {
        JNA.INSTANCE.tx_report_destroy(p);
    }
}
