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

import java.io.IOException;

/**
 * This class wraps a raw pointer that points to a Rust {@link InProgress} object.
 * </p>
 * {@link InProgress} allows for multiple transacts to be performed in a single transaction.
 * Each transact performed results in a {@link TxReport} that can be used to gather information
 * to be used in subsequent transacts.
 * </p>
 * Committing an {@link InProgress} commits all the transacts that have been performed using
 * that {@link InProgress}.
 * </p>
 * Rolling back and {@link InProgress} rolls back all the transacts that have been performed
 * using that {@link InProgress}.
 * </p>
 * <pre>{@code
 * do {
 * let inProgress = try mentat.beginTransaction()
 * let txReport = try inProgress.transact(transaction: "[[:db/add "a" :foo/long 22]]")
 * let aEntid = txReport.entid(forTempId: "a")
 * let report = try inProgress.transact(transaction: "[[:db/add "b" :foo/ref \(aEntid)] [:db/add "b" :foo/boolean true]]")
 * try inProgress.commit()
 * } catch {
 * ...
 * }
 * }</pre>
 * </p>
 * {@link InProgress} also provides a number of functions to generating an builder to assert datoms
 * programatically. The two types of builder are {@link InProgressBuilder} and {@link EntityBuilder}.
 * </p>
 * {@link InProgressBuilder} takes the current {@link InProgress} and provides a programmatic
 * interface to add and retract values from entities for which there exists an `Entid`. The provided
 * {@link InProgress} is used to perform the transacts.
 * </p>
 * <pre>{@code
 * long aEntid = txReport.getEntidForTempId("a");
 * long bEntid = txReport.getEntidForTempId("b");
 * InProgress inProgress = mentat.beginTransaction();
 * InProgressBuilder builder = inProgress.builder();
 * builder.add(bEntid, ":foo/boolean", true);
 * builder.add(aEntid, ":foo/instant", newDate);
 * inProgress  = builder.transact().getInProgress();
 * inProgress.transact("[[:db/add \(aEntid) :foo/long 22]]");
 * inProgress.commit();
 * }
 * }</pre>
 * </p>
 * {@link EntityBuilder} takes the current {@link InProgress} and either an `Entid` or a `tempid` to
 * provide a programmatic interface to add and retract values from a specific entity. The provided
 * {@link InProgress} is used to perform the transacts.
 * </p>
 * <pre>{@code
 * long aEntid = txReport.getEntidForTempId("a");
 * long bEntid = txReport.getEntidForTempId("b");
 * InProgress inProgress = mentat.beginTransaction();
 * EntityBuilder builder = inProgress.builderForEntid(bEntid);
 * builder.add(":foo/boolean", true);
 * builder.add(":foo/instant", newDate);
 * inProgress  = builder.transact().getInProgress();
 * inProgress.transact("[[:db/add \(aEntid) :foo/long 22]]");
 * inProgress.commit();
 * }</pre>
 */
public class InProgress extends RustObject {

    public InProgress(Pointer pointer) {
        this.rawPointer = pointer;
    }

    /**
     * Creates an {@link InProgressBuilder}  using this {@link InProgress} .
     *
     * @return an {@link InProgressBuilder} for this {@link InProgress}
     */
    public InProgressBuilder builder() {
        this.validate();
        InProgressBuilder builder = new InProgressBuilder(JNA.INSTANCE.in_progress_builder(this.rawPointer));
        this.rawPointer = null;
        return builder;
    }

    /**
     * Creates an `EntityBuilder` using this `InProgress` for the entity with `entid`.
     *
     *
     * @param entid The `Entid` for this entity.
     * @return  an `EntityBuilder` for this `InProgress`
     */
    public EntityBuilder builderForEntid(long entid){
        this.validate();
        EntityBuilder builder = new EntityBuilder(JNA.INSTANCE.in_progress_entity_builder_from_entid(this.rawPointer, entid));
        this.rawPointer = null;
        return builder;
    }

    /**
     * Creates an `EntityBuilder` using this `InProgress` for a new entity with `tempid`.
     *
     *
     * @param tempid The temporary identifier for this entity.
     * @return  an `EntityBuilder` for this `InProgress`
     */
    public EntityBuilder builderForTempid(String tempid){
        this.validate();
        EntityBuilder builder = new EntityBuilder(JNA.INSTANCE.in_progress_entity_builder_from_temp_id(this.rawPointer, tempid));
        this.rawPointer = null;
        return builder;
    }

    /**
     Transacts the `transaction`

     This does not commit the transaction. In order to do so, `commit` can be called.

     - Throws: `PointerError.pointerConsumed` if the underlying raw pointer has already consumed, which will occur if the builder
     has already been transacted or committed.
     - Throws: `ResultError.error` if the transaction failed.
     - Throws: `ResultError.empty` if no `TxReport` is returned from the transact.

     - Returns: The `TxReport` generated by the transact.
     */
    /**
     * Transacts the `transaction`
     *
     * This does not commit the transaction. In order to do so, `commit` can be called.
     *
     * TODO throw Exception on result failure.
     *
     * @param transaction   The EDN string to be transacted.
     * @return  The `TxReport` generated by the transact.
     */
    public TxReport transact(String transaction) {
        this.validate();
        RustResult result = JNA.INSTANCE.in_progress_transact(this.rawPointer, transaction);
        if (result.isFailure()) {
            Log.e("InProgress", result.err);
            return null;
        }
        return new TxReport(result.ok);
    }

    /**
     * Commits all the transacts that have been performed on this `InProgress`, either directly
     * or through a Builder. This consumes the pointer associated with this {@link InProgress} such
     * that no further assertions can be performed after the `commit` has completed.
     *
     * TODO throw exception if error occurs
     */
    public void commit() {
        this.validate();
        RustResult result = JNA.INSTANCE.in_progress_commit(this.rawPointer);
        this.rawPointer = null;
        if (result.isFailure()) {
            Log.e("InProgressBuilder", result.err);
        }
    }

    /**
     * Rolls back all the transacts that have been performed on this `InProgress`, either directly
     * or through a Builder.
     *
     * TODO throw exception if error occurs
     */
    public void rollback() {
        this.validate();
        RustResult result = JNA.INSTANCE.in_progress_rollback(this.rawPointer);
        this.rawPointer = null;
        if (result.isFailure()) {
            Log.e("InProgressBuilder", result.err);
        }
    }

    @Override
    public void close() throws IOException {
        if (this.rawPointer != null) {
            JNA.INSTANCE.in_progress_destroy(this.rawPointer);
        }
    }
}
