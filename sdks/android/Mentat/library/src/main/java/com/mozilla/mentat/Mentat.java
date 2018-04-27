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

import com.sun.jna.Memory;
import com.sun.jna.Pointer;

/**
 * The primary class for accessing Mentat's API.<br/>
 * This class provides all of the basic API that can be found in Mentat's Store struct.<br/>
 * The raw pointer it holds is a pointer to a Store.
 */
public class Mentat extends RustObject {

    static {
        System.loadLibrary("mentat_ffi");
    }

    /**
     * Open a connection to a Store in a given location.<br/>
     * If the store does not already exist, one will be created.
     * @param dbPath    The URI as a String of the store to open.
     */
    public Mentat(String dbPath) {
        this.rawPointer = JNA.INSTANCE.store_open(dbPath);
    }

    /**
     * Open a connection to an in-memory Store.
     */
    public Mentat() {
        this.rawPointer = JNA.INSTANCE.store_open("");
    }

    /**
     * Create a new Mentat with the provided pointer to a Mentat Store
     * @param rawPointer    A pointer to a Mentat Store.
     */
    public Mentat(Pointer rawPointer) { this.rawPointer = rawPointer; }

    /**
     * Add an attribute to the cache. The {@link CacheDirection} determines how that attribute can be
     * looked up.
     *
     * TODO: Throw an exception if cache action fails
     *
     * @param attribute The attribute to cache
     * @param direction The direction the attribute should be keyed.
     * FORWARD caches values for an attribute keyed by entity
     * (i.e. find values and entities that have this attribute, or find values of attribute for an entity)
     * REVERSE caches entities for an attribute keyed by value.
     * (i.e. find entities that have a particular value for an attribute).
     * BOTH adds an attribute such that it is cached in both directions.
     */
    public void cache(String attribute, CacheDirection direction) {
        RustResult result = null;
        switch (direction) {
            case FORWARD:
                result = JNA.INSTANCE.store_cache_attribute_forward(this.rawPointer, attribute);
            case REVERSE:
                result = JNA.INSTANCE.store_cache_attribute_reverse(this.rawPointer, attribute);
            case BOTH:
                result = JNA.INSTANCE.store_cache_attribute_bi_directional(this.rawPointer, attribute);
        }
        if (result.isFailure()) {
            Log.e("Mentat", result.err);
        }
    }

    /**
     * Simple transact of an EDN string.
     * TODO: Throw an exception if the transact fails
     * @param transaction   The string, as EDN, to be transacted.
     * @return  The {@link TxReport} of the completed transaction
     */
    public TxReport transact(String transaction) {
        RustResult result = JNA.INSTANCE.store_transact(this.rawPointer, transaction);
        if (result.isFailure()) {
            Log.e("Mentat", result.err);
            return null;
        }

        if (result.isSuccess()) {
            return new TxReport(result.ok);
        } else {
            return null;
        }
    }

    /**
     * Get the the `Entid` of the attribute
     * @param attribute The string represeting the attribute whose `Entid` we are after. The string is represented as `:namespace/name`.
     * @return  The `Entid` associated with the attribute.
     */
    public long entIdForAttribute(String attribute) {
        return JNA.INSTANCE.store_entid_for_attribute(this.rawPointer, attribute);
    }

    /**
     * Start a query.
     * @param query The string represeting the the query to be executed.
     * @return  The {@link Query} representing the query that can be executed.
     */
    public Query query(String query) {
        return new Query(JNA.INSTANCE.store_query(this.rawPointer, query));
    }

    /**
     * Retrieve a single value of an attribute for an Entity
     * TODO: Throw an exception if the result contains an error.
     * @param attribute The string the attribute whose value is to be returned. The string is represented as `:namespace/name`.
     * @param entid The `Entid` of the entity we want the value from.
     * @return  The {@link TypedValue} containing the value of the attribute for the entity.
     */
    public TypedValue valueForAttributeOfEntity(String attribute, long entid) {
        RustResult result = JNA.INSTANCE.store_value_for_attribute(this.rawPointer, entid, attribute);

        if (result.isSuccess()) {
            return new TypedValue(result.ok);
        }

        if (result.isFailure()) {
            Log.e("Mentat", result.err);
        }

        return null;
    }

    /**
     * Register an callback and a set of attributes to observer for transaction observation.
     * The callback function is called when a transaction occurs in the `Store` that this `Mentat`
     * is connected to that affects the attributes that an observer has registered for.
     * @param key   `String` representing an identifier for the observer.
     * @param attributes    An array of Strings representing the attributes that the observer wishes
     *                     to be notified about if they are referenced in a transaction.
     * @param callback  the function to call when an observer notice is fired.
     */
    public void registerObserver(String key, String[] attributes, TxObserverCallback callback) {
        // turn string array into int array
        long[] attrEntids = new long[attributes.length];
        for(int i = 0; i < attributes.length; i++) {
            attrEntids[i] = JNA.INSTANCE.store_entid_for_attribute(this.rawPointer, attributes[i]);
        }
        final Pointer entidsNativeArray = new Memory(8 * attrEntids.length);
        entidsNativeArray.write(0, attrEntids, 0, attrEntids.length);
        JNA.INSTANCE.store_register_observer(rawPointer, key, entidsNativeArray, attrEntids.length, callback);
    }

    /**
     * Unregister the observer that was registered with the provided key such that it will no longer be called
     * if a transaction occurs that affects the attributes that the observer was registered to observe.
     * <p/>
     * The observer will need to re-register if it wants to start observing again.
     * @param key   String representing an identifier for the observer.
     */
    public void unregisterObserver(String key) {
        JNA.INSTANCE.store_unregister_observer(rawPointer, key);
    }


    /**
     * Start a new transaction
     *
     * TODO: Throw an exception if the result contains an error.
     *
     * @return The {@link InProgress} used to manage the transaction
     */
    public InProgress beginTransaction() {
        RustResult result = JNA.INSTANCE.store_begin_transaction(this.rawPointer);
        if (result.isSuccess()) {
            return new InProgress(result.ok);
        }

        if (result.isFailure()) {
            Log.i("Mentat", result.err);
        }

        return null;
    }

    /**
     * Creates a new transaction ({@link InProgress}) and returns an {@link InProgressBuilder} for
     * that transaction.
     *
     * TODO: Throw an exception if the result contains an error.
     *
     * @return  an {@link InProgressBuilder} for a new transaction.
     */
    public InProgressBuilder entityBuilder() {
        RustResult result = JNA.INSTANCE.store_in_progress_builder(this.rawPointer);
        if (result.isSuccess()) {
            return new InProgressBuilder(result.ok);
        }

        if (result.isFailure()) {
            Log.i("Mentat", result.err);
        }

        return null;
    }

    /**
     * Creates a new transaction ({@link InProgress}) and returns an {@link EntityBuilder} for the
     * entity with `entid` for that transaction.
     *
     * TODO: Throw an exception if the result contains an error.
     *
     * @param entid The `Entid` for this entity.
     * @return  an {@link EntityBuilder} for a new transaction.
     */
    public EntityBuilder entityBuilder(long entid) {
        RustResult result = JNA.INSTANCE.store_entity_builder_from_entid(this.rawPointer, entid);
        if (result.isSuccess()) {
            return new EntityBuilder(result.ok);
        }

        if (result.isFailure()) {
            Log.i("Mentat", result.err);
        }

        return null;
    }

    /**
     * Creates a new transaction ({@link InProgress}) and returns an {@link EntityBuilder} for a new
     * entity with `tempId` for that transaction.
     *
     * TODO: Throw an exception if the result contains an error.
     *
     * @param tempId    The temporary identifier for this entity.
     * @return  an {@link EntityBuilder} for a new transaction.
     */
    public EntityBuilder entityBuilder(String tempId) {
        RustResult result = JNA.INSTANCE.store_entity_builder_from_temp_id(this.rawPointer, tempId);
        if (result.isSuccess()) {
            return new EntityBuilder(result.ok);
        }

        if (result.isFailure()) {
            Log.i("Mentat", result.err);
        }

        return null;
    }

    @Override
    public void close() {
        if (this.rawPointer != null) {
            JNA.INSTANCE.store_destroy(this.rawPointer);
        }
    }
}
