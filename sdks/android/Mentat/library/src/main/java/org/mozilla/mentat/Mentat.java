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

import com.sun.jna.Memory;
import com.sun.jna.Pointer;

/**
 * The primary class for accessing Mentat's API.<br/>
 * This class provides all of the basic API that can be found in Mentat's Store struct.<br/>
 * The raw pointer it holds is a pointer to a Store.
 */
public class Mentat extends RustObject<JNA.Store> {
    /**
     * Create a new Mentat with the provided pointer to a Mentat Store
     * @param rawPointer    A pointer to a Mentat Store.
     */
    private Mentat(JNA.Store rawPointer) { super(rawPointer); }

    /**
     * Open a connection to an in-memory Mentat Store.
     */
    public static Mentat open() {
        return open("");
    }

   /**
    * Open a connection to a Store in a given location.
    * <br/>
    * If the store does not already exist, one will be created.
    * @param dbPath    The URI as a String of the store to open.
    */
    public static Mentat open(String dbPath) {
        RustError.ByReference err = new RustError.ByReference();
        JNA.Store store = JNA.INSTANCE.store_open(dbPath, err);
        if (!err.isSuccess()) {
            err.logAndConsumeError("Mentat");
            throw new RuntimeException("Failed to open store: " + dbPath);
        }

        return new Mentat(store);
    }

    /**
     * Add an attribute to the cache. The {@link CacheDirection} determines how that attribute can be
     * looked up.
     *
     * TODO: Throw an exception if cache action fails. https://github.com/mozilla/mentat/issues/700
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
        RustError.ByReference err = new RustError.ByReference();
        switch (direction) {
            case FORWARD:
                JNA.INSTANCE.store_cache_attribute_forward(this.validPointer(), attribute, err);
                break;
            case REVERSE:
                JNA.INSTANCE.store_cache_attribute_reverse(this.validPointer(), attribute, err);
                break;
            case BOTH:
                JNA.INSTANCE.store_cache_attribute_bi_directional(this.validPointer(), attribute, err);
                break;
        }
        err.logAndConsumeError("Mentat");
    }

    /**
     * Simple transact of an EDN string.
     * TODO: Throw an exception if the transact fails. https://github.com/mozilla/mentat/issues/700
     * @param transaction   The string, as EDN, to be transacted.
     * @return  The {@link TxReport} of the completed transaction
     */
    public TxReport transact(String transaction) {
        RustError.ByReference err = new RustError.ByReference();
        JNA.TxReport report = JNA.INSTANCE.store_transact(this.validPointer(), transaction, err);
        if (err.isSuccess()) {
            return new TxReport(report);
        } else {
            err.logAndConsumeError("Mentat");
            return null;
        }
    }

    /**
     * Get the the `Entid` of the attribute
     * @param attribute The string represeting the attribute whose `Entid` we are after. The string is represented as `:namespace/name`.
     * @return  The `Entid` associated with the attribute.
     */
    public long entIdForAttribute(String attribute) {
        return JNA.INSTANCE.store_entid_for_attribute(this.validPointer(), attribute);
    }

    /**
     * Start a query.
     * @param query The string represeting the the query to be executed.
     * @return  The {@link Query} representing the query that can be executed.
     */
    public Query query(String query) {
        return new Query(JNA.INSTANCE.store_query(this.validPointer(), query));
    }

    /**
     * Retrieve a single value of an attribute for an Entity
     * TODO: Throw an exception if the result contains an error. https://github.com/mozilla/mentat/issues/700
     * @param attribute The string the attribute whose value is to be returned. The string is represented as `:namespace/name`.
     * @param entid The `Entid` of the entity we want the value from.
     * @return  The {@link TypedValue} containing the value of the attribute for the entity.
     */
    public TypedValue valueForAttributeOfEntity(String attribute, long entid) {
        RustError.ByReference err = new RustError.ByReference();
        JNA.TypedValue typedVal = JNA.INSTANCE.store_value_for_attribute(this.validPointer(), entid, attribute, err);

        if (err.isSuccess()) {
            return new TypedValue(typedVal);
        }

        err.logAndConsumeError("Mentat");
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
            attrEntids[i] = JNA.INSTANCE.store_entid_for_attribute(this.validPointer(), attributes[i]);
        }
        final Pointer entidsNativeArray = new Memory(8 * attrEntids.length);
        entidsNativeArray.write(0, attrEntids, 0, attrEntids.length);
        JNA.INSTANCE.store_register_observer(validPointer(), key, entidsNativeArray, attrEntids.length, callback);
    }

    /**
     * Unregister the observer that was registered with the provided key such that it will no longer be called
     * if a transaction occurs that affects the attributes that the observer was registered to observe.
     * <p/>
     * The observer will need to re-register if it wants to start observing again.
     * @param key   String representing an identifier for the observer.
     */
    public void unregisterObserver(String key) {
        JNA.INSTANCE.store_unregister_observer(validPointer(), key);
    }

    /**
     * Start a new transaction
     *
     * TODO: Throw an exception if the result contains an error. https://github.com/mozilla/mentat/issues/700
     *
     * @return The {@link InProgress} used to manage the transaction
     */
    public InProgress beginTransaction() {
        RustError.ByReference err = new RustError.ByReference();
        JNA.InProgress inProg = JNA.INSTANCE.store_begin_transaction(this.validPointer(), err);
        if (err.isSuccess()) {
            return new InProgress(inProg);
        }
        err.logAndConsumeError("Mentat");
        return null;
    }

    /**
     * Creates a new transaction ({@link InProgress}) and returns an {@link InProgressBuilder} for
     * that transaction.
     *
     * TODO: Throw an exception if the result contains an error. https://github.com/mozilla/mentat/issues/700
     *
     * @return  an {@link InProgressBuilder} for a new transaction.
     */
    public InProgressBuilder entityBuilder() {
        RustError.ByReference err = new RustError.ByReference();
        JNA.InProgressBuilder builder = JNA.INSTANCE.store_in_progress_builder(this.validPointer(), err);
        if (err.isSuccess()) {
            return new InProgressBuilder(builder);
        }
        err.logAndConsumeError("Mentat");
        return null;
    }

    /**
     * Creates a new transaction ({@link InProgress}) and returns an {@link EntityBuilder} for the
     * entity with `entid` for that transaction.
     *
     * TODO: Throw an exception if the result contains an error. https://github.com/mozilla/mentat/issues/700
     *
     * @param entid The `Entid` for this entity.
     * @return  an {@link EntityBuilder} for a new transaction.
     */
    public EntityBuilder entityBuilder(long entid) {
        RustError.ByReference err = new RustError.ByReference();
        JNA.EntityBuilder builder = JNA.INSTANCE.store_entity_builder_from_entid(this.validPointer(), entid, err);
        if (err.isSuccess()) {
            return new EntityBuilder(builder);
        }
        err.logAndConsumeError("Mentat");
        return null;
    }

    /**
     * Creates a new transaction ({@link InProgress}) and returns an {@link EntityBuilder} for a new
     * entity with `tempId` for that transaction.
     *
     * TODO: Throw an exception if the result contains an error. https://github.com/mozilla/mentat/issues/700
     *
     * @param tempId    The temporary identifier for this entity.
     * @return  an {@link EntityBuilder} for a new transaction.
     */
    public EntityBuilder entityBuilder(String tempId) {
        RustError.ByReference err = new RustError.ByReference();
        JNA.EntityBuilder builder = JNA.INSTANCE.store_entity_builder_from_temp_id(this.validPointer(), tempId, err);
        if (err.isSuccess()) {
            return new EntityBuilder(builder);
        }
        err.logAndConsumeError("Mentat");
        return null;
    }

    @Override
    protected void destroyPointer(JNA.Store p) {
        JNA.INSTANCE.store_destroy(p);
    }
}
