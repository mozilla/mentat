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

import java.nio.ByteBuffer;
import java.util.Date;
import java.util.UUID;

/**
 * This class allows you to construct a query, bind values to variables and run those queries against a mentat DB.
 * <p/>
 * This class cannot be created directly, but must be created through `Mentat.query(String:)`.
 * <p/>
 *  The types of values you can bind are:
 * <ul>
 * <li>{@link TypedValue}</li>
 * <li>long</li>
 * <li>Entid (as long)</li>
 * <li>Keyword (as String)</li>
 * <li>boolean</li>
 * <li>double</li>
 * <li>{@link Date}</li>
 * <li>{@link String}</li>
 * <li>{@link UUID}</li>
 * </ul>
 * <p>
 * <p/>
 * Each bound variable must have a corresponding value in the query string used to create this query.
 * <p/>
 * <pre>{@code
 * String query = "[:find ?name ?cat\n" +
 *          "        :in ?type\n" +
 *          "        :where\n" +
 *          "        [?c :community/name ?name]\n" +
 *          "        [?c :community/type ?type]\n" +
 *          "        [?c :community/category ?cat]]";
 * mentat.query(query).bindKeywordReference("?type", ":community.type/website").run(new RelResultHandler() {
 *      @Override
 *      public void handleRows(RelResult rows) {
 *          ...
 *      }
 * });
 *}</pre>
 * <p/>
 * Queries can be run and the results returned in a number of different formats. Individual result values are returned as `TypedValues` and
 * the format differences relate to the number and structure of those values. The result format is related to the format provided in the query string.
 * <p/>
 * - `Rel` - This is the default `run` function and returns a list of rows of values. Queries that wish to have `Rel` results should format their query strings:
 *
 * <pre>{@code
 * String query = "[: find ?a ?b ?c\n" +
 *          "        : where ... ]";
 * mentat.query(query).run(new RelResultHandler() {
 *      @Override
 *      public void handleRows(RelResult rows) {
 *          ...
 *      }
 * });
 *}</pre>
 * <p/>
 * - `Scalar` - This returns a single value as a result. This can be optional, as the value may not be present. Queries that wish to have `Scalar` results should format their query strings:
 *
 * <pre>{@code
 * String query = "[: find ?a .\n" +
 *          "        : where ... ]";
 * mentat.query(query).runScalar(new ScalarResultHandler() {
 *      @Override
 *      public void handleValue(TypedValue value) {
 *          ...
 *      }
 * });
 *}</pre>
 * <p/>
 * - `Coll` - This returns a list of single values as a result.  Queries that wish to have `Coll` results should format their query strings:
 * <pre>{@code
 * String query = "[: find [?a ...]\n" +
 *          "        : where ... ]";
 * mentat.query(query).runColl(new ScalarResultHandler() {
 *      @Override
 *      public void handleList(CollResult list) {
 *          ...
 *      }
 * });
 *}</pre>
 * <p/>
 * - `Tuple` - This returns a single row of values.  Queries that wish to have `Tuple` results should format their query strings:
 * <pre>{@code
 * String query = "[: find [?a ?b ?c]\n" +
 *          "        : where ... ]";
 * mentat.query(query).runTuple(new TupleResultHandler() {
 *      @Override
 *      public void handleRow(TupleResult row) {
 *          ...
 *      }
 * });
 *}</pre>
 */
public class Query extends RustObject {

    public Query(Pointer pointer) {
        this.rawPointer = pointer;
    }

    /**
     * Binds a long value to the provided variable name.
     * TODO: Throw an exception if the query raw pointer has been consumed.
     * @param varName   The name of the variable in the format `?name`.
     * @param value The value to be bound
     * @return  This {@link Query} such that further function can be called.
     */
    Query bindLong(String varName, long value) {
        this.validate();
        JNA.INSTANCE.query_builder_bind_long(this.rawPointer, varName, value);
        return this;
    }

    /**
     * Binds a Entid value to the provided variable name.
     * TODO: Throw an exception if the query raw pointer has been consumed.
     * @param varName   The name of the variable in the format `?name`.
     * @param value The value to be bound
     * @return  This {@link Query} such that further function can be called.
     */
    Query bindEntidReference(String varName, long value) {
        this.validate();
        JNA.INSTANCE.query_builder_bind_ref(this.rawPointer, varName, value);
        return this;
    }

    /**
     * Binds a String keyword value to the provided variable name.
     * TODO: Throw an exception if the query raw pointer has been consumed.
     * @param varName   The name of the variable in the format `?name`.
     * @param value The value to be bound
     * @return  This {@link Query} such that further function can be called.
     */
    Query bindKeywordReference(String varName, String value) {
        this.validate();
        JNA.INSTANCE.query_builder_bind_ref_kw(this.rawPointer, varName, value);
        return this;
    }

    /**
     * Binds a keyword value to the provided variable name.
     * TODO: Throw an exception if the query raw pointer has been consumed.
     * @param varName   The name of the variable in the format `?name`.
     * @param value The value to be bound
     * @return  This {@link Query} such that further function can be called.
     */
    Query bindKeyword(String varName, String value) {
        this.validate();
        JNA.INSTANCE.query_builder_bind_kw(this.rawPointer, varName, value);
        return this;
    }

    /**
     * Binds a boolean value to the provided variable name.
     * TODO: Throw an exception if the query raw pointer has been consumed.
     * @param varName   The name of the variable in the format `?name`.
     * @param value The value to be bound
     * @return  This {@link Query} such that further function can be called.
     */
    Query bindBoolean(String varName, boolean value) {
        this.validate();
        JNA.INSTANCE.query_builder_bind_boolean(this.rawPointer, varName, value ? 1 : 0);
        return this;
    }

    /**
     * Binds a double value to the provided variable name.
     * TODO: Throw an exception if the query raw pointer has been consumed.
     * @param varName   The name of the variable in the format `?name`.
     * @param value The value to be bound
     * @return  This {@link Query} such that further function can be called.
     */
    Query bindDouble(String varName, double value) {
        this.validate();
        JNA.INSTANCE.query_builder_bind_double(this.rawPointer, varName, value);
        return this;
    }

    /**
     * Binds a {@link Date} value to the provided variable name.
     * TODO: Throw an exception if the query raw pointer has been consumed.
     * @param varName   The name of the variable in the format `?name`.
     * @param value The value to be bound
     * @return  This {@link Query} such that further function can be called.
     */
    Query bindDate(String varName, Date value) {
        this.validate();
        long timestamp = value.getTime() * 1000;
        JNA.INSTANCE.query_builder_bind_timestamp(this.rawPointer, varName, timestamp);
        return this;
    }

    /**
     * Binds a {@link String} value to the provided variable name.
     * TODO: Throw an exception if the query raw pointer has been consumed.
     * @param varName   The name of the variable in the format `?name`.
     * @param value The value to be bound
     * @return  This {@link Query} such that further function can be called.
     */
    Query bindString(String varName, String value) {
        this.validate();
        JNA.INSTANCE.query_builder_bind_string(this.rawPointer, varName, value);
        return this;
    }

    /**
     * Binds a {@link UUID} value to the provided variable name.
     * TODO: Throw an exception if the query raw pointer has been consumed.
     * @param varName   The name of the variable in the format `?name`.
     * @param value The value to be bound
     * @return  This {@link Query} such that further function can be called.
     */
    Query bindUUID(String varName, UUID value) {
        this.validate();
        ByteBuffer bb = ByteBuffer.wrap(new byte[16]);
        bb.putLong(value.getMostSignificantBits());
        bb.putLong(value.getLeastSignificantBits());
        byte[] bytes = bb.array();
        final Pointer bytesNativeArray = new Memory(bytes.length);
        bytesNativeArray.write(0, bytes, 0, bytes.length);
        JNA.INSTANCE.query_builder_bind_uuid(this.rawPointer, varName, bytesNativeArray);
        return this;
    }

    /**
     * Execute the query with the values bound associated with this {@link Query} and call the provided
     * callback function with the results as a list of rows of {@link TypedValue}s.
     * TODO: Throw an exception if the query raw pointer has been consumed or the query fails to execute
     * @param handler   the handler to call with the results of this query
     */
    void run(final RelResultHandler handler) {
        this.validate();
        RustResult result = JNA.INSTANCE.query_builder_execute(rawPointer);
        rawPointer = null;

        if (result.isFailure()) {
            Log.e("Query", result.err);
            return;
        }
        handler.handleRows(new RelResult(result.ok));
    }

    /**
     * Execute the query with the values bound associated with this {@link Query} and call the provided
     * callback function with the results with the result as a single {@link TypedValue}.
     * TODO: Throw an exception if the query raw pointer has been consumed or the query fails to execute
     * @param handler   the handler to call with the results of this query
     */
    void runScalar(final ScalarResultHandler handler) {
        this.validate();
        RustResult result = JNA.INSTANCE.query_builder_execute_scalar(rawPointer);
        rawPointer = null;

        if (result.isFailure()) {
            Log.e("Query", result.err);
            return;
        }

        if (result.isSuccess()) {
            handler.handleValue(new TypedValue(result.ok));
        } else {
            handler.handleValue(null);
        }
    }

    /**
     * Execute the query with the values bound associated with this {@link Query} and call the provided
     * callback function with the results with the result as a list of single {@link TypedValue}s.
     * TODO: Throw an exception if the query raw pointer has been consumed or the query fails to execute
     * @param handler   the handler to call with the results of this query
     */
    void runColl(final CollResultHandler handler) {
        this.validate();
        RustResult result = JNA.INSTANCE.query_builder_execute_coll(rawPointer);
        rawPointer = null;

        if (result.isFailure()) {
            Log.e("Query", result.err);
            return;
        }
        handler.handleList(new CollResult(result.ok));
    }

    /**
     * Execute the query with the values bound associated with this {@link Query} and call the provided
     * callback function with the results with the result as a list of single {@link TypedValue}s.
     * TODO: Throw an exception if the query raw pointer has been consumed or the query fails to execute
     * @param handler   the handler to call with the results of this query
     */
    void runTuple(final TupleResultHandler handler) {
        this.validate();
        RustResult result = JNA.INSTANCE.query_builder_execute_tuple(rawPointer);
        rawPointer = null;

        if (result.isFailure()) {
            Log.e("Query", result.err);
            return;
        }

        if (result.isSuccess()) {
            handler.handleRow(new TupleResult(result.ok));
        } else {
            handler.handleRow(null);
        }
    }

    @Override
    public void close() {
        Log.i("Query", "close");

        if (this.rawPointer == null) {
            return;
        }
        JNA.INSTANCE.query_builder_destroy(this.rawPointer);
    }
}
