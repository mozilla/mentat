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

import com.sun.jna.Library;
import com.sun.jna.Native;
import com.sun.jna.NativeLibrary;
import com.sun.jna.Pointer;

/**
 * JNA interface for FFI to Mentat's Rust library
 * Each function definition here link directly to a function in Mentat's FFI crate.
 * Signatures must match for the linking to work correctly.
 */
public interface JNA extends Library {
    String JNA_LIBRARY_NAME = "mentat_ffi";
    NativeLibrary JNA_NATIVE_LIB = NativeLibrary.getInstance(JNA_LIBRARY_NAME);

    JNA INSTANCE = (JNA) Native.loadLibrary(JNA_LIBRARY_NAME, JNA.class);

    Pointer store_open(String dbPath);

    void destroy(Pointer obj);
    void query_builder_destroy(Pointer obj);
    void store_destroy(Pointer obj);
    void typed_value_destroy(Pointer obj);
    void typed_value_list_destroy(Pointer obj);
    void typed_value_list_iter_destroy(Pointer obj);
    void typed_value_result_set_destroy(Pointer obj);
    void typed_value_result_set_iter_destroy(Pointer obj);
    void tx_report_destroy(Pointer obj);

    // transact
    RustResult store_transact(Pointer store, String transaction);
    Pointer tx_report_entity_for_temp_id(Pointer report, String tempid);
    long tx_report_get_entid(Pointer report);
    long tx_report_get_tx_instant(Pointer report);

    // sync
    RustResult store_sync(Pointer store, String userUuid, String serverUri);

    // observers
    void store_register_observer(Pointer store, String key, Pointer attributes, int len, TxObserverCallback callback);
    void store_unregister_observer(Pointer store, String key);
    long store_entid_for_attribute(Pointer store, String attr);

    // Query Building
    Pointer store_query(Pointer store, String query);
    RustResult store_value_for_attribute(Pointer store, long entid, String attribute);
    void query_builder_bind_long(Pointer query, String var, long value);
    void query_builder_bind_ref(Pointer query, String var, long value);
    void query_builder_bind_ref_kw(Pointer query, String var, String value);
    void query_builder_bind_kw(Pointer query, String var, String value);
    void query_builder_bind_boolean(Pointer query, String var, int value);
    void query_builder_bind_double(Pointer query, String var, double value);
    void query_builder_bind_timestamp(Pointer query, String var, long value);
    void query_builder_bind_string(Pointer query, String var, String value);
    void query_builder_bind_uuid(Pointer query, String var, Pointer value);

    // Query Execution
    RustResult query_builder_execute(Pointer query);
    RustResult query_builder_execute_scalar(Pointer query);
    RustResult query_builder_execute_coll(Pointer query);
    RustResult query_builder_execute_tuple(Pointer query);

    // Query Result Processing
    long typed_value_as_long(Pointer value);
    long typed_value_as_entid(Pointer value);
    String typed_value_as_kw(Pointer value);
    String typed_value_as_string(Pointer value);
    Pointer typed_value_as_uuid(Pointer value);
    int typed_value_as_boolean(Pointer value);
    double typed_value_as_double(Pointer value);
    long typed_value_as_timestamp(Pointer value);
    Pointer typed_value_value_type(Pointer value);

    Pointer row_at_index(Pointer rows, int index);
    Pointer rows_iter(Pointer rows);
    Pointer rows_iter_next(Pointer iter);

    Pointer values_iter(Pointer rows);
    Pointer values_iter_next(Pointer iter);

    Pointer value_at_index(Pointer rows, int index);
    long value_at_index_as_long(Pointer rows, int index);
    long value_at_index_as_entid(Pointer rows, int index);
    String value_at_index_as_kw(Pointer rows, int index);
    String value_at_index_as_string(Pointer rows, int index);
    Pointer value_at_index_as_uuid(Pointer rows, int index);
    long value_at_index_as_boolean(Pointer rows, int index);
    double value_at_index_as_double(Pointer rows, int index);
    long value_at_index_as_timestamp(Pointer rows, int index);
}
