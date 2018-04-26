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
    void in_progress_destroy(Pointer obj);
    void in_progress_builder_destroy(Pointer obj);
    void entity_builder_destroy(Pointer obj);

    // transact
    RustResult store_transact(Pointer store, String transaction);
    Pointer tx_report_entity_for_temp_id(Pointer report, String tempid);
    long tx_report_get_entid(Pointer report);
    long tx_report_get_tx_instant(Pointer report);
    RustResult store_begin_transaction(Pointer store);

    // in progress
    RustResult in_progress_transact(Pointer in_progress, String transaction);
    RustResult in_progress_commit(Pointer in_progress);
    RustResult in_progress_rollback(Pointer in_progress);
    Pointer in_progress_builder(Pointer in_progress);
    Pointer in_progress_entity_builder_from_temp_id(Pointer in_progress, String temp_id);
    Pointer in_progress_entity_builder_from_entid(Pointer in_progress, long entid);

    // in_progress entity building
    RustResult store_in_progress_builder(Pointer store);
    RustResult in_progress_builder_add_string(Pointer builder, long entid, String kw, String value);
    RustResult in_progress_builder_add_long(Pointer builder, long entid, String kw, long value);
    RustResult in_progress_builder_add_ref(Pointer builder, long entid, String kw, long value);
    RustResult in_progress_builder_add_keyword(Pointer builder, long entid, String kw, String value);
    RustResult in_progress_builder_add_timestamp(Pointer builder, long entid, String kw, long value);
    RustResult in_progress_builder_add_boolean(Pointer builder, long entid, String kw, int value);
    RustResult in_progress_builder_add_double(Pointer builder, long entid, String kw, double value);
    RustResult in_progress_builder_add_uuid(Pointer builder, long entid, String kw, Pointer value);
    RustResult in_progress_builder_retract_string(Pointer builder, long entid, String kw, String value);
    RustResult in_progress_builder_retract_long(Pointer builder, long entid, String kw, long value);
    RustResult in_progress_builder_retract_ref(Pointer builder, long entid, String kw, long value);
    RustResult in_progress_builder_retract_keyword(Pointer builder, long entid, String kw, String value);
    RustResult in_progress_builder_retract_timestamp(Pointer builder, long entid, String kw, long value);
    RustResult in_progress_builder_retract_boolean(Pointer builder, long entid, String kw, int value);
    RustResult in_progress_builder_retract_double(Pointer builder, long entid, String kw, double value);
    RustResult in_progress_builder_retract_uuid(Pointer builder, long entid, String kw, Pointer value);
    InProgressTransactionResult in_progress_builder_transact(Pointer builder);
    RustResult in_progress_builder_commit(Pointer builder);

    // entity building
    RustResult store_entity_builder_from_temp_id(Pointer store, String temp_id);
    RustResult store_entity_builder_from_entid(Pointer store, long entid);
    RustResult entity_builder_add_string(Pointer builder, String kw, String value);
    RustResult entity_builder_add_long(Pointer builder, String kw, long value);
    RustResult entity_builder_add_ref(Pointer builder, String kw, long value);
    RustResult entity_builder_add_keyword(Pointer builder, String kw, String value);
    RustResult entity_builder_add_boolean(Pointer builder, String kw, int value);
    RustResult entity_builder_add_double(Pointer builder, String kw, double value);
    RustResult entity_builder_add_timestamp(Pointer builder, String kw, long value);
    RustResult entity_builder_add_uuid(Pointer builder, String kw, Pointer value);
    RustResult entity_builder_retract_string(Pointer builder, String kw, String value);
    RustResult entity_builder_retract_long(Pointer builder, String kw, long value);
    RustResult entity_builder_retract_ref(Pointer builder, String kw, long value);
    RustResult entity_builder_retract_keyword(Pointer builder, String kw, String value);
    RustResult entity_builder_retract_boolean(Pointer builder, String kw, int value);
    RustResult entity_builder_retract_double(Pointer builder, String kw, double value);
    RustResult entity_builder_retract_timestamp(Pointer builder, String kw, long value);
    RustResult entity_builder_retract_uuid(Pointer builder, String kw, Pointer value);
    InProgressTransactionResult entity_builder_transact(Pointer builder);
    RustResult entity_builder_commit(Pointer builder);

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
    long typed_value_into_long(Pointer value);
    long typed_value_into_entid(Pointer value);
    String typed_value_into_kw(Pointer value);
    String typed_value_into_string(Pointer value);
    Pointer typed_value_into_uuid(Pointer value);
    int typed_value_into_boolean(Pointer value);
    double typed_value_into_double(Pointer value);
    long typed_value_into_timestamp(Pointer value);
    Pointer typed_value_value_type(Pointer value);

    Pointer row_at_index(Pointer rows, int index);
    Pointer typed_value_result_set_into_iter(Pointer rows);
    Pointer typed_value_result_set_iter_next(Pointer iter);

    Pointer typed_value_list_into_iter(Pointer rows);
    Pointer typed_value_list_iter_next(Pointer iter);

    Pointer value_at_index(Pointer rows, int index);
    long value_at_index_into_long(Pointer rows, int index);
    long value_at_index_into_entid(Pointer rows, int index);
    String value_at_index_into_kw(Pointer rows, int index);
    String value_at_index_into_string(Pointer rows, int index);
    Pointer value_at_index_into_uuid(Pointer rows, int index);
    int value_at_index_into_boolean(Pointer rows, int index);
    double value_at_index_into_double(Pointer rows, int index);
    long value_at_index_into_timestamp(Pointer rows, int index);
}
