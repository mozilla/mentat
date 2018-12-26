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

import com.sun.jna.Library;
import com.sun.jna.Native;
import com.sun.jna.NativeLibrary;
import com.sun.jna.Pointer;
import com.sun.jna.PointerType;

import java.lang.reflect.Type;

/**
 * JNA interface for FFI to Mentat's Rust library
 * Each function definition here link directly to a function in Mentat's FFI crate.
 * Signatures must match for the linking to work correctly.
 */
public interface JNA extends Library {
    String JNA_LIBRARY_NAME = "mentat_ffi";
    NativeLibrary JNA_NATIVE_LIB = NativeLibrary.getInstance(JNA_LIBRARY_NAME);

    JNA INSTANCE = (JNA) Native.loadLibrary(JNA_LIBRARY_NAME, JNA.class);


    class Store extends PointerType {}
    class QueryBuilder extends PointerType {}
    class TypedValue extends PointerType {}
    class TypedValueList extends PointerType {}
    class TypedValueListIter extends PointerType {}
    class RelResult extends PointerType {}
    class RelResultIter extends PointerType {}
    class TxReport extends PointerType {}
    class InProgress extends PointerType {}
    class InProgressBuilder extends PointerType {}
    class EntityBuilder extends PointerType {}

    Store store_open(String dbPath, RustError.ByReference err);

    void destroy(Pointer obj);
    void uuid_destroy(Pointer obj);
    void query_builder_destroy(QueryBuilder obj);
    void store_destroy(Store obj);
    void typed_value_destroy(TypedValue obj);
    void typed_value_list_destroy(TypedValueList obj);
    void typed_value_list_iter_destroy(TypedValueListIter obj);
    void typed_value_result_set_destroy(RelResult obj);
    void typed_value_result_set_iter_destroy(RelResultIter obj);
    void tx_report_destroy(TxReport obj);
    void in_progress_destroy(InProgress obj);
    void in_progress_builder_destroy(InProgressBuilder obj);
    void entity_builder_destroy(EntityBuilder obj);
    void rust_c_string_destroy(Pointer str);

    // caching
    void store_cache_attribute_forward(Store store, String attribute, RustError.ByReference err);
    void store_cache_attribute_reverse(Store store, String attribute, RustError.ByReference err);
    void store_cache_attribute_bi_directional(Store store, String attribute, RustError.ByReference err);

    // transact
    TxReport store_transact(Store store, String transaction, RustError.ByReference err);
    Pointer tx_report_entity_for_temp_id(TxReport report, String tempid); // returns a pointer to a 64 bit int on the heap
    long tx_report_get_entid(TxReport report);
    long tx_report_get_tx_instant(TxReport report);
    InProgress store_begin_transaction(Store store, RustError.ByReference error);

    // in progress
    TxReport in_progress_transact(InProgress in_progress, String transaction, RustError.ByReference err);
    void in_progress_commit(InProgress in_progress, RustError.ByReference err);
    void in_progress_rollback(InProgress in_progress, RustError.ByReference err);
    InProgressBuilder in_progress_builder(InProgress in_progress);
    EntityBuilder in_progress_entity_builder_from_temp_id(InProgress in_progress, String temp_id);
    EntityBuilder in_progress_entity_builder_from_entid(InProgress in_progress, long entid);

    // in_progress entity building
    InProgressBuilder store_in_progress_builder(Store store, RustError.ByReference err);
    void in_progress_builder_add_string(InProgressBuilder builder, long entid, String kw, String value, RustError.ByReference err);
    void in_progress_builder_add_long(InProgressBuilder builder, long entid, String kw, long value, RustError.ByReference err);
    void in_progress_builder_add_ref(InProgressBuilder builder, long entid, String kw, long value, RustError.ByReference err);
    void in_progress_builder_add_keyword(InProgressBuilder builder, long entid, String kw, String value, RustError.ByReference err);
    void in_progress_builder_add_timestamp(InProgressBuilder builder, long entid, String kw, long value, RustError.ByReference err);
    void in_progress_builder_add_boolean(InProgressBuilder builder, long entid, String kw, int value, RustError.ByReference err);
    void in_progress_builder_add_double(InProgressBuilder builder, long entid, String kw, double value, RustError.ByReference err);
    void in_progress_builder_add_uuid(InProgressBuilder builder, long entid, String kw, Pointer value, RustError.ByReference err);
    void in_progress_builder_retract_string(InProgressBuilder builder, long entid, String kw, String value, RustError.ByReference err);
    void in_progress_builder_retract_long(InProgressBuilder builder, long entid, String kw, long value, RustError.ByReference err);
    void in_progress_builder_retract_ref(InProgressBuilder builder, long entid, String kw, long value, RustError.ByReference err);
    void in_progress_builder_retract_keyword(InProgressBuilder builder, long entid, String kw, String value, RustError.ByReference err);
    void in_progress_builder_retract_timestamp(InProgressBuilder builder, long entid, String kw, long value, RustError.ByReference err);
    void in_progress_builder_retract_boolean(InProgressBuilder builder, long entid, String kw, int value, RustError.ByReference err);
    void in_progress_builder_retract_double(InProgressBuilder builder, long entid, String kw, double value, RustError.ByReference err);
    void in_progress_builder_retract_uuid(InProgressBuilder builder, long entid, String kw, Pointer value, RustError.ByReference err);
    InProgressTransactionResult.ByValue in_progress_builder_transact(InProgressBuilder builder);
    TxReport in_progress_builder_commit(InProgressBuilder builder, RustError.ByReference err);

    // entity building
    EntityBuilder store_entity_builder_from_temp_id(Store store, String temp_id, RustError.ByReference err);
    EntityBuilder store_entity_builder_from_entid(Store store, long entid, RustError.ByReference err);
    void entity_builder_add_string(EntityBuilder builder, String kw, String value, RustError.ByReference err);
    void entity_builder_add_long(EntityBuilder builder, String kw, long value, RustError.ByReference err);
    void entity_builder_add_ref(EntityBuilder builder, String kw, long value, RustError.ByReference err);
    void entity_builder_add_keyword(EntityBuilder builder, String kw, String value, RustError.ByReference err);
    void entity_builder_add_boolean(EntityBuilder builder, String kw, int value, RustError.ByReference err);
    void entity_builder_add_double(EntityBuilder builder, String kw, double value, RustError.ByReference err);
    void entity_builder_add_timestamp(EntityBuilder builder, String kw, long value, RustError.ByReference err);
    void entity_builder_add_uuid(EntityBuilder builder, String kw, Pointer value, RustError.ByReference err);
    void entity_builder_retract_string(EntityBuilder builder, String kw, String value, RustError.ByReference err);
    void entity_builder_retract_long(EntityBuilder builder, String kw, long value, RustError.ByReference err);
    void entity_builder_retract_ref(EntityBuilder builder, String kw, long value, RustError.ByReference err);
    void entity_builder_retract_keyword(EntityBuilder builder, String kw, String value, RustError.ByReference err);
    void entity_builder_retract_boolean(EntityBuilder builder, String kw, int value, RustError.ByReference err);
    void entity_builder_retract_double(EntityBuilder builder, String kw, double value, RustError.ByReference err);
    void entity_builder_retract_timestamp(EntityBuilder builder, String kw, long value, RustError.ByReference err);
    void entity_builder_retract_uuid(EntityBuilder builder, String kw, Pointer value, RustError.ByReference err);
    InProgressTransactionResult.ByValue entity_builder_transact(EntityBuilder builder);
    TxReport entity_builder_commit(EntityBuilder builder, RustError.ByReference err);

    // observers
    void store_register_observer(Store store, String key, Pointer attributes, int len, TxObserverCallback callback);
    void store_unregister_observer(Store store, String key);
    long store_entid_for_attribute(Store store, String attr);
    Pointer store_current_schema(Store store);

    // Query Building
    QueryBuilder store_query(Store store, String query);
    TypedValue store_value_for_attribute(Store store, long entid, String attribute, RustError.ByReference err);
    void query_builder_bind_long(QueryBuilder query, String var, long value);
    void query_builder_bind_ref(QueryBuilder query, String var, long value);
    void query_builder_bind_ref_kw(QueryBuilder query, String var, String value);
    void query_builder_bind_kw(QueryBuilder query, String var, String value);
    void query_builder_bind_boolean(QueryBuilder query, String var, int value);
    void query_builder_bind_double(QueryBuilder query, String var, double value);
    void query_builder_bind_timestamp(QueryBuilder query, String var, long value);
    void query_builder_bind_string(QueryBuilder query, String var, String value);
    void query_builder_bind_uuid(QueryBuilder query, String var, Pointer value);

    // Query Execution
    RelResult query_builder_execute(QueryBuilder query, RustError.ByReference err);
    TypedValue query_builder_execute_scalar(QueryBuilder query, RustError.ByReference err);
    TypedValueList query_builder_execute_coll(QueryBuilder query, RustError.ByReference err);
    TypedValueList query_builder_execute_tuple(QueryBuilder query, RustError.ByReference err);

    // Query Result Processing
    long typed_value_into_long(TypedValue value);
    long typed_value_into_entid(TypedValue value);
    Pointer typed_value_into_kw(TypedValue value);
    Pointer typed_value_into_string(TypedValue value);
    Pointer typed_value_into_uuid(TypedValue value);
    int typed_value_into_boolean(TypedValue value);
    double typed_value_into_double(TypedValue value);
    long typed_value_into_timestamp(TypedValue value);
    int typed_value_value_type(TypedValue value);

    TypedValueList row_at_index(RelResult rows, int index);
    RelResultIter typed_value_result_set_into_iter(RelResult rows);
    TypedValueList typed_value_result_set_iter_next(RelResultIter iter);

    TypedValueListIter typed_value_list_into_iter(TypedValueList rows);
    TypedValue typed_value_list_iter_next(TypedValueListIter iter);

    TypedValue value_at_index(TypedValueList rows, int index);
    long value_at_index_into_long(TypedValueList rows, int index);
    long value_at_index_into_entid(TypedValueList rows, int index);
    Pointer value_at_index_into_kw(TypedValueList rows, int index);
    Pointer value_at_index_into_string(TypedValueList rows, int index);
    Pointer value_at_index_into_uuid(TypedValueList rows, int index);
    int value_at_index_into_boolean(TypedValueList rows, int index);
    double value_at_index_into_double(TypedValueList rows, int index);
    long value_at_index_into_timestamp(TypedValueList rows, int index);
}
