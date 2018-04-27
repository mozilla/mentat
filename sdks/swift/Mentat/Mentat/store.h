/* Copyright 2018 Mozilla
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License. */

#ifndef store_h
#define store_h
#include <stdint.h>
#include <Foundation/NSObjCRuntime.h>

/*
 * This file contains headers for all of the structs and functions that map directly to the functions
 * defined in mentat/ffi/src/lib.rs.
 *
 * The C in this file is specifically formatted to be used with Objective C and Swift and contains
 * macros and flags that will not be recognised by other C based languages.
 */

/*
 A mapping of the TxChange repr(C) Rust object.
 The memory for this is managed by Swift.
 */
struct TxChange {
    int64_t txid;
    int64_t*_Nonnull* _Nonnull changes;
    uint64_t len;
};

/*
 A mapping of the TxChangeList repr(C) Rust object.
 The memory for this is managed by Swift.
 */
struct TxChangeList {
    struct TxChange*_Nonnull* _Nonnull reports;
    uint64_t len;
};
typedef struct TxChangeList TxChangeList;

/*
 A mapping of the ExternResult repr(C) Rust object.
 The memory for this is managed by Swift.
 */
struct Result {
    void* _Nullable ok;
    char* _Nullable err;
};
typedef struct Result Result;

/*
 A mapping of the ExternOption repr(C) Rust object.
 The memory for this is managed by Swift.
 */
struct Option {
    void* _Nullable value;
};
typedef struct Option Option;

/*
 A mapping of the InProgressTransactResult repr(C) Rust object.
 The memory for this is managed by Swift.
 */
struct InProgressTransactResult {
    struct InProgress*_Nonnull inProgress;
    struct Result*_Nonnull result;
};
typedef struct InProgressTransactResult InProgressTransactResult;

/*
 A Mapping for the ValueType Rust object.
 */
typedef NS_ENUM(NSInteger, ValueType) {
    ValueTypeRef = 1,
    ValueTypeBoolean,
    ValueTypeInstant,
    ValueTypeLong,
    ValueTypeDouble,
    ValueTypeString,
    ValueTypeKeyword,
    ValueTypeUuid
};

// Opaque Structs mapping to Rust types that are passed over the FFI boundary
struct EntityBuilder;
struct InProgress;
struct InProgressBuilder;
struct Query;
struct QueryResultRow;
struct QueryResultRows;
struct QueryRowsIterator;
struct QueryRowIterator;
struct Store;
struct TxReport;
struct TypedValue;

// Store
struct Store*_Nonnull store_open(const char*_Nonnull uri);

// Destructors.
void destroy(void* _Nullable obj);
void query_builder_destroy(struct Query* _Nullable obj);
void store_destroy(struct Store* _Nonnull obj);
void tx_report_destroy(struct TxReport* _Nonnull obj);
void typed_value_destroy(struct TypedValue* _Nullable obj);
void typed_value_list_destroy(struct QueryResultRow* _Nullable obj);
void typed_value_list_iter_destroy(struct QueryRowIterator* _Nullable obj);
void typed_value_result_set_destroy(struct QueryResultRows* _Nullable obj);
void typed_value_result_set_iter_destroy(struct QueryRowsIterator* _Nullable obj);
void in_progress_destroy(struct InProgress* _Nullable obj);
void in_progress_builder_destroy(struct InProgressBuilder* _Nullable obj);
void entity_builder_destroy(struct EntityBuilder* _Nullable obj);

// caching
struct Result*_Nonnull  store_cache_attribute_forward(struct Store*_Nonnull store, const char* _Nonnull attribute);
struct Result*_Nonnull  store_cache_attribute_reverse(struct Store*_Nonnull store, const char* _Nonnull attribute);
struct Result*_Nonnull  store_cache_attribute_bi_directional(struct Store*_Nonnull store, const char* _Nonnull attribute);

// transact
struct Result*_Nonnull store_transact(struct Store*_Nonnull store, const char* _Nonnull transaction);
const int64_t* _Nullable tx_report_entity_for_temp_id(const struct TxReport* _Nonnull report, const char* _Nonnull tempid);
int64_t tx_report_get_entid(const struct TxReport* _Nonnull report);
int64_t tx_report_get_tx_instant(const struct TxReport* _Nonnull report);
struct Result*_Nonnull store_begin_transaction(struct Store*_Nonnull store);

// in progress
struct Result*_Nonnull in_progress_transact(struct InProgress*_Nonnull in_progress, const char* _Nonnull transaction);
struct Result*_Nonnull in_progress_commit(struct InProgress*_Nonnull in_progress);
struct Result*_Nonnull in_progress_rollback(struct InProgress*_Nonnull in_progress);

// in_progress entity building
struct Result*_Nonnull store_in_progress_builder(struct Store*_Nonnull store);
struct InProgressBuilder*_Nonnull in_progress_builder(struct InProgress*_Nonnull in_progress);
struct EntityBuilder*_Nonnull in_progress_entity_builder_from_temp_id(struct InProgress*_Nonnull in_progress, const char*_Nonnull temp_id);
struct EntityBuilder*_Nonnull in_progress_entity_builder_from_entid(struct InProgress*_Nonnull in_progress, const int64_t entid);
struct Result*_Nonnull in_progress_builder_add_string(struct InProgressBuilder*_Nonnull builder, const int64_t entid, const char*_Nonnull kw, const char*_Nonnull value);
struct Result*_Nonnull in_progress_builder_add_long(struct InProgressBuilder*_Nonnull builder, const int64_t entid, const char*_Nonnull kw, const int64_t value);
struct Result*_Nonnull in_progress_builder_add_ref(struct InProgressBuilder*_Nonnull builder, const int64_t entid, const char*_Nonnull kw, const int64_t value);
struct Result*_Nonnull in_progress_builder_add_keyword(struct InProgressBuilder*_Nonnull builder, const int64_t entid, const char*_Nonnull kw, const char*_Nonnull value);
struct Result*_Nonnull in_progress_builder_add_timestamp(struct InProgressBuilder*_Nonnull builder, const int64_t entid, const char*_Nonnull kw, const int64_t value);
struct Result*_Nonnull in_progress_builder_add_boolean(struct InProgressBuilder*_Nonnull builder, const int64_t entid, const char*_Nonnull kw, const int32_t value);
struct Result*_Nonnull in_progress_builder_add_double(struct InProgressBuilder*_Nonnull builder, const int64_t entid, const char*_Nonnull kw, const double value);
struct Result*_Nonnull in_progress_builder_add_uuid(struct InProgressBuilder*_Nonnull builder, const int64_t entid, const char*_Nonnull kw, const uuid_t* _Nonnull value);
struct Result*_Nonnull in_progress_builder_retract_string(struct InProgressBuilder*_Nonnull builder, const int64_t entid, const char*_Nonnull kw, const char*_Nonnull value);
struct Result*_Nonnull in_progress_builder_retract_long(struct InProgressBuilder*_Nonnull builder, const int64_t entid, const char*_Nonnull kw, const int64_t value);
struct Result*_Nonnull in_progress_builder_retract_ref(struct InProgressBuilder*_Nonnull builder, const int64_t entid, const char*_Nonnull kw, const int64_t value);
struct Result*_Nonnull in_progress_builder_retract_keyword(struct InProgressBuilder*_Nonnull builder, const int64_t entid, const char*_Nonnull kw, const char*_Nonnull value);
struct Result*_Nonnull in_progress_builder_retract_timestamp(struct InProgressBuilder*_Nonnull builder, const int64_t entid, const char*_Nonnull kw, const int64_t value);
struct Result*_Nonnull in_progress_builder_retract_boolean(struct InProgressBuilder*_Nonnull builder, const int64_t entid, const char*_Nonnull kw, const int32_t value);
struct Result*_Nonnull in_progress_builder_retract_double(struct InProgressBuilder*_Nonnull builder, const int64_t entid, const char*_Nonnull kw, const double value);
struct Result*_Nonnull in_progress_builder_retract_uuid(struct InProgressBuilder*_Nonnull builder, const int64_t entid, const char*_Nonnull kw, const uuid_t* _Nonnull value);
struct InProgressTransactResult*_Nonnull in_progress_builder_transact(struct InProgressBuilder*_Nonnull builder);
struct Result*_Nonnull in_progress_builder_commit(struct InProgressBuilder*_Nonnull builder);

// entity building
struct Result*_Nonnull store_entity_builder_from_temp_id(struct Store*_Nonnull store, const char*_Nonnull temp_id);
struct Result*_Nonnull store_entity_builder_from_entid(struct Store*_Nonnull store, const int64_t entid);
struct Result*_Nonnull entity_builder_add_string(struct EntityBuilder*_Nonnull builder, const char*_Nonnull kw, const char*_Nonnull value);
struct Result*_Nonnull entity_builder_add_long(struct EntityBuilder*_Nonnull builder, const char*_Nonnull kw, const int64_t value);
struct Result*_Nonnull entity_builder_add_ref(struct EntityBuilder*_Nonnull builder, const char*_Nonnull kw, const int64_t value);
struct Result*_Nonnull entity_builder_add_keyword(struct EntityBuilder*_Nonnull builder, const char*_Nonnull kw, const char*_Nonnull value);
struct Result*_Nonnull entity_builder_add_boolean(struct EntityBuilder*_Nonnull builder, const char*_Nonnull kw, const int32_t value);
struct Result*_Nonnull entity_builder_add_double(struct EntityBuilder*_Nonnull builder, const char*_Nonnull kw, const double value);
struct Result*_Nonnull entity_builder_add_timestamp(struct EntityBuilder*_Nonnull builder, const char*_Nonnull kw, const int64_t value);
struct Result*_Nonnull entity_builder_add_uuid(struct EntityBuilder*_Nonnull builder, const char*_Nonnull kw, const uuid_t* _Nonnull value);
struct Result*_Nonnull entity_builder_retract_string(struct EntityBuilder*_Nonnull builder, const char*_Nonnull kw, const char*_Nonnull value);
struct Result*_Nonnull entity_builder_retract_long(struct EntityBuilder*_Nonnull builder, const char*_Nonnull kw, const int64_t value);
struct Result*_Nonnull entity_builder_retract_ref(struct EntityBuilder*_Nonnull builder, const char*_Nonnull kw, const int64_t value);
struct Result*_Nonnull entity_builder_retract_keyword(struct EntityBuilder*_Nonnull builder, const char*_Nonnull kw, const char*_Nonnull value);
struct Result*_Nonnull entity_builder_retract_boolean(struct EntityBuilder*_Nonnull builder, const char*_Nonnull kw, const int32_t value);
struct Result*_Nonnull entity_builder_retract_double(struct EntityBuilder*_Nonnull builder, const char*_Nonnull kw, const double value);
struct Result*_Nonnull entity_builder_retract_timestamp(struct EntityBuilder*_Nonnull builder, const char*_Nonnull kw, const int64_t value);
struct Result*_Nonnull entity_builder_retract_uuid(struct EntityBuilder*_Nonnull builder, const char*_Nonnull kw, const uuid_t* _Nonnull value);
struct InProgressTransactResult*_Nonnull entity_builder_transact(struct InProgressBuilder*_Nonnull builder);
struct Result*_Nonnull entity_builder_commit(struct EntityBuilder*_Nonnull builder);

// Sync
struct Result*_Nonnull store_sync(struct Store*_Nonnull store, const char* _Nonnull user_uuid, const char* _Nonnull server_uri);

// Observers
void store_register_observer(struct Store*_Nonnull  store, const char* _Nonnull key, const int64_t* _Nonnull attributes, const int64_t len, void (*_Nonnull callback_fn)(const char* _Nonnull key, const struct TxChangeList* _Nonnull reports));
void store_unregister_observer(struct Store*_Nonnull  store, const char* _Nonnull key);
int64_t store_entid_for_attribute(struct Store*_Nonnull store, const char*_Nonnull attr);
int64_t changelist_entry_at(const struct TxChange* _Nonnull report, size_t index);

// Query
struct Query*_Nonnull store_query(struct Store*_Nonnull store, const char* _Nonnull query);
struct Result*_Nonnull store_value_for_attribute(struct Store*_Nonnull store, const int64_t entid, const char* _Nonnull attribute);

// Query Variable Binding
void query_builder_bind_long(struct Query*_Nonnull query, const char* _Nonnull var, const int64_t value);
void query_builder_bind_ref(struct Query*_Nonnull query, const char* _Nonnull var, const int64_t value);
void query_builder_bind_ref_kw(struct Query*_Nonnull query, const char* _Nonnull var, const char* _Nonnull value);
void query_builder_bind_kw(struct Query*_Nonnull query, const char* _Nonnull var, const char* _Nonnull value);
void query_builder_bind_boolean(struct Query*_Nonnull query, const char* _Nonnull var, const int32_t value);
void query_builder_bind_double(struct Query*_Nonnull query, const char* _Nonnull var, const double value);
void query_builder_bind_timestamp(struct Query*_Nonnull query, const char* _Nonnull var, const int64_t value);
void query_builder_bind_string(struct Query*_Nonnull query, const char* _Nonnull var, const char* _Nonnull value);
void query_builder_bind_uuid(struct Query*_Nonnull query, const char* _Nonnull var, const uuid_t* _Nonnull value);

// Query execution
struct Result*_Nonnull query_builder_execute(struct Query*_Nonnull query);
struct Result*_Nonnull query_builder_execute_scalar(struct Query*_Nonnull query);
struct Result*_Nonnull query_builder_execute_coll(struct Query*_Nonnull query);
struct Result*_Nonnull query_builder_execute_tuple(struct Query*_Nonnull query);

// Query Result Processing
int64_t typed_value_into_long(struct TypedValue*_Nonnull  value);
int64_t typed_value_into_entid(struct TypedValue*_Nonnull  value);
const char* _Nonnull typed_value_into_kw(struct TypedValue*_Nonnull  value);
int32_t typed_value_into_boolean(struct TypedValue*_Nonnull  value);
double typed_value_into_double(struct TypedValue*_Nonnull  value);
int64_t typed_value_into_timestamp(struct TypedValue*_Nonnull  value);
const char* _Nonnull typed_value_into_string(struct TypedValue*_Nonnull  value);
const uuid_t* _Nonnull typed_value_into_uuid(struct TypedValue*_Nonnull  value);
enum ValueType typed_value_value_type(struct TypedValue*_Nonnull value);

struct QueryResultRow* _Nullable row_at_index(struct QueryResultRows* _Nonnull rows, const int32_t index);
struct QueryRowsIterator* _Nonnull typed_value_result_set_into_iter(struct QueryResultRows* _Nonnull rows);
struct QueryResultRow* _Nullable typed_value_result_set_iter_next(struct QueryRowsIterator* _Nonnull iter);
struct QueryRowIterator* _Nonnull typed_value_list_into_iter(struct QueryResultRow* _Nonnull row);
struct TypedValue* _Nullable typed_value_list_iter_next(struct QueryRowIterator* _Nonnull iter);

struct TypedValue* _Nonnull value_at_index(struct QueryResultRow* _Nonnull row, const int32_t index);
int64_t value_at_index_into_long(struct QueryResultRow* _Nonnull row, const int32_t index);
int64_t value_at_index_into_entid(struct QueryResultRow* _Nonnull row, const int32_t index);
const char* _Nonnull value_at_index_into_kw(struct QueryResultRow* _Nonnull row, const int32_t index);
int32_t value_at_index_into_boolean(struct QueryResultRow* _Nonnull row, const int32_t index);
double value_at_index_into_double(struct QueryResultRow* _Nonnull row, const int32_t index);
int64_t value_at_index_into_timestamp(struct QueryResultRow* _Nonnull row, const int32_t index);
const char* _Nonnull value_at_index_into_string(struct QueryResultRow* _Nonnull row, const int32_t index);
const uuid_t* _Nonnull value_at_index_into_uuid(struct QueryResultRow* _Nonnull row, const int32_t index);

// Transaction change lists
const struct TxChange* _Nullable tx_change_list_entry_at(const struct TxChangeList* _Nonnull list, size_t index);

#endif /* store_h */
