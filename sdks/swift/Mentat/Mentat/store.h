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


// Opaque Structs mapping to Rust types that are passed over the FFI boundary. In cases where the
// struct's name differs from the name used for the Rust type, it's noted in a comment.
struct EntityBuilder; // Note: a `mentat::EntityBuilder<mentat::InProgressBuilder<'a, 'c>>`
struct InProgress;
struct InProgressBuilder;
struct Query; // Note: a `mentat::QueryBuilder`
struct QueryResultRow; // Note: a `Vec<mentat::Binding>`
struct QueryResultRows; // Note: a `mentat::RelResult<Binding>`
struct QueryRowsIterator; // Note: a `mentat::BindingListIterator`
struct QueryRowIterator; // Note: a `mentat::BindingIterator`
struct Store;
struct TxReport;
struct TypedValue; // Note: a `mentat::Binding`


/*
 A mapping of the TransactionChange repr(C) Rust object.
 The memory for this is managed by Rust.
 */
struct TxChange {
    int64_t txid;
    const int64_t* _Nonnull changes;
    uint64_t len;
};

/*
 A mapping of the TxChangeList repr(C) Rust object.
 The memory for this is managed by Rust.
 */
struct TxChangeList {
    const struct TxChange* _Nonnull reports;
    uint64_t len;
};
typedef struct TxChangeList TxChangeList;

/* Representation of the `ExternError` Rust type.

   If `message` is not null, an error occur occurred (and we're responsible for freeing `message`,
   using `rust_c_string_destroy`).
*/
struct RustError {
    char *message;
};

/*
 A mapping of the ExternResult<()> repr(C) Rust object.
 These are not allocated on the heap, but the memory for `ok` and `err`
 is managed by Swift.
 */
struct VoidResult { void* _Nullable ok; char* _Nullable err; };
#define DEFINE_RESULT(Name, Type) struct Name { struct Type *_Nullable ok; char *_Nullable err; }

/*
 A mapping of the InProgressTransactResult repr(C) Rust object.
 These are not allocated on the heap, but the memory for `inProgress`,
 `txReport`, and `result.message` (if pressent)
 as well as `result.ok` and `result.err`, are managed by Swift.
 */
struct InProgressTransactResult {
    struct InProgress *_Nonnull inProgress;
    struct TxReport *_Nullable txReport;
    struct RustError error;
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

// Store
struct Store*_Nonnull store_open(const char*_Nonnull uri, struct RustError* _Nonnull error);

// Destructors.
void destroy(void* _Nullable obj);
void uuid_destroy(uuid_t* _Nullable obj);
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
void rust_c_string_destroy(char *_Nullable s);
// caching
void store_cache_attribute_forward(struct Store*_Nonnull store, const char* _Nonnull attribute, struct RustError* _Nonnull error);
void store_cache_attribute_reverse(struct Store*_Nonnull store, const char* _Nonnull attribute, struct RustError* _Nonnull error);
void store_cache_attribute_bi_directional(struct Store*_Nonnull store, const char* _Nonnull attribute, struct RustError* _Nonnull error);

// transact
struct TxReport*_Nullable store_transact(struct Store*_Nonnull store, const char* _Nonnull transaction, struct RustError* _Nonnull error);
int64_t* _Nullable tx_report_entity_for_temp_id(const struct TxReport* _Nonnull report, const char* _Nonnull tempid);
int64_t  tx_report_get_entid(const struct TxReport* _Nonnull report);
int64_t tx_report_get_tx_instant(const struct TxReport* _Nonnull report);
struct InProgress *_Nullable store_begin_transaction(struct Store*_Nonnull store, struct RustError* _Nonnull error);

// in progress
struct TxReport*_Nullable in_progress_transact(struct InProgress*_Nonnull in_progress, const char* _Nonnull transaction, struct RustError*_Nonnull err);
void in_progress_commit(struct InProgress*_Nonnull in_progress, struct RustError* _Nonnull error);
void in_progress_rollback(struct InProgress*_Nonnull in_progress, struct RustError* _Nonnull error);

// in_progress entity building
struct InProgressBuilder*_Nullable store_in_progress_builder(struct Store*_Nonnull store, struct RustError* _Nonnull error);
struct InProgressBuilder*_Nonnull in_progress_builder(struct InProgress*_Nonnull in_progress);

struct EntityBuilder*_Nonnull in_progress_entity_builder_from_temp_id(struct InProgress*_Nonnull in_progress, const char*_Nonnull temp_id, struct RustError* _Nonnull error);
struct EntityBuilder*_Nonnull in_progress_entity_builder_from_entid(struct InProgress*_Nonnull in_progress, const int64_t entid);

void in_progress_builder_add_string(struct InProgressBuilder*_Nonnull builder, const int64_t entid, const char*_Nonnull kw, const char*_Nonnull value, struct RustError* _Nonnull error);
void in_progress_builder_add_long(struct InProgressBuilder*_Nonnull builder, const int64_t entid, const char*_Nonnull kw, const int64_t value, struct RustError* _Nonnull error);
void in_progress_builder_add_ref(struct InProgressBuilder*_Nonnull builder, const int64_t entid, const char*_Nonnull kw, const int64_t value, struct RustError* _Nonnull error);
void in_progress_builder_add_keyword(struct InProgressBuilder*_Nonnull builder, const int64_t entid, const char*_Nonnull kw, const char*_Nonnull value, struct RustError* _Nonnull error);
void in_progress_builder_add_timestamp(struct InProgressBuilder*_Nonnull builder, const int64_t entid, const char*_Nonnull kw, const int64_t value, struct RustError* _Nonnull error);
void in_progress_builder_add_boolean(struct InProgressBuilder*_Nonnull builder, const int64_t entid, const char*_Nonnull kw, const int32_t value, struct RustError* _Nonnull error);
void in_progress_builder_add_double(struct InProgressBuilder*_Nonnull builder, const int64_t entid, const char*_Nonnull kw, const double value, struct RustError* _Nonnull error);
void in_progress_builder_add_uuid(struct InProgressBuilder*_Nonnull builder, const int64_t entid, const char*_Nonnull kw, const uuid_t* _Nonnull value, struct RustError* _Nonnull error);
void in_progress_builder_retract_string(struct InProgressBuilder*_Nonnull builder, const int64_t entid, const char*_Nonnull kw, const char*_Nonnull value, struct RustError* _Nonnull error);
void in_progress_builder_retract_long(struct InProgressBuilder*_Nonnull builder, const int64_t entid, const char*_Nonnull kw, const int64_t value, struct RustError* _Nonnull error);
void in_progress_builder_retract_ref(struct InProgressBuilder*_Nonnull builder, const int64_t entid, const char*_Nonnull kw, const int64_t value, struct RustError* _Nonnull error);
void in_progress_builder_retract_keyword(struct InProgressBuilder*_Nonnull builder, const int64_t entid, const char*_Nonnull kw, const char*_Nonnull value, struct RustError* _Nonnull error);
void in_progress_builder_retract_timestamp(struct InProgressBuilder*_Nonnull builder, const int64_t entid, const char*_Nonnull kw, const int64_t value, struct RustError* _Nonnull error);
void in_progress_builder_retract_boolean(struct InProgressBuilder*_Nonnull builder, const int64_t entid, const char*_Nonnull kw, const int32_t value, struct RustError* _Nonnull error);
void in_progress_builder_retract_double(struct InProgressBuilder*_Nonnull builder, const int64_t entid, const char*_Nonnull kw, const double value, struct RustError* _Nonnull error);
void in_progress_builder_retract_uuid(struct InProgressBuilder*_Nonnull builder, const int64_t entid, const char*_Nonnull kw, const uuid_t* _Nonnull value, struct RustError* _Nonnull error);
struct InProgressTransactResult in_progress_builder_transact(struct InProgressBuilder*_Nonnull builder);
struct TxReport*_Nullable in_progress_builder_commit(struct InProgressBuilder*_Nonnull builder, struct RustError* _Nonnull error);

// entity building
struct EntityBuilder*_Nullable store_entity_builder_from_temp_id(struct Store*_Nonnull store, const char*_Nonnull temp_id, struct RustError* _Nonnull error);
struct EntityBuilder*_Nullable store_entity_builder_from_entid(struct Store*_Nonnull store, const int64_t entid, struct RustError* _Nonnull error) ;
void entity_builder_add_string(struct EntityBuilder*_Nonnull builder, const char*_Nonnull kw, const char*_Nonnull value, struct RustError* _Nonnull error);
void entity_builder_add_long(struct EntityBuilder*_Nonnull builder, const char*_Nonnull kw, const int64_t value, struct RustError* _Nonnull error);
void entity_builder_add_ref(struct EntityBuilder*_Nonnull builder, const char*_Nonnull kw, const int64_t value, struct RustError* _Nonnull error);
void entity_builder_add_keyword(struct EntityBuilder*_Nonnull builder, const char*_Nonnull kw, const char*_Nonnull value, struct RustError* _Nonnull error);
void entity_builder_add_boolean(struct EntityBuilder*_Nonnull builder, const char*_Nonnull kw, const int32_t value, struct RustError* _Nonnull error);
void entity_builder_add_double(struct EntityBuilder*_Nonnull builder, const char*_Nonnull kw, const double value, struct RustError* _Nonnull error);
void entity_builder_add_timestamp(struct EntityBuilder*_Nonnull builder, const char*_Nonnull kw, const int64_t value, struct RustError* _Nonnull error);
void entity_builder_add_uuid(struct EntityBuilder*_Nonnull builder, const char*_Nonnull kw, const uuid_t* _Nonnull value, struct RustError* _Nonnull error);

void entity_builder_retract_string(struct EntityBuilder*_Nonnull builder, const char*_Nonnull kw, const char*_Nonnull value, struct RustError* _Nonnull error);
void entity_builder_retract_long(struct EntityBuilder*_Nonnull builder, const char*_Nonnull kw, const int64_t value, struct RustError* _Nonnull error);
void entity_builder_retract_ref(struct EntityBuilder*_Nonnull builder, const char*_Nonnull kw, const int64_t value, struct RustError* _Nonnull error);
void entity_builder_retract_keyword(struct EntityBuilder*_Nonnull builder, const char*_Nonnull kw, const char*_Nonnull value, struct RustError* _Nonnull error);
void entity_builder_retract_boolean(struct EntityBuilder*_Nonnull builder, const char*_Nonnull kw, const int32_t value, struct RustError* _Nonnull error);
void entity_builder_retract_double(struct EntityBuilder*_Nonnull builder, const char*_Nonnull kw, const double value, struct RustError* _Nonnull error);
void entity_builder_retract_timestamp(struct EntityBuilder*_Nonnull builder, const char*_Nonnull kw, const int64_t value, struct RustError* _Nonnull error);
void entity_builder_retract_uuid(struct EntityBuilder*_Nonnull builder, const char*_Nonnull kw, const uuid_t* _Nonnull value, struct RustError* _Nonnull error);

struct InProgressTransactResult entity_builder_transact(struct EntityBuilder*_Nonnull builder);
struct TxReport*_Nullable entity_builder_commit(struct EntityBuilder*_Nonnull builder, struct RustError* _Nonnull error);

// Observers
void store_register_observer(struct Store*_Nonnull  store, const char* _Nonnull key, const int64_t* _Nonnull attributes, const int64_t len, void (*_Nonnull callback_fn)(const char* _Nonnull key, const struct TxChangeList* _Nonnull reports));
void store_unregister_observer(struct Store*_Nonnull  store, const char* _Nonnull key);
int64_t store_entid_for_attribute(struct Store*_Nonnull store, const char*_Nonnull attr);
int64_t changelist_entry_at(const struct TxChange* _Nonnull report, size_t index);

// Query
struct Query*_Nonnull store_query(struct Store*_Nonnull store, const char* _Nonnull query);
struct TypedValue*_Nullable store_value_for_attribute(struct Store*_Nonnull store, const int64_t entid, const char* _Nonnull attribute, struct RustError* _Nonnull error);

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
struct QueryResultRows* _Nullable query_builder_execute(struct Query*_Nonnull query, struct RustError* _Nonnull error);
struct TypedValue* _Nullable query_builder_execute_scalar(struct Query*_Nonnull query, struct RustError* _Nonnull error);
struct QueryResultRow* _Nullable query_builder_execute_coll(struct Query*_Nonnull query, struct RustError* _Nonnull error);
struct QueryResultRow* _Nullable query_builder_execute_tuple(struct Query*_Nonnull query, struct RustError* _Nonnull error);

// Query Result Processing
int64_t typed_value_into_long(struct TypedValue*_Nonnull  value);
int64_t typed_value_into_entid(struct TypedValue*_Nonnull  value);
char* _Nonnull typed_value_into_kw(struct TypedValue*_Nonnull  value);
int32_t typed_value_into_boolean(struct TypedValue*_Nonnull  value);
double typed_value_into_double(struct TypedValue*_Nonnull  value);
int64_t typed_value_into_timestamp(struct TypedValue*_Nonnull  value);
char* _Nonnull typed_value_into_string(struct TypedValue*_Nonnull  value);
uuid_t* _Nonnull typed_value_into_uuid(struct TypedValue*_Nonnull  value);
enum ValueType typed_value_value_type(struct TypedValue*_Nonnull value);

struct QueryResultRow* _Nullable row_at_index(struct QueryResultRows* _Nonnull rows, const int32_t index);
struct QueryRowsIterator* _Nonnull typed_value_result_set_into_iter(struct QueryResultRows* _Nonnull rows);
struct QueryResultRow* _Nullable typed_value_result_set_iter_next(struct QueryRowsIterator* _Nonnull iter);
struct QueryRowIterator* _Nonnull typed_value_list_into_iter(struct QueryResultRow* _Nonnull row);
struct TypedValue* _Nullable typed_value_list_iter_next(struct QueryRowIterator* _Nonnull iter);

struct TypedValue* _Nonnull value_at_index(struct QueryResultRow* _Nonnull row, const int32_t index);
int64_t value_at_index_into_long(struct QueryResultRow* _Nonnull row, const int32_t index);
int64_t value_at_index_into_entid(struct QueryResultRow* _Nonnull row, const int32_t index);
char* _Nonnull value_at_index_into_kw(struct QueryResultRow* _Nonnull row, const int32_t index);
int32_t value_at_index_into_boolean(struct QueryResultRow* _Nonnull row, const int32_t index);
double value_at_index_into_double(struct QueryResultRow* _Nonnull row, const int32_t index);
int64_t value_at_index_into_timestamp(struct QueryResultRow* _Nonnull row, const int32_t index);
char* _Nonnull value_at_index_into_string(struct QueryResultRow* _Nonnull row, const int32_t index);
uuid_t* _Nonnull value_at_index_into_uuid(struct QueryResultRow* _Nonnull row, const int32_t index);

// Transaction change lists
const struct TxChange* _Nonnull tx_change_list_entry_at(const struct TxChangeList* _Nonnull list, size_t index);

#endif /* store_h */

