/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

#ifndef store_h
#define store_h
#include <stdint.h>
#include <Foundation/NSObjCRuntime.h>

struct TransactionChange {
    int64_t txid;
    int64_t*_Nonnull* _Nonnull changes;
    uint64_t len;
};

struct TxChangeList {
    struct TransactionChange*_Nonnull* _Nonnull reports;
    uint64_t len;
};
typedef struct TxChangeList TxChangeList;

struct Result {
    void* _Nullable ok;
    char* _Nullable err;
};
typedef struct Result Result;

struct Option {
    void* _Nullable value;
};
typedef struct Option Option;

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

void destroy(void* _Nullable obj);
void query_builder_destroy(struct Query* _Nullable obj);
void store_destroy(struct Store* _Nonnull obj);
void tx_report_destroy(struct TxReport* _Nonnull obj);
void typed_value_destroy(struct TypedValue* _Nullable obj);
void typed_value_list_destroy(struct QueryResultRow* _Nullable obj);
void typed_value_list_iter_destroy(struct QueryRowIterator* _Nullable obj);
void typed_value_result_set_destroy(struct QueryResultRows* _Nullable obj);
void typed_value_result_set_iter_destroy(struct QueryRowsIterator* _Nullable obj);

// transact
struct Result*_Nonnull store_transact(struct Store*_Nonnull store, const char* _Nonnull transaction);
const int64_t* _Nullable tx_report_entity_for_temp_id(const struct TxReport* _Nonnull report, const char* _Nonnull tempid);
int64_t tx_report_get_entid(const struct TxReport* _Nonnull report);
int64_t tx_report_get_tx_instant(const struct TxReport* _Nonnull report);

// Sync
struct Result*_Nonnull store_sync(struct Store*_Nonnull store, const char* _Nonnull user_uuid, const char* _Nonnull server_uri);

// Observers
void store_register_observer(struct Store*_Nonnull  store, const char* _Nonnull key, const int64_t* _Nonnull attributes, const int64_t len, void (*_Nonnull callback_fn)(const char* _Nonnull key, const struct TxChangeList* _Nonnull reports));
void store_unregister_observer(struct Store*_Nonnull  store, const char* _Nonnull key);
int64_t store_entid_for_attribute(struct Store*_Nonnull store, const char*_Nonnull attr);
int64_t changelist_entry_at(const struct TransactionChange* _Nonnull report, size_t index);

// Query
struct Query*_Nonnull store_query(struct Store*_Nonnull store, const char* _Nonnull query);
struct Result*_Nonnull store_value_for_attribute(struct Store*_Nonnull store, const int64_t entid, const char* _Nonnull attribute);

// Query Variable Binding
void query_builder_bind_int(struct Store*_Nonnull store, const char* _Nonnull var, const int32_t value);
void query_builder_bind_long(struct Query*_Nonnull query, const char* _Nonnull var, const int64_t value);
void query_builder_bind_ref(struct Query*_Nonnull query, const char* _Nonnull var, const int64_t value);
void query_builder_bind_ref_kw(struct Query*_Nonnull query, const char* _Nonnull var, const char* _Nonnull value);
void query_builder_bind_kw(struct Query*_Nonnull query, const char* _Nonnull var, const char* _Nonnull value);
void query_builder_bind_boolean(struct Query*_Nonnull query, const char* _Nonnull var, const int32_t value);
void query_builder_bind_double(struct Query*_Nonnull query, const char* _Nonnull var, const double value);
void query_builder_bind_timestamp(struct Query*_Nonnull query, const char* _Nonnull var, const int64_t value);
void query_builder_bind_string(struct Query*_Nonnull query, const char* _Nonnull var, const char* _Nonnull value);
void query_builder_bind_uuid(struct Query*_Nonnull query, const char* _Nonnull var, const char* _Nonnull value);

// Query execution
struct Result*_Nonnull query_builder_execute(struct Query*_Nonnull query);
struct Result*_Nonnull query_builder_execute_scalar(struct Query*_Nonnull query);
struct Result*_Nonnull query_builder_execute_coll(struct Query*_Nonnull query);
struct Result*_Nonnull query_builder_execute_tuple(struct Query*_Nonnull query);

// Query Result Processing
int64_t typed_value_as_long(struct TypedValue*_Nonnull  value);
int64_t typed_value_as_entid(struct TypedValue*_Nonnull  value);
const char* _Nonnull typed_value_as_kw(struct TypedValue*_Nonnull  value);
int32_t typed_value_as_boolean(struct TypedValue*_Nonnull  value);
double typed_value_as_double(struct TypedValue*_Nonnull  value);
int64_t typed_value_as_timestamp(struct TypedValue*_Nonnull  value);
const char* _Nonnull typed_value_as_string(struct TypedValue*_Nonnull  value);
const char* _Nonnull typed_value_as_uuid(struct TypedValue*_Nonnull  value);
enum ValueType typed_value_value_type(struct TypedValue*_Nonnull value);

struct QueryResultRow* _Nullable row_at_index(struct QueryResultRows* _Nonnull rows, const int32_t index);
struct QueryRowsIterator* _Nonnull rows_iter(struct QueryResultRows* _Nonnull rows);
struct QueryResultRow* _Nullable rows_iter_next(struct QueryRowsIterator* _Nonnull iter);
struct QueryRowIterator* _Nonnull values_iter(struct QueryResultRow* _Nonnull row);
struct TypedValue* _Nullable values_iter_next(struct QueryRowIterator* _Nonnull iter);
const int64_t* _Nullable values_iter_next_as_long(struct QueryRowIterator* _Nonnull iter);
const int64_t* _Nullable values_iter_next_as_entid(struct QueryRowIterator* _Nonnull iter);
const char* _Nullable values_iter_next_as_kw(struct QueryRowIterator* _Nonnull iter);
const int32_t* _Nullable values_iter_next_as_boolean(struct QueryRowIterator* _Nonnull iter);
const double* _Nullable values_iter_next_as_double(struct QueryRowIterator* _Nonnull iter);
const int64_t* _Nullable values_iter_next_as_timestamp(struct QueryRowIterator* _Nonnull iter);
const char* _Nullable values_iter_next_as_string(struct QueryRowIterator* _Nonnull iter);
const char* _Nullable values_iter_next_as_uuid(struct QueryRowIterator* _Nonnull iter);

struct TypedValue* _Nonnull value_at_index(struct QueryResultRow* _Nonnull row, const int32_t index);
int64_t value_at_index_as_long(struct QueryResultRow* _Nonnull row, const int32_t index);
int64_t value_at_index_as_entid(struct QueryResultRow* _Nonnull row, const int32_t index);
const char* _Nonnull value_at_index_as_kw(struct QueryResultRow* _Nonnull row, const int32_t index);
int32_t value_at_index_as_boolean(struct QueryResultRow* _Nonnull row, const int32_t index);
double value_at_index_as_double(struct QueryResultRow* _Nonnull row, const int32_t index);
int64_t value_at_index_as_timestamp(struct QueryResultRow* _Nonnull row, const int32_t index);
const char* _Nonnull value_at_index_as_string(struct QueryResultRow* _Nonnull row, const int32_t index);
const char* _Nonnull value_at_index_as_uuid(struct QueryResultRow* _Nonnull row, const int32_t index);

// Set single values
struct Result*_Nonnull store_set_long_for_attribute_on_entid(struct Store*_Nonnull store, const int64_t entid, const char* _Nonnull attribute, const int64_t value);
struct Result*_Nonnull store_set_entid_for_attribute_on_entid(struct Store*_Nonnull store, const int64_t entid, const char* _Nonnull attribute, const int64_t value);
struct Result*_Nonnull store_set_kw_ref_for_attribute_on_entid(struct Store*_Nonnull store, const int64_t entid, const char* _Nonnull attribute, const char* _Nonnull value);
struct Result*_Nonnull store_set_boolean_for_attribute_on_entid(struct Store*_Nonnull store, const int64_t entid, const char* _Nonnull attribute, const int32_t value);
struct Result*_Nonnull store_set_double_for_attribute_on_entid(struct Store*_Nonnull store, const int64_t entid, const char* _Nonnull attribute, const double value);
struct Result*_Nonnull store_set_timestamp_for_attribute_on_entid(struct Store*_Nonnull store, const int64_t entid, const char* _Nonnull attribute, const int64_t value);
struct Result*_Nonnull store_set_string_for_attribute_on_entid(struct Store*_Nonnull store, const int64_t entid, const char* _Nonnull attribute, const char* _Nonnull value);
struct Result*_Nonnull store_set_uuid_for_attribute_on_entid(struct Store*_Nonnull store, const int64_t entid, const char* _Nonnull attribute, const char* _Nonnull value);

// Transaction change lists
const struct TransactionChange* _Nullable tx_change_list_entry_at(const struct TxChangeList* _Nonnull list, size_t index);

#endif /* store_h */
