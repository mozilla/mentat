/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

import Foundation
import Mentatlib

enum QueryResult<T> {
    case error(Error)
    case success(T)
}

class Query: OptionalRustObject {

    func bind(varName: String, toInt value: Int32) throws {
        guard let r = self.raw else {
            throw QueryError.builderConsumed
        }
        query_builder_bind_int(r, varName, value)
    }

    func bind(varName: String, toLong value: Int64) throws {
        guard let r = self.raw else {
            throw QueryError.builderConsumed
        }
        query_builder_bind_long(r, varName, value)
    }

    func bind(varName: String, toReference value: Int64) throws {
        guard let r = self.raw else {
            throw QueryError.builderConsumed
        }
        query_builder_bind_ref(r, varName, value)
    }

    func bind(varName: String, toReference value: String) throws {
        guard let r = self.raw else {
            throw QueryError.builderConsumed
        }
        query_builder_bind_ref_kw(r, varName, value)
    }

    func bind(varName: String, toKeyword value: String) throws {
        guard let r = self.raw else {
            throw QueryError.builderConsumed
        }
        query_builder_bind_kw(r, varName, value)
    }

    func bind(varName: String, toBoolean value: Bool) throws {
        guard let r = self.raw else {
            throw QueryError.builderConsumed
        }
        query_builder_bind_boolean(r, varName, value ? 1 : 0)
    }

    func bind(varName: String, toDouble value: Double) throws {
        guard let r = self.raw else {
            throw QueryError.builderConsumed
        }
        query_builder_bind_double(r, varName, value)
    }

    func bind(varName: String, toDate value: Date) throws {
        guard let r = self.raw else {
            throw QueryError.builderConsumed
        }
        query_builder_bind_timestamp(r, varName, value.toMicroseconds())
    }

    func bind(varName: String, toString value: String) throws {
        guard let r = self.raw else {
            throw QueryError.builderConsumed
        }
        query_builder_bind_string(r, varName, value)
    }

    func bind(varName: String, toUuid value: UUID) throws {
        guard let r = self.raw else {
            throw QueryError.builderConsumed
        }
        query_builder_bind_uuid(r, varName, value.uuidString)
    }

    func executeMap(map: @escaping (QueryResult<TupleResult>) -> Void) throws {
        guard let r = self.raw else {
            throw QueryError.builderConsumed
        }

        DispatchQueue.global(qos: .background).async {
            let result = query_builder_execute(r)
            self.raw = nil

            if let err = result.pointee.err {
                let message = String(cString: err)
                map(QueryResult.error(QueryError.executionFailed(message: message)))
                return
            }
            guard let rowsPtr = result.pointee.ok else {
                return
            }
            let rows = RelResult(raw: rowsPtr)
            for row in rows {
                map(QueryResult.success(row))
            }
        }
    }

    func execute(callback: @escaping (QueryResult<RelResult?>) -> Void) throws {
        guard let r = self.raw else {
            throw QueryError.builderConsumed
        }

        DispatchQueue.global(qos: .background).async {
            let result = query_builder_execute(r)
            self.raw = nil

            if let err = result.pointee.err {
                let message = String(cString: err)
                callback(QueryResult.error(QueryError.executionFailed(message: message)))
                return
            }
            guard let results = result.pointee.ok else {
                callback(QueryResult.success(nil))
                return
            }
            callback(QueryResult.success(RelResult(raw: results)))
        }
    }

    func executeScalar(callback: @escaping (QueryResult<TypedValue?>) -> Void) throws {
        guard let r = self.raw else {
            throw QueryError.builderConsumed
        }

        DispatchQueue.global(qos: .background).async {
            let result = query_builder_execute_scalar(r)
            self.raw = nil

            if let err = result.pointee.err {
                let message = String(cString: err)
                callback(QueryResult.error(QueryError.executionFailed(message: message)))
            }
            guard let results = result.pointee.ok else {
                callback(QueryResult.success(nil))
                return
            }
            callback(QueryResult.success(TypedValue(raw: OpaquePointer(results))))
        }
    }

    func executeColl(callback: @escaping (QueryResult<ColResult?>) -> Void) throws {
        guard let r = self.raw else {
            throw QueryError.builderConsumed
        }

        DispatchQueue.global(qos: .background).async {
            let result = query_builder_execute_coll(r)
            self.raw = nil

            if let err = result.pointee.err {
                let message = String(cString: err)
                callback(QueryResult.error(QueryError.executionFailed(message: message)))
            }
            guard let results = result.pointee.ok else {
                callback(QueryResult.success(nil))
                return
            }
            callback(QueryResult.success(ColResult(raw: results)))
        }
    }

    func executeCollMap(map: @escaping (QueryResult<TypedValue>) -> Void) throws {
        guard let r = self.raw else {
            throw QueryError.builderConsumed
        }

        DispatchQueue.global(qos: .background).async {
            let result = query_builder_execute(r)
            self.raw = nil

            if let err = result.pointee.err {
                let message = String(cString: err)
                map(QueryResult.error(QueryError.executionFailed(message: message)))
                return
            }
            guard let cols = result.pointee.ok else {
                return
            }
            let rowList = ColResult(raw: cols)
            for row in rowList {
                map(QueryResult.success(row))
            }
        }
    }

    func executeTuple(callback: @escaping (QueryResult<TupleResult?>) -> Void) throws {
        guard let r = self.raw else {
            throw QueryError.builderConsumed
        }

        DispatchQueue.global(qos: .background).async {
            let result = query_builder_execute_tuple(r)
            self.raw = nil

            if let err = result.pointee.err {
                let message = String(cString: err)
                callback(QueryResult.error(QueryError.executionFailed(message: message)))
            }
            guard let results = result.pointee.ok else {
                callback(QueryResult.success(nil))
                return
            }
            callback(QueryResult.success(TupleResult(raw: OpaquePointer(results))))
        }
    }

    override func cleanup(pointer: OpaquePointer) {
        query_builder_destroy(pointer)
    }
}
