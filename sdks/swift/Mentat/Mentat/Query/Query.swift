/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

import Foundation
import Mentatlib

class Query: OptionalRustObject {

    func bind(varName: String, toLong value: Int64) throws -> Query {
        guard let r = self.raw else {
            throw QueryError.builderConsumed
        }
        query_builder_bind_long(r, varName, value)
        return self
    }

    func bind(varName: String, toReference value: Int64) throws -> Query {
        guard let r = self.raw else {
            throw QueryError.builderConsumed
        }
        query_builder_bind_ref(r, varName, value)
        return self
    }

    func bind(varName: String, toReference value: String) throws -> Query {
        guard let r = self.raw else {
            throw QueryError.builderConsumed
        }
        query_builder_bind_ref_kw(r, varName, value)
        return self
    }

    func bind(varName: String, toKeyword value: String) throws -> Query {
        guard let r = self.raw else {
            throw QueryError.builderConsumed
        }
        query_builder_bind_kw(r, varName, value)
        return self
    }

    func bind(varName: String, toBoolean value: Bool) throws -> Query {
        guard let r = self.raw else {
            throw QueryError.builderConsumed
        }
        query_builder_bind_boolean(r, varName, value ? 1 : 0)
        return self
    }

    func bind(varName: String, toDouble value: Double) throws -> Query {
        guard let r = self.raw else {
            throw QueryError.builderConsumed
        }
        query_builder_bind_double(r, varName, value)
        return self
    }

    func bind(varName: String, toDate value: Date) throws -> Query {
        guard let r = self.raw else {
            throw QueryError.builderConsumed
        }
        query_builder_bind_timestamp(r, varName, value.toMicroseconds())
        return self
    }

    func bind(varName: String, toString value: String) throws -> Query {
        guard let r = self.raw else {
            throw QueryError.builderConsumed
        }
        query_builder_bind_string(r, varName, value)
        return self
    }

    func bind(varName: String, toUuid value: UUID) throws -> Query {
        guard let r = self.raw else {
            throw QueryError.builderConsumed
        }
        query_builder_bind_uuid(r, varName, value.uuidString)
        return self
    }

    func executeMap(map: @escaping (TupleResult?, QueryError?) -> Void) throws {
        guard let r = self.raw else {
            throw QueryError.builderConsumed
        }

        DispatchQueue.global(qos: .background).async {
            let result = query_builder_execute(r)
            self.raw = nil

            if let err = result.pointee.err {
                let message = String(cString: err)
                map(nil, QueryError.executionFailed(message: message))
                return
            }
            guard let rowsPtr = result.pointee.ok else {
                return
            }
            let rows = RelResult(raw: rowsPtr)
            for row in rows {
                map(row, nil)
            }
        }
    }

    func execute(callback: @escaping (RelResult?, QueryError?) -> Void) throws {
        guard let r = self.raw else {
            throw QueryError.builderConsumed
        }

        DispatchQueue.global(qos: .background).async {
            let result = query_builder_execute(r)
            self.raw = nil

            if let err = result.pointee.err {
                let message = String(cString: err)
                callback(nil, QueryError.executionFailed(message: message))
                return
            }
            guard let results = result.pointee.ok else {
                callback(nil, nil)
                return
            }
            callback(RelResult(raw: results), nil)
        }
    }

    func executeScalar(callback: @escaping (TypedValue?, QueryError?) -> Void) throws {
        guard let r = self.raw else {
            throw QueryError.builderConsumed
        }

        DispatchQueue.global(qos: .background).async {
            let result = query_builder_execute_scalar(r)
            self.raw = nil

            if let err = result.pointee.err {
                let message = String(cString: err)
                callback(nil, QueryError.executionFailed(message: message))
            }
            guard let results = result.pointee.ok else {
                callback(nil, nil)
                return
            }
            callback(TypedValue(raw: OpaquePointer(results)), nil)
        }
    }

    func executeColl(callback: @escaping (ColResult?, QueryError?) -> Void) throws {
        guard let r = self.raw else {
            throw QueryError.builderConsumed
        }

        DispatchQueue.global(qos: .background).async {
            let result = query_builder_execute_coll(r)
            self.raw = nil

            if let err = result.pointee.err {
                let message = String(cString: err)
                callback(nil, QueryError.executionFailed(message: message))
            }
            guard let results = result.pointee.ok else {
                callback(nil, nil)
                return
            }
            callback(ColResult(raw: results), nil)
        }
    }

    func executeCollMap(map: @escaping (TypedValue?, QueryError?) -> Void) throws {
        guard let r = self.raw else {
            throw QueryError.builderConsumed
        }

        DispatchQueue.global(qos: .background).async {
            let result = query_builder_execute(r)
            self.raw = nil

            if let err = result.pointee.err {
                let message = String(cString: err)
                map(nil, QueryError.executionFailed(message: message))
                return
            }
            guard let cols = result.pointee.ok else {
                return
            }
            let rowList = ColResult(raw: cols)
            for row in rowList {
                map(row, nil)
            }
        }
    }

    func executeTuple(callback: @escaping (TupleResult?, QueryError?) -> Void) throws {
        guard let r = self.raw else {
            throw QueryError.builderConsumed
        }

        DispatchQueue.global(qos: .background).async {
            let result = query_builder_execute_tuple(r)
            self.raw = nil

            if let err = result.pointee.err {
                let message = String(cString: err)
                callback(nil, QueryError.executionFailed(message: message))
            }
            guard let results = result.pointee.ok else {
                callback(nil, nil)
                return
            }
            callback(TupleResult(raw: OpaquePointer(results)), nil)
        }
    }

    override func cleanup(pointer: OpaquePointer) {
        query_builder_destroy(pointer)
    }
}
