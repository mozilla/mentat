/* Copyright 2018 Mozilla
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License. */

import Foundation
import MentatStore


/**
 This class allows you to construct a query, bind values to variables and run those queries against a mentat DB.

 This class cannot be created directly, but must be created through `Mentat.query(String:)`.

 The types of values you can bind are
 - `Int64`
 - `Entid`
 - `Keyword`
 - `Bool`
 - `Double`
 - `Date`
 - `String`
 - `UUID`.

 Each bound variable must have a corresponding value in the query string used to create this query.

 ```
 let query = """
            [:find ?name ?cat
             :in ?type
             :where
             [?c :community/name ?name]
             [?c :community/type ?type]
             [?c :community/category ?cat]]
            """
 mentat.query(query: query)
        .bind(varName: "?type", toKeyword: ":community.type/website")
        .run { result in
             ...
         }
 ```

 Queries can be run and the results returned in a number of different formats. Individual result values are returned as `TypedValues` and
 the format differences relate to the number and structure of those values. The result format is related to the format provided in the query string.

 - `Rel` - This is the default `run` function and returns a list of rows of values. Queries that wish to have `Rel` results should format their query strings:
 ```
 let query = """
             [: find ?a ?b ?c
              : where ... ]
            """
 mentat.query(query: query)
     .run { result in
        ...
     }
 ```
 - `Scalar` - This returns a single value as a result. This can be optional, as the value may not be present. Queries that wish to have `Scalar` results should format their query strings:
 ```
 let query = """
             [: find ?a .
              : where ... ]
             """
 mentat.query(query: query)
     .runScalar { result in
        ...
     }
 ```
 - `Coll` - This returns a list of single values as a result.  Queries that wish to have `Coll` results should format their query strings:
 ```
 let query = """
             [: find [?a ...]
              : where ... ]
             """
 mentat.query(query: query)
         .runColl { result in
            ...
         }
 ```
 - `Tuple` - This returns a single row of values.  Queries that wish to have `Tuple` results should format their query strings:
 ```
 let query = """
             [: find [?a ?b ?c]
              : where ... ]
            """
 mentat.query(query: query)
     .runTuple { result in
        ...
     }
 ```
 */
class Query: OptionalRustObject {

    /**
     Binds a `Int64` value to the provided variable name.

     - Parameter varName: The name of the variable in the format `?name`.
     - Parameter value: The value to be bound

     - Throws: `PointerError.pointerConsumed` if the underlying raw pointer has already consumed, which will occur if the query has already been executed.

     - Returns: This `Query` such that further function can be called.
     */
    func bind(varName: String, toLong value: Int64) throws -> Query {
        query_builder_bind_long(try! self.validPointer(), varName, value)
        return self
    }

    /**
     Binds a `Entid` value to the provided variable name.

     - Parameter varName: The name of the variable in the format `?name`.
     - Parameter value: The value to be bound

     - Throws: `PointerError.pointerConsumed` if the underlying raw pointer has already consumed, which will occur if the query has already been executed.

     - Returns: This `Query` such that further function can be called.
     */
    func bind(varName: String, toReference value: Entid) throws -> Query {
        query_builder_bind_ref(try! self.validPointer(), varName, value)
        return self
    }

    /**
     Binds a `String` value representing a keyword for an attribute to the provided variable name.
     Keywords take the format `:namespace/name`.

     - Parameter varName: The name of the variable in the format `?name`.
     - Parameter value: The value to be bound

     - Throws: `PointerError.pointerConsumed` if the underlying raw pointer has already consumed, which will occur if the query has already been executed.

     - Returns: This `Query` such that further function can be called.
     */
    func bind(varName: String, toReference value: String) throws -> Query {
        query_builder_bind_ref_kw(try! self.validPointer(), varName, value)
        return self
    }

    /**
     Binds a keyword `String` value to the provided variable name.
     Keywords take the format `:namespace/name`.

     - Parameter varName: The name of the variable in the format `?name`.
     - Parameter value: The value to be bound

     - Throws: `PointerError.pointerConsumed` if the underlying raw pointer has already consumed, which will occur if the query has already been executed.

     - Returns: This `Query` such that further function can be called.
     */
    func bind(varName: String, toKeyword value: String) throws -> Query {
        query_builder_bind_kw(try! self.validPointer(), varName, value)
        return self
    }

    /**
     Binds a `Bool` value to the provided variable name.

     - Parameter varName: The name of the variable in the format `?name`.
     - Parameter value: The value to be bound

     - Throws: `PointerError.pointerConsumed` if the underlying raw pointer has already consumed, which will occur if the query has already been executed.

     - Returns: This `Query` such that further function can be called.
     */
    func bind(varName: String, toBoolean value: Bool) throws -> Query {
        query_builder_bind_boolean(try! self.validPointer(), varName, value ? 1 : 0)
        return self
    }

    /**
     Binds a `Double` value to the provided variable name.

     - Parameter varName: The name of the variable in the format `?name`.
     - Parameter value: The value to be bound

     - Throws: `PointerError.pointerConsumed` if the underlying raw pointer has already consumed, which will occur if the query has already been executed.

     - Returns: This `Query` such that further function can be called.
     */
    func bind(varName: String, toDouble value: Double) throws -> Query {
        query_builder_bind_double(try! self.validPointer(), varName, value)
        return self
    }

    /**
     Binds a `Date` value to the provided variable name.

     - Parameter varName: The name of the variable in the format `?name`.
     - Parameter value: The value to be bound

     - Throws: `PointerError.pointerConsumed` if the underlying raw pointer has already consumed, which will occur if the query has already been executed.

     - Returns: This `Query` such that further function can be called.
     */
    func bind(varName: String, toDate value: Date) throws -> Query {
        query_builder_bind_timestamp(try! self.validPointer(), varName, value.toMicroseconds())
        return self
    }

    /**
     Binds a `String` value to the provided variable name.

     - Parameter varName: The name of the variable in the format `?name`.
     - Parameter value: The value to be bound

     - Throws: `PointerError.pointerConsumed` if the underlying raw pointer has already consumed, which will occur if the query has already been executed.

     - Returns: This `Query` such that further function can be called.
     */
    func bind(varName: String, toString value: String) throws -> Query {
        query_builder_bind_string(try! self.validPointer(), varName, value)
        return self
    }

    /**
     Binds a `UUID` value to the provided variable name.

     - Parameter varName: The name of the variable in the format `?name`.
     - Parameter value: The value to be bound

     - Throws: `PointerError.pointerConsumed` if the underlying raw pointer has already consumed, which will occur if the query has already been executed.

     - Returns: This `Query` such that further function can be called.
     */
    func bind(varName: String, toUuid value: UUID) throws -> Query {
        var rawUuid = value.uuid
        withUnsafePointer(to: &rawUuid) { uuidPtr in
            query_builder_bind_uuid(try! self.validPointer(), varName, uuidPtr)
        }
        return self
    }

    /**
     Execute the query with the values bound associated with this `Query` and call the provided callback function with the results as a list of rows of `TypedValues`.

     - Parameter callback: the function to call with the results of this query

     - Throws: `QueryError.executionFailed` if the query fails to execute. This could be because the provided query did not parse, or that
     variable we incorrectly bound, or that the query provided was not `Rel`.
     - Throws: `PointerError.pointerConsumed` if the underlying raw pointer has already consumed, which will occur if the query has previously been executed.
     */
    func run(callback: @escaping (RelResult?) -> Void) throws {
        let result = query_builder_execute(try! self.validPointer())
        self.raw = nil

        if let err = result.pointee.err {
            let message = String(cString: err)
            throw QueryError.executionFailed(message: message)
        }
        guard let results = result.pointee.ok else {
            callback(nil)
            return
        }
        callback(RelResult(raw: results))
    }

    /**
     Execute the query with the values bound associated with this `Query` and call the provided callback function with the result as a single `TypedValue`.

     - Parameter callback: the function to call with the results of this query

     - Throws: `QueryError.executionFailed` if the query fails to execute. This could be because the provided query did not parse, that
     variable we incorrectly bound, or that the query provided was not `Scalar`.
     - Throws: `PointerError.pointerConsumed` if the underlying raw pointer has already consumed, which will occur if the query has previously been executed.
     */
    func runScalar(callback: @escaping (TypedValue?) -> Void) throws {
        let result = query_builder_execute_scalar(try! self.validPointer())
        self.raw = nil

        if let err = result.pointee.err {
            let message = String(cString: err)
            throw QueryError.executionFailed(message: message)
        }
        guard let results = result.pointee.ok else {
            callback(nil)
            return
        }
        callback(TypedValue(raw: OpaquePointer(results)))
    }

    /**
     Execute the query with the values bound associated with this `Query` and call the provided callback function with the result as a list of single `TypedValues`.

     - Parameter callback: the function to call with the results of this query

     - Throws: `QueryError.executionFailed` if the query fails to execute. This could be because the provided query did not parse, that
     variable we incorrectly bound, or that the query provided was not `Coll`.
     - Throws: `PointerError.pointerConsumed` if the underlying raw pointer has already consumed, which will occur if the query has previously been executed.
     */
    func runColl(callback: @escaping (ColResult?) -> Void) throws {
        let result = query_builder_execute_coll(try! self.validPointer())
        self.raw = nil

        if let err = result.pointee.err {
            let message = String(cString: err)
             throw QueryError.executionFailed(message: message)
        }
        guard let results = result.pointee.ok else {
            callback(nil)
            return
        }
        callback(ColResult(raw: results))
    }

    /**
     Execute the query with the values bound associated with this `Query` and call the provided callback function with the result as a list of single `TypedValues`.

     - Parameter callback: the function to call with the results of this query

     - Throws: `QueryError.executionFailed` if the query fails to execute. This could be because the provided query did not parse, that
     variable we incorrectly bound, or that the query provided was not `Tuple`.
     - Throws: `PointerError.pointerConsumed` if the underlying raw pointer has already consumed, which will occur if the query has previously been executed.
     */
    func runTuple(callback: @escaping (TupleResult?) -> Void) throws {
        let result = query_builder_execute_tuple(try! self.validPointer())
        self.raw = nil

        if let err = result.pointee.err {
            let message = String(cString: err)
            throw QueryError.executionFailed(message: message)
        }
        guard let results = result.pointee.ok else {
            callback(nil)
            return
        }
        callback(TupleResult(raw: OpaquePointer(results)))
    }

    override func cleanup(pointer: OpaquePointer) {
        query_builder_destroy(pointer)
    }
}
