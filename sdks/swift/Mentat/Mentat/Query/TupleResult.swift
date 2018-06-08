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
 Wraps a `Tuple` result from a Mentat query.
 A `Tuple` result is a list of `TypedValues`.
 Individual values can be fetched as `TypedValues` or converted into a requested type.

 Values can be fetched as one of the following types:
 - `TypedValue`
 - `Int64`
 - `Entid`
 - `Keyword`
 - `Bool`
 - `Double`
 - `Date`
 - `String`
 - `UUID`.
 */
open class TupleResult: OptionalRustObject {

    /**
     Return the `TypedValue` at the specified index.
     If the index is greater than the number of values then this function will crash.

     - Parameter index: The index of the value to fetch.

     - Returns: The `TypedValue` at that index.
     */
    open func get(index: Int) -> TypedValue {
        return TypedValue(raw: value_at_index(self.raw!, Int32(index)))
    }

    /**
     Return the `Int64` at the specified index.
     If the index is greater than the number of values then this function will crash.
     If the value type if the `TypedValue` at this index is not `Long` then this function will crash.

     - Parameter index: The index of the value to fetch.

     - Returns: The `Int64` at that index.
     */
    open func asLong(index: Int) -> Int64 {
        return value_at_index_into_long(self.raw!, Int32(index))
    }

    /**
     Return the `Entid` at the specified index.
     If the index is greater than the number of values then this function will crash.
     If the value type if the `TypedValue` at this index is not `Ref` then this function will crash.

     - Parameter index: The index of the value to fetch.

     - Returns: The `Entid` at that index.
     */
    open func asEntid(index: Int) -> Entid {
        return value_at_index_into_entid(self.raw!, Int32(index))
    }

    /**
     Return the keyword `String` at the specified index.
     If the index is greater than the number of values then this function will crash.
     If the value type if the `TypedValue` at this index is not `Keyword` then this function will crash.

     - Parameter index: The index of the value to fetch.

     - Returns: The keyword `String` at that index.
     */
    open func asKeyword(index: Int) -> String {
        return String(cString: value_at_index_into_kw(self.raw!, Int32(index)))
    }

    /**
     Return the `Bool` at the specified index.
     If the index is greater than the number of values then this function will crash.
     If the value type if the `TypedValue` at this index is not `Boolean` then this function will crash.

     - Parameter index: The index of the value to fetch.

     - Returns: The `Bool` at that index.
     */
    open func asBool(index: Int) -> Bool {
        return value_at_index_into_boolean(self.raw!, Int32(index)) == 0 ? false : true
    }

    /**
     Return the `Double` at the specified index.
     If the index is greater than the number of values then this function will crash.
     If the value type if the `TypedValue` at this index is not `Double` then this function will crash.

     - Parameter index: The index of the value to fetch.

     - Returns: The `Double` at that index.
     */
    open func asDouble(index: Int) -> Double {
        return value_at_index_into_double(self.raw!, Int32(index))
    }

    /**
     Return the `Date` at the specified index.
     If the index is greater than the number of values then this function will crash.
     If the value type if the `TypedValue` at this index is not `Instant` then this function will crash.

     - Parameter index: The index of the value to fetch.

     - Returns: The `Date` at that index.
     */
    open func asDate(index: Int) -> Date {
        return Date(timeIntervalSince1970: TimeInterval(value_at_index_into_timestamp(self.raw!, Int32(index))))
    }

    /**
     Return the `String` at the specified index.
     If the index is greater than the number of values then this function will crash.
     If the value type if the `TypedValue` at this index is not `String` then this function will crash.

     - Parameter index: The index of the value to fetch.

     - Returns: The `String` at that index.
     */
    open func asString(index: Int) -> String {
        return String(cString: value_at_index_into_string(self.raw!, Int32(index)))
    }

    /**
     Return the `UUID` at the specified index.
     If the index is greater than the number of values then this function will crash.
     If the value type if the `TypedValue` at this index is not `Uuid` then this function will crash.

     - Parameter index: The index of the value to fetch.

     - Returns: The `UUID` at that index.
     */
    open func asUUID(index: Int) -> UUID? {
        return UUID(uuid: value_at_index_into_uuid(self.raw!, Int32(index)).pointee)
    }

    override open func cleanup(pointer: OpaquePointer) {
        typed_value_list_destroy(pointer)
    }
}

/**
 Wraps a `Coll` result from a Mentat query.
 A `Coll` result is a list of rows of single values of type `TypedValue`.
 Values for individual rows can be fetched as `TypedValue` or converted into a requested type.

 Row values can be fetched as one of the following types:
 - `TypedValue`
 - `Int64`
 - `Entid`
 - `Keyword`
 - `Bool`
 - `Double`
 - `Date`
 - `String`
 - `UUID`.
 */
open class ColResult: TupleResult {
}

/**
 Iterator for `ColResult`.

 To iterate over the result set use standard iteration flows.
 ```
 query.runColl { rows in
     rows.forEach { value in
        ...
     }
 }
 ```

 Note that iteration is consuming and can only be done once.
 */
open class ColResultIterator: OptionalRustObject, IteratorProtocol  {
    public typealias Element = TypedValue

    init(iter: OpaquePointer?) {
        super.init(raw: iter)
    }

    open func next() -> Element? {
        guard let iter = self.raw,
            let rowPtr = typed_value_list_iter_next(iter) else {
                return nil
        }
        return TypedValue(raw: rowPtr)
    }

    override open func cleanup(pointer: OpaquePointer) {
        typed_value_list_iter_destroy(pointer)
    }
}

extension ColResult: Sequence {
    open func makeIterator() -> ColResultIterator {
        defer {
            self.raw = nil
        }
        guard let raw = self.raw else {
            return ColResultIterator(iter: nil)
        }
        let rowIter = typed_value_list_into_iter(raw)
        return ColResultIterator(iter: rowIter)
    }
}
