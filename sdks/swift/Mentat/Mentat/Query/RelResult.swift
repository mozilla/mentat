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
 Wraps a `Rel` result from a Mentat query.
 A `Rel` result is a list of rows of `TypedValues`.
 Individual rows can be fetched or the set can be iterated.

 To fetch individual rows from a `RelResult` use `row(Int32)`.

 ```
 query.run { rows in
    let row1 = rows.row(0)
    let row2 = rows.row(1)
 }
 ```

 To iterate over the result set use standard iteration flows.
 ```
 query.run { rows in
     rows.forEach { row in
         ...
     }
 }
 ```

 Note that iteration is consuming and can only be done once.
 */
class RelResult: OptionalRustObject {

    /**
     Fetch the row at the requested index.

     - Parameter index: the index of the row to be fetched

     - Throws: `PointerError.pointerConsumed` if the result set has already been iterated.

     - Returns: The row at the requested index as a `TupleResult`, if present, or nil if there is no row at that index.
     */
    func row(index: Int32) throws -> TupleResult? {
        guard let row = row_at_index(try self.validPointer(), index) else {
            return nil
        }
        return TupleResult(raw: row)
    }

    override func cleanup(pointer: OpaquePointer) {
        destroy(UnsafeMutableRawPointer(pointer))
    }
}

/**
 Iterator for `RelResult`.

 To iterate over the result set use standard iteration flows.
 ```
 query.run { result in
     rows.forEach { row in
        ...
     }
 }
 ```

 Note that iteration is consuming and can only be done once.
 */
class RelResultIterator: OptionalRustObject, IteratorProtocol  {
    typealias Element = TupleResult

    init(iter: OpaquePointer?) {
        super.init(raw: iter)
    }

    func next() -> Element? {
        guard let iter = self.raw,
            let rowPtr = typed_value_result_set_iter_next(iter) else {
            return nil
        }
        return TupleResult(raw: rowPtr)
    }

    override func cleanup(pointer: OpaquePointer) {
        typed_value_result_set_iter_destroy(pointer)
    }
}

extension RelResult: Sequence {
    func makeIterator() -> RelResultIterator {
        do {
            let rowIter = typed_value_result_set_into_iter(try self.validPointer())
            self.raw = nil
            return RelResultIterator(iter: rowIter)
        } catch {
            return RelResultIterator(iter: nil)
        }
    }
}
