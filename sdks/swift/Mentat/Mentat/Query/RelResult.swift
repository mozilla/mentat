/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

import Foundation
import Mentatlib

enum QueryResultError: Error {
    case resultsConsumed
}

class RelResult: OptionalRustObject {
    private func getRaw() throws -> OpaquePointer {
        guard let r = self.raw else {
            throw QueryResultError.resultsConsumed
        }
        return r
    }

    func row(index: Int32) throws -> TupleResult? {
        guard let row = row_at_index(try self.getRaw(), index) else {
            return nil
        }
        return TupleResult(raw: row)
    }

    override func cleanup(pointer: OpaquePointer) {
        destroy(UnsafeMutableRawPointer(pointer))
    }
}

class RelResultIterator: OptionalRustObject, IteratorProtocol  {
    typealias Element = TupleResult

    init(iter: OpaquePointer?) {
        super.init(raw: iter)
    }

    func next() -> Element? {
        guard let iter = self.raw,
            let rowPtr = rows_iter_next(iter) else {
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
            let rowIter = rows_iter(try self.getRaw())
            self.raw = nil
            return RelResultIterator(iter: rowIter)
        } catch {
            return RelResultIterator(iter: nil)
        }
    }
}
