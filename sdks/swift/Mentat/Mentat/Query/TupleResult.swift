/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

import Foundation
import Mentatlib

class TupleResult: OptionalRustObject {

    func get(index: Int) -> TypedValue {
        return TypedValue(raw: value_at_index(self.raw!, Int32(index)))
    }

    func asLong(index: Int) -> Int64 {
        return value_at_index_as_long(self.raw!, Int32(index))
    }

    func asEntid(index: Int) -> Int64 {
        return value_at_index_as_entid(self.raw!, Int32(index))
    }

    func asKeyword(index: Int) -> String {
        return String(cString: value_at_index_as_kw(self.raw!, Int32(index)))
    }

    func asBool(index: Int) -> Bool {
        return value_at_index_as_boolean(self.raw!, Int32(index)) == 0 ? false : true
    }

    func asDouble(index: Int) -> Double {
        return value_at_index_as_double(self.raw!, Int32(index))
    }

    func asDate(index: Int) -> Date {
        return Date(timeIntervalSince1970: TimeInterval(value_at_index_as_timestamp(self.raw!, Int32(index))))
    }

    func asString(index: Int) -> String {
        return String(cString: value_at_index_as_string(self.raw!, Int32(index)))
    }

    func asUUID(index: Int) -> UUID? {
        return UUID(uuidString: String(cString: value_at_index_as_uuid(self.raw!, Int32(index))))
    }

    override func cleanup(pointer: OpaquePointer) {
        typed_value_list_destroy(pointer)
    }
}

class ColResult: TupleResult {
}

class ColResultIterator: OptionalRustObject, IteratorProtocol  {
    typealias Element = TypedValue

    init(iter: OpaquePointer?) {
        super.init(raw: iter)
    }

    func next() -> Element? {
        guard let iter = self.raw,
            let rowPtr = values_iter_next(iter) else {
                return nil
        }
        return TypedValue(raw: rowPtr)
    }

    override func cleanup(pointer: OpaquePointer) {
        typed_value_list_iter_destroy(pointer)
    }
}

extension ColResult: Sequence {
    func makeIterator() -> ColResultIterator {
        defer {
            self.raw = nil
        }
        guard let raw = self.raw else {
            return ColResultIterator(iter: nil)
        }
        let rowIter = values_iter(raw)
        return ColResultIterator(iter: rowIter)
    }
}
