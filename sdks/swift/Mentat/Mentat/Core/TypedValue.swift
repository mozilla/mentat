/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

import Foundation
import Mentatlib

class TypedValue: OptionalRustObject {

    var valueType: ValueType {
        return typed_value_value_type(self.raw!)
    }

    func asLong() -> Int64 {
        defer {
            self.raw = nil
        }
        return typed_value_as_long(self.raw!)
    }

    func asEntid() -> Int64 {
        defer {
            self.raw = nil
        }
        return typed_value_as_entid(self.raw!)
    }

    func asKeyword() -> String {
        defer {
            self.raw = nil
        }
        return String(cString: typed_value_as_kw(self.raw!))
    }

    func asBool() -> Bool {
        defer {
            self.raw = nil
        }
        let v = typed_value_as_boolean(self.raw!)
        return  v > 0
    }

    func asDouble() -> Double {
        defer {
            self.raw = nil
        }
        return typed_value_as_double(self.raw!)
    }

    func asDate() -> Date {
        defer {
            self.raw = nil
        }
        let timestamp = typed_value_as_timestamp(self.raw!)
        return Date(timeIntervalSince1970: TimeInterval(timestamp))
    }

    func asString() -> String {
        defer {
            self.raw = nil
        }
        return String(cString: typed_value_as_string(self.raw!))
    }

    func asUUID() -> UUID? {
        defer {
            self.raw = nil
        }
        return UUID(uuidString: String(cString: typed_value_as_uuid(self.raw!)))
    }

    override func cleanup(pointer: OpaquePointer) {
        typed_value_destroy(pointer)
    }
}
