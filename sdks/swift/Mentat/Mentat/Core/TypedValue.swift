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
 A wrapper around Mentat's `TypedValue` Rust object. This class wraps a raw pointer to a Rust `TypedValue`
 struct and provides accessors to the values according to expected result type.

 As the FFI functions for fetching values are consuming, this class keeps a copy of the result internally after
 fetching so that the value can be referenced several times.

 Also, due to the consuming nature of the FFI layer, this class also manages it's raw pointer, nilling it after calling the
 FFI conversion function so that the underlying base class can manage cleanup.
 */
open class TypedValue: OptionalRustObject {

    private var value: Any?

    /**
    The `ValueType` for this `TypedValue`.
     - Returns: The `ValueType` for this `TypedValue`.
     */
    var valueType: ValueType {
        return typed_value_value_type(self.raw!)
    }

    private func isConsumed() -> Bool {
        return self.raw == nil
    }

    /**
    This value as a `Int64`. This function will panic if the `ValueType` of this `TypedValue`
    is not a `Long`

     - Returns: the value of this `TypedValue` as a `Int64`
    */
    open func asLong() -> Int64 {
        defer {
            self.raw = nil
        }
        if !self.isConsumed() {
            self.value = typed_value_into_long(self.raw!)
        }
        return self.value as! Int64
    }

    /**
     This value as an `Entid`. This function will panic if the `ValueType` of this `TypedValue`
     is not a `Ref`

     - Returns: the value of this `TypedValue` as an `Entid`
     */
    open func asEntid() -> Entid {
        defer {
            self.raw = nil
        }

        if !self.isConsumed() {
            self.value = typed_value_into_entid(self.raw!)
        }
        return self.value as! Entid
    }

    /**
     This value as a keyword `String`. This function will panic if the `ValueType` of this `TypedValue`
     is not a `Keyword`

     - Returns: the value of this `TypedValue` as a keyword `String`
     */
    open func asKeyword() -> String {
        defer {
            self.raw = nil
        }

        if !self.isConsumed() {
            self.value = String(destroyingRustString: typed_value_into_kw(self.raw!))
        }
        return self.value as! String
    }

    /**
     This value as a `Bool`. This function will panic if the `ValueType` of this `TypedValue`
     is not a `Boolean`

     - Returns: the value of this `TypedValue` as a `Bool`
     */
    open func asBool() -> Bool {
        defer {
            self.raw = nil
        }

        if !self.isConsumed() {
            let v = typed_value_into_boolean(self.raw!)
            self.value =  v > 0
        }
        return self.value as! Bool
    }

    /**
     This value as a `Double`. This function will panic if the `ValueType` of this `TypedValue`
     is not a `Double`

     - Returns: the value of this `TypedValue` as a `Double`
     */
    open func asDouble() -> Double {
        defer {
            self.raw = nil
        }

        if !self.isConsumed() {
            self.value = typed_value_into_double(self.raw!)
        }
        return self.value as! Double
    }

    /**
     This value as a `Date`. This function will panic if the `ValueType` of this `TypedValue`
     is not a `Instant`

     - Returns: the value of this `TypedValue` as a `Date`
     */
    open func asDate() -> Date {
        defer {
            self.raw = nil
        }

        if !self.isConsumed() {
            let timestamp = typed_value_into_timestamp(self.raw!)
            self.value = Date(timeIntervalSince1970: TimeInterval(timestamp))
        }
        return self.value as! Date
    }

    /**
     This value as a `String`. This function will panic if the `ValueType` of this `TypedValue`
     is not a `String`

     - Returns: the value of this `TypedValue` as a `String`
     */
    open func asString() -> String {
        defer {
            self.raw = nil
        }

        if !self.isConsumed() {
            self.value = String(destroyingRustString: typed_value_into_string(self.raw!));
        }
        return self.value as! String
    }

    /**
     This value as a `UUID`. This function will panic if the `ValueType` of this `TypedValue`
     is not a `Uuid`

     - Returns: the value of this `TypedValue` as a `UUID?`. If the `UUID` is not valid then this function returns nil.
     */
    open func asUUID() -> UUID? {
        defer {
            self.raw = nil
        }

        if !self.isConsumed() {
            let bytes = typed_value_into_uuid(self.raw!);
            self.value = UUID(destroyingRustUUID: bytes);
        }
        return self.value as! UUID?
    }

    override open func cleanup(pointer: OpaquePointer) {
        typed_value_destroy(pointer)
    }
}
