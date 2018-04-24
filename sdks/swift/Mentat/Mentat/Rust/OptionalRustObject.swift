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
 Base class that wraps an optional `OpaquePointer` representing a pointer to a Rust object.
 This class should be used to wrap Rust pointer that point to consuming structs, that is, calling a function
 for that Rust pointer, will cause Rust to destroy the pointer, leaving the Swift pointer dangling.
 These classes are responsible for ensuring that their raw `OpaquePointer` are `nil`led after calling a consuming
 FFI function.
 This class provides cleanup functions on deinit, ensuring that all classes
 that inherit from it will have their `OpaquePointer` destroyed when the Swift wrapper is destroyed.
 If a class does not override `cleanup` then a `fatalError` is thrown.
 The optional pointer is managed here such that is the pointer is nil, then the cleanup function is not called
 ensuring that we do not double free the pointer on exit.
 */
class OptionalRustObject: Destroyable {
    var raw: OpaquePointer?
    lazy var uniqueId: ObjectIdentifier = {
         ObjectIdentifier(self)
    }()

    init(raw: UnsafeMutableRawPointer) {
        self.raw = OpaquePointer(raw)
    }

    init(raw: OpaquePointer?) {
        self.raw = raw
    }

    func intoRaw() -> OpaquePointer? {
        return self.raw
    }

    deinit {
        guard let raw = self.raw else { return }
        self.cleanup(pointer: raw)
    }

    /**
    Provides a non-optional `OpaquePointer` if one exists for this class.

    - Throws: `Pointer.pointerConsumed` if the raw pointer wrapped by this class is nil

    - Returns: the raw `OpaquePointer` wrapped by this class.
    */
    func validPointer() throws -> OpaquePointer {
        guard let r = self.raw else {
            throw PointerError.pointerConsumed
        }

        return r
    }

    func cleanup(pointer: OpaquePointer) {
        fatalError("\(cleanup) is not implemented.")
    }
}
