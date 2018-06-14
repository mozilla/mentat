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

protocol Destroyable {
    func cleanup(pointer: OpaquePointer)
}

/**
 Base class that wraps an non-optional `OpaquePointer` representing a pointer to a Rust object.
 This class provides cleanup functions on deinit, ensuring that all classes
 that inherit from it will have their `OpaquePointer` destroyed when the Swift wrapper is destroyed.
 If a class does not override `cleanup` then a `fatalError` is thrown.
 */
open class RustObject: Destroyable {
    var raw: OpaquePointer

    public init(raw: OpaquePointer) {
        self.raw = raw
    }

    public init(raw: UnsafeMutableRawPointer) {
        self.raw = OpaquePointer(raw)
    }

    public init?(raw: OpaquePointer?) {
        guard let r = raw else {
            return nil
        }
        self.raw = r
    }

    public func getRaw() -> OpaquePointer {
        return self.raw
    }

    deinit {
        self.cleanup(pointer: self.raw)
    }

    open func cleanup(pointer: OpaquePointer) {
        fatalError("\(cleanup) is not implemented.")
    }
}
