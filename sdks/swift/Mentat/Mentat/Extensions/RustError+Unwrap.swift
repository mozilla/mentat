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

extension RustError {

    @discardableResult
    public static func unwrap(_ callback: (UnsafeMutablePointer<RustError>) throws -> OpaquePointer?) throws -> OpaquePointer {
        var err = RustError(message: nil)
        guard let result = try callback(&err) else {
            if let message = err.message {
                throw ResultError.error(message: String(destroyingRustString: message))
            }
            throw ResultError.empty
        }
        return result;
    }

    @discardableResult
    public static func tryUnwrap(_ callback: (UnsafeMutablePointer<RustError>) throws -> OpaquePointer?) throws -> OpaquePointer? {
        var err = RustError(message: nil)
        guard let result = try callback(&err) else {
            if let message = err.message {
                throw ResultError.error(message: String(destroyingRustString: message))
            }
            return nil
        }
        return result;
    }

    public static func withErrorCheck(_ callback: (UnsafeMutablePointer<RustError>) throws -> Void) throws {
        var err = RustError(message: nil)
        try callback(&err);
        if let message = err.message {
            throw ResultError.error(message: String(destroyingRustString: message))
        }
    }
}

