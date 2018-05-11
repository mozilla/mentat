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

extension Result {
    /**
     Force unwraps a result.
     Expects there to be a value attached and throws an error is there is not.

     - Throws: `ResultError.error` if the result contains an error
     - Throws: `ResultError.empty` if the result contains no error but also no result.

     - Returns: The pointer to the successful result value.
     */
    @discardableResult public func unwrap() throws -> UnsafeMutableRawPointer {
        guard let success = self.ok else {
            if let error = self.err {
                throw ResultError.error(message: String(cString: error))
            }
            throw ResultError.empty
        }
        return success
    }

    /**
     Unwraps an optional result, yielding either a successful value or a nil.

     - Throws: `ResultError.error` if the result contains an error

     - Returns: The pointer to the successful result value, or nil if no value is present.
     */
    @discardableResult public func tryUnwrap() throws -> UnsafeMutableRawPointer? {
        guard let success = self.ok else {
            if let error = self.err {
                throw ResultError.error(message: String(cString: error))
            }
            return nil
        }
        return success
    }
}
