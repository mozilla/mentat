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

public extension String {
    /** Helper to construct a String from a Rust string without leaking it. */
    public init(destroyingRustString rustCString: UnsafeMutablePointer<CChar>) {
        defer { rust_c_string_destroy(rustCString); }
        self.init(cString: rustCString)
    }
}
