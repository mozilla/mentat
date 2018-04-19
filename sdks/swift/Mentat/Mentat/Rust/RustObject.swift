/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

import Foundation
import Mentatlib

protocol Destroyable {
    func cleanup(pointer: OpaquePointer)
}

public class RustObject: Destroyable {
    var raw: OpaquePointer

    lazy var uniqueId: ObjectIdentifier = {
        ObjectIdentifier(self)
    }()

    init(raw: OpaquePointer) {
        self.raw = raw
    }

    init(raw: UnsafeMutableRawPointer) {
        self.raw = OpaquePointer(raw)
    }

    init?(raw: OpaquePointer?) {
        guard let r = raw else {
            return nil
        }
        self.raw = r
    }

    func intoRaw() -> OpaquePointer {
        return self.raw
    }

    deinit {
        self.cleanup(pointer: self.raw)
    }
    
    func cleanup(pointer: OpaquePointer) {
        fatalError("\(cleanup) is not implemented.")
    }
}
