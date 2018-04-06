///* This Source Code Form is subject to the terms of the Mozilla Public
// * License, v. 2.0. If a copy of the MPL was not distributed with this
// * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

import Foundation

extension Date {
    func toMicroseconds() -> Int64 {
        return Int64(self.timeIntervalSince1970 * 1_000_000)
    }
}
