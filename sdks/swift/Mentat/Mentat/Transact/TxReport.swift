//
/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

import Foundation

import Mentatlib

class TxReport: RustObject {

    public var txId: Int64 {
        return tx_report_get_entid(self.raw)
    }

    public var txInstant: Date {
        return Date(timeIntervalSince1970: TimeInterval(tx_report_get_tx_instant(self.raw)))
    }

    public func entidForTmpId(tmpId: String) -> Int64? {
        guard let entidPtr = tx_report_entity_for_temp_id(self.raw, tmpId) else {
            return nil
        }
        return entidPtr.pointee
    }

    override func cleanup(pointer: OpaquePointer) {
        tx_report_destroy(pointer)
    }
}
