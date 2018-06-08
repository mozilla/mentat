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
 This class wraps a raw pointer that points to a Rust `TxReport` object.

 The `TxReport` contains information about a successful Mentat transaction.

 This information includes:
 - `txId` - the identifier for the transaction.
 - `txInstant` - the time that the transaction occured.
 - a map of temporary identifiers provided in the transaction and the `Entid`s that they were mapped to,

 Access an `Entid` for a temporary identifier that was provided in the transaction can be done through `entid(String:)`.

 ```
 let report = mentat.transact("[[:db/add "a" :foo/boolean true]]")
 let aEntid = report.entid(forTempId: "a")
 ```
 */
open class TxReport: RustObject {

    // The identifier for the transaction.
    open var txId: Entid {
        return tx_report_get_entid(self.raw)
    }

    // The time that the transaction occured.
    open var txInstant: Date {
        return Date(timeIntervalSince1970: TimeInterval(tx_report_get_tx_instant(self.raw)))
    }

    /**
     Access an `Entid` for a temporary identifier that was provided in the transaction.

     - Parameter tempId: A `String` representing the temporary identifier to fetch the `Entid` for.

     - Returns: The `Entid` for the temporary identifier, if present, otherwise `nil`.
    */
    open func entid(forTempId tempId: String) -> Entid? {
        guard let entidPtr = tx_report_entity_for_temp_id(self.raw, tempId) else {
            return nil
        }
        return entidPtr.pointee
    }

    override open func cleanup(pointer: OpaquePointer) {
        tx_report_destroy(pointer)
    }
}
