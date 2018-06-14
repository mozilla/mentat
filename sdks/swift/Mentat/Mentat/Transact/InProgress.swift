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
 This class wraps a raw pointer that points to a Rust `InProgress` object.

 `InProgress` allows for multiple transacts to be performed in a single transaction.
 Each transact performed results in a `TxReport` that can be used to gather information
 to be used in subsequent transacts.

 Committing an `InProgress` commits all the transacts that have been performed using
 that `InProgress`.

 Rolling back and `InProgress` rolls back all the transacts that have been performed
 using that `InProgress`.

 ```
 do {
     let inProgress = try mentat.beginTransaction()
     let txReport = try inProgress.transact(transaction: "[[:db/add "a" :foo/long 22]]")
     let aEntid = txReport.entid(forTempId: "a")
     let report = try inProgress.transact(transaction: "[[:db/add "b" :foo/ref \(aEntid)] [:db/add "b" :foo/boolean true]]")
     try inProgress.commit()
 } catch {
    ...
 }
 ```

 `InProgress` also provides a number of functions to generating an builder to assert datoms programatically.
 The two types of builder are `InProgressBuilder` and `EntityBuilder`.

 `InProgressBuilder` takes the current `InProgress` and provides a programmatic interface to add
 and retract values from entities for which there exists an `Entid`. The provided `InProgress`
 is used to perform the transacts.

 ```
 let aEntid = txReport.entid(forTempId: "a")
 let bEntid = txReport.entid(forTempId: "b")
 do {
     let inProgress = try mentat.beginTransaction()
     let builder = try inProgress.builder()
     try builder.add(entid: bEntid, keyword: ":foo/boolean", boolean: true)
     try builder.add(entid: aEntid, keyword: ":foo/instant", date: newDate)
     let (inProgress, report) = try builder.transact()
     try inProgress.transact(transaction: "[[:db/add \(aEntid) :foo/long 22]]")
     try inProgress.commit()
 } catch {
    ...
 }
 ```

`EntityBuilder` takes the current `InProgress` and either an `Entid` or a `tempid` to provide
 a programmatic interface to add and retract values from a specific entity. The provided `InProgress`
 is used to perform the transacts.

 ```
 do {
     let transaction = try mentat.beginTransaction()
     let builder = try transaction.builder(forTempId: "b")
     try builder.add(keyword: ":foo/boolean", boolean: true)
     try builder.add(keyword: ":foo/instant", date: newDate)
     let (inProgress, report) = try builder.transact()
     let bEntid = report.entid(forTempId: "b")
     try inProgress.transact(transaction: "[[:db/add \(bEntid) :foo/long 22]]")
     try inProgress.commit()
 } catch {
    ...
 }
 ```
 */
open class InProgress: OptionalRustObject {

    /**
     Creates an `InProgressBuilder` using this `InProgress`.

     - Throws: `PointerError.pointerConsumed` if the underlying raw pointer has already consumed, which will occur if the `InProgress`
     has already been committed, or converted into a Builder.

     - Returns: an `InProgressBuilder` for this `InProgress`
     */
    open func builder() throws -> InProgressBuilder {
        defer {
            self.raw = nil
        }
        return InProgressBuilder(raw: in_progress_builder(try self.validPointer()))
    }

    /**
     Creates an `EntityBuilder` using this `InProgress` for the entity with `entid`.

     - Parameter entid: The `Entid` for this entity.

     - Throws: `PointerError.pointerConsumed` if the underlying raw pointer has already consumed, which will occur if the `InProgress`
     has already been committed, or converted into a Builder.

     - Returns: an `EntityBuilder` for this `InProgress`
     */
    open func builder(forEntid entid: Int64) throws -> EntityBuilder {
        defer {
            self.raw = nil
        }
        return EntityBuilder(raw: in_progress_entity_builder_from_entid(try self.validPointer(), entid))
    }

    /**
     Creates an `EntityBuilder` using this `InProgress` for a new entity with `tempId`.

     - Parameter tempId: The temporary identifier for this entity.

     - Throws: `PointerError.pointerConsumed` if the underlying raw pointer has already consumed, which will occur if the `InProgress`
     has already been committed, or converted into a Builder.

     - Returns: an `EntityBuilder` for this `InProgress`
     */
    open func builder(forTempId tempId: String) throws -> EntityBuilder {
        defer {
            self.raw = nil
        }
        return EntityBuilder(raw: in_progress_entity_builder_from_temp_id(try self.validPointer(), tempId))
    }

    /**
     Transacts the `transaction`

     This does not commit the transaction. In order to do so, `commit` can be called.

     - Parameter transaction: The EDN string to be transacted.

     - Throws: `PointerError.pointerConsumed` if the underlying raw pointer has already consumed, which will occur if the builder
     has already been transacted or committed.
     - Throws: `ResultError.error` if the transaction failed.
     - Throws: `ResultError.empty` if no `TxReport` is returned from the transact.

     - Returns: The `TxReport` generated by the transact.
     */
    open func transact(transaction: String) throws -> TxReport {
        let result = in_progress_transact(try self.validPointer(), transaction)
        return TxReport(raw: try result.unwrap())
    }

    /**
         Commits all the transacts that have been performed on this `InProgress`, either directly
         or through a Builder.

     - Throws: `PointerError.pointerConsumed` if the underlying raw pointer has already consumed, which will occur if the builder
     has already been transacted or committed.
     - Throws: `ResultError.error` if the commit failed.
     */
    open func commit() throws {
        defer {
            self.raw = nil
        }
        try in_progress_commit(try self.validPointer()).tryUnwrap()
    }

    /**
     Rolls back all the transacts that have been performed on this `InProgress`, either directly
     or through a Builder.

     - Throws: `PointerError.pointerConsumed` if the underlying raw pointer has already consumed, which will occur if the builder
     has already been transacted or committed.
     - Throws: `ResultError.error` if the rollback failed.
     */
    open func rollback() throws {
        defer {
            self.raw = nil
        }
        try in_progress_rollback(try self.validPointer()).tryUnwrap()
    }

    override open func cleanup(pointer: OpaquePointer) {
        in_progress_destroy(pointer)
    }
}
