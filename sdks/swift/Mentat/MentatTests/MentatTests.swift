/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

import XCTest
@testable import Mentat

class MentatTests: XCTestCase {

    var schema: String?
    
    override func setUp() {
        super.setUp()
        // Put setup code here. This method is called before the invocation of each test method in the class.
    }
    
    override func tearDown() {
        // Put teardown code here. This method is called after the invocation of each test method in the class.
        super.tearDown()
    }

    // test that a store can be opened in memory
    func testOpenInMemoryStore() {
        XCTAssertNotNil(Mentat().intoRaw())
    }

    // test that a store can be opened in a specific location
    func testOpenStoreInLocation() {
        let paths = FileManager.default.urls(for: .documentDirectory, in: .userDomainMask)
        let documentsURL = paths[0]
        let storeURI = documentsURL.appendingPathComponent("test.db", isDirectory: false).absoluteString
        XCTAssertNotNil(Mentat(storeURI: storeURI).intoRaw())
    }

    func readSchema() throws -> String {
        guard let schema = self.schema else {
            let bundle = Bundle(for: type(of: self))
            guard let schemaPath = bundle.path(forResource: "cities", ofType: "schema") else { return "" }
            let schema = try String(contentsOf: URL(fileURLWithPath: schemaPath))
            self.schema = schema
            return schema
        }

        return schema
    }

    func testTransactVocabulary() {
        do {
            let vocab = try readSchema()
            let mentat = Mentat()
            let success = try mentat.transact(transaction: vocab)
            assert( success )
        } catch {
            assertionFailure(error.localizedDescription)
        }
    }

    // TODO: Add more tests once we are able to add vocabulary and transact entities
}
