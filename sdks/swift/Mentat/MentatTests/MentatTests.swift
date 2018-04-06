/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

import XCTest
@testable import Mentat

class MentatTests: XCTestCase {
    
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
}
