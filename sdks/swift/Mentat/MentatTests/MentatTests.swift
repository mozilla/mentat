/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

import XCTest

@testable import Mentat

class MentatTests: XCTestCase {

    var schema: String?
    var edn: String?
    var store: Mentat?
    
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
            let schemaUrl = bundle.url(forResource: "cities", withExtension: "schema", subdirectory: "fixtures")!
            let schema = try String(contentsOf: schemaUrl)
            self.schema = schema
            return schema
        }

        return schema
    }

    func readEdn() throws -> String {
        guard let edn = self.edn else {
            let bundle = Bundle(for: type(of: self))
            let ednUrl = bundle.url(forResource: "all_seattle", withExtension: "edn", subdirectory: "fixtures")!
            let edn = try String(contentsOf: ednUrl)
            self.edn = edn
            return edn
        }

        return edn
    }

    func transactSchema(mentat: Mentat) throws -> TxReport {
        let vocab = try readSchema()
        let report = try mentat.transact(transaction: vocab)
        return report
    }

    func transactEdn(mentat: Mentat) throws -> TxReport {
        let edn = try readEdn()
        let report = try mentat.transact(transaction: edn)
        return report
    }

    func newStore() -> Mentat {
        guard let mentat = self.store else {
            let mentat = Mentat()
            let _ = try! self.transactSchema(mentat: mentat)
            let _ = try! self.transactEdn(mentat: mentat)
            self.store = mentat
            return mentat
        }

        return mentat
    }

    func populateWithTypesSchema(mentat: Mentat) -> TxReport? {
        do {
            let schema = """
            [
                [:db/add "b" :db/ident :foo/boolean]
                [:db/add "b" :db/valueType :db.type/boolean]
                [:db/add "b" :db/cardinality :db.cardinality/one]
                [:db/add "l" :db/ident :foo/long]
                [:db/add "l" :db/valueType :db.type/long]
                [:db/add "l" :db/cardinality :db.cardinality/one]
                [:db/add "r" :db/ident :foo/ref]
                [:db/add "r" :db/valueType :db.type/ref]
                [:db/add "r" :db/cardinality :db.cardinality/one]
                [:db/add "i" :db/ident :foo/instant]
                [:db/add "i" :db/valueType :db.type/instant]
                [:db/add "i" :db/cardinality :db.cardinality/one]
                [:db/add "d" :db/ident :foo/double]
                [:db/add "d" :db/valueType :db.type/double]
                [:db/add "d" :db/cardinality :db.cardinality/one]
                [:db/add "s" :db/ident :foo/string]
                [:db/add "s" :db/valueType :db.type/string]
                [:db/add "s" :db/cardinality :db.cardinality/one]
                [:db/add "k" :db/ident :foo/keyword]
                [:db/add "k" :db/valueType :db.type/keyword]
                [:db/add "k" :db/cardinality :db.cardinality/one]
                [:db/add "u" :db/ident :foo/uuid]
                [:db/add "u" :db/valueType :db.type/uuid]
                [:db/add "u" :db/cardinality :db.cardinality/one]
            ]
            """
            let report = try mentat.transact(transaction: schema)
            let stringEntid = report.entidForTmpId(tmpId: "s")!

            let data = """
            [
                [:db/add "a" :foo/boolean true]
                [:db/add "a" :foo/long 25]
                [:db/add "a" :foo/instant #inst "2017-01-01T11:00:00.000Z"]
                [:db/add "a" :foo/double 11.23]
                [:db/add "a" :foo/string "The higher we soar the smaller we appear to those who cannot fly."]
                [:db/add "a" :foo/keyword :foo/string]
                [:db/add "a" :foo/uuid #uuid "550e8400-e29b-41d4-a716-446655440000"]
                [:db/add "b" :foo/boolean false]
                [:db/add "b" :foo/ref \(stringEntid)]
                [:db/add "b" :foo/long 50]
                [:db/add "b" :foo/instant #inst "2018-01-01T11:00:00.000Z"]
                [:db/add "b" :foo/double 22.46]
                [:db/add "b" :foo/string "Silence is worse; all truths that are kept silent become poisonous."]
                [:db/add "b" :foo/uuid #uuid "4cb3f828-752d-497a-90c9-b1fd516d5644"]
            ]
            """
            return try mentat.transact(transaction: data)
        } catch {
            assertionFailure(error.localizedDescription)
        }
        return nil
    }

    func test1TransactVocabulary() {
        do {
            let mentat = Mentat()
            let report = try transactSchema(mentat: mentat)
            XCTAssertNotNil(report)
            assert(report.txId > 0)
        } catch {
            assertionFailure(error.localizedDescription)
        }
    }

    func test2TransactEntities() {
        do {
            let mentat = Mentat()
            let _ = try self.transactSchema(mentat: mentat)
            let report = try self.transactEdn(mentat: mentat)
            XCTAssertNotNil(report)
            assert(report.txId > 0)
            let entid = report.entidForTmpId(tmpId: "a17592186045438")
            assert(entid == 65566)
        } catch {
            assertionFailure(error.localizedDescription)
        }
    }

    func testQueryScalar() {
        let mentat = newStore()
        let query = "[:find ?n . :in ?name :where [(fulltext $ :community/name ?name) [[?e ?n]]]]"
        let expect = expectation(description: "Query is executed")
        try! mentat.query(query: query).bind(varName: "?name", toString: "Wallingford").executeScalar(callback: { (scalarResult, error) in
            assert(error == nil, "Unexpected error: \(String(describing: error))")
            guard let result = scalarResult?.asString() else {
                return assertionFailure("No String value received")
            }
            assert(result == "KOMO Communities - Wallingford")
            expect.fulfill()
        })
        waitForExpectations(timeout: 1) { error in
            if let error = error {
                assertionFailure("waitForExpectationsWithTimeout errored: \(error)")
            }
        }
    }

    func testQueryColl() {
        let mentat = newStore()
        let query = "[:find [?when ...] :where [_ :db/txInstant ?when] :order (asc ?when)]"
        let expect = expectation(description: "Query is executed")
        try! mentat.query(query: query).executeColl(callback: { (collResult, error) in
            assert(error == nil, "Unexpected error: \(String(describing: error))")
            guard let rows = collResult else {
                return assertionFailure("No results received")
            }
            // we are expecting 3 results
            for i in 0..<3 {
                let _ = rows.asDate(index: i)
                assert(true)
            }
            expect.fulfill()
        })
        waitForExpectations(timeout: 1) { error in
            if let error = error {
                assertionFailure("waitForExpectationsWithTimeout errored: \(error)")
            }
        }
    }

    func testQueryCollResultIterator() {
        let mentat = newStore()
        let query = "[:find [?when ...] :where [_ :db/txInstant ?when] :order (asc ?when)]"
        let expect = expectation(description: "Query is executed")
        try! mentat.query(query: query).executeColl(callback: { (collResult, error) in
            assert(error == nil, "Unexpected error: \(String(describing: error))")
            guard let rows = collResult else {
                return assertionFailure("No results received")
            }

            rows.forEach({ (value) in
                assert(value.valueType.rawValue == 2)
            })
            expect.fulfill()
        })
        waitForExpectations(timeout: 1) { error in
            if let error = error {
                assertionFailure("waitForExpectationsWithTimeout errored: \(error)")
            }
        }
    }

    func testQueryTuple() {
        let mentat = newStore()
        let query = """
        [:find [?name ?cat]
        :where
        [?c :community/name ?name]
        [?c :community/type :community.type/website]
        [(fulltext $ :community/category "food") [[?c ?cat]]]]
        """
        let expect = expectation(description: "Query is executed")
        try! mentat.query(query: query).executeTuple(callback: { (tupleResult, error) in
            assert(error == nil, "Unexpected error: \(String(describing: error))")
            guard let tuple = tupleResult else {
                return assertionFailure("expecting a result")
            }
            let name = tuple.asString(index: 0)
            let category = tuple.asString(index: 1)
            assert(name == "Community Harvest of Southwest Seattle")
            assert(category == "sustainable food")
            expect.fulfill()
        })
        waitForExpectations(timeout: 1) { error in
            if let error = error {
                assertionFailure("waitForExpectationsWithTimeout errored: \(error)")
            }
        }
    }

    func testQueryRel() {
        let mentat = newStore()
        let query = """
        [:find ?name ?cat
        :where
        [?c :community/name ?name]
        [?c :community/type :community.type/website]
        [(fulltext $ :community/category "food") [[?c ?cat]]]]
        """
        let expect = expectation(description: "Query is executed")
        let expectedResults = [("InBallard", "food"),
                               ("Seattle Chinatown Guide", "food"),
                               ("Community Harvest of Southwest Seattle", "sustainable food"),
                               ("University District Food Bank", "food bank")]
        try! mentat.query(query: query).execute(callback: { (relResult, error) in
            assert(error == nil, "Unexpected error: \(String(describing: error))")
            guard let rows = relResult else {
                return assertionFailure("No results received")
            }

            for (i, row) in rows.enumerated() {
                let (name, category) = expectedResults[i]
                assert( row.asString(index: 0) == name)
                assert(row.asString(index: 1) == category)
            }
            expect.fulfill()
        })
        waitForExpectations(timeout: 1) { error in
            if let error = error {
                assertionFailure("waitForExpectationsWithTimeout errored: \(error)")
            }
        }
    }

    func testQueryRelResultIterator() {
        let mentat = newStore()
        let query = """
        [:find ?name ?cat
        :where
        [?c :community/name ?name]
        [?c :community/type :community.type/website]
        [(fulltext $ :community/category "food") [[?c ?cat]]]]
        """
        let expect = expectation(description: "Query is executed")
        let expectedResults = [("InBallard", "food"),
                               ("Seattle Chinatown Guide", "food"),
                               ("Community Harvest of Southwest Seattle", "sustainable food"),
                               ("University District Food Bank", "food bank")]
        try! mentat.query(query: query).execute(callback: { (relResult, error) in
            assert(error == nil, "Unexpected error: \(String(describing: error))")
            guard let rows = relResult else {
                return assertionFailure("No results received")
            }
            
            var i = 0
            rows.forEach({ (row) in
                let (name, category) = expectedResults[i]
                i += 1
                assert(row.asString(index: 0) == name)
                assert(row.asString(index: 1) == category)
            })
            assert(i == 4)
            expect.fulfill()
        })
        waitForExpectations(timeout: 1) { error in
            if let error = error {
                assertionFailure("waitForExpectationsWithTimeout errored: \(error)")
            }
        }
    }

    func testBindLong() {
        let mentat = Mentat()
        let report = self.populateWithTypesSchema(mentat: mentat)!
        let aEntid = report.entidForTmpId(tmpId: "a")
        let query = "[:find ?e . :in ?bool :where [?e :foo/boolean ?bool]]"
        let expect = expectation(description: "Query is executed")
        try! mentat.query(query: query)
              .bind(varName: "?bool", toBoolean: true)
              .executeScalar { (value, error) in
                assert(error == nil, "Unexpected error: \(String(describing: error))")
                XCTAssertNotNil(value)
                assert(value?.asEntid() == aEntid)
                expect.fulfill()
        }
        waitForExpectations(timeout: 1) { error in
            if let error = error {
                assertionFailure("waitForExpectationsWithTimeout errored: \(error)")
            }
        }
    }

    func testBindRef() {
        let mentat = Mentat()
        let report = self.populateWithTypesSchema(mentat: mentat)!
        let stringEntid = mentat.entidForAttribute(attribute: ":foo/string")
        let bEntid = report.entidForTmpId(tmpId: "b")
        let query = "[:find ?e . :in ?ref :where [?e :foo/ref ?ref]]"
        let expect = expectation(description: "Query is executed")
        try! mentat.query(query: query)
            .bind(varName: "?ref", toReference: stringEntid)
            .executeScalar { (value, error) in
                assert(error == nil, "Unexpected error: \(String(describing: error))")
                XCTAssertNotNil(value)
                assert(value?.asEntid() == bEntid)
                expect.fulfill()
        }
        waitForExpectations(timeout: 1) { error in
            if let error = error {
                assertionFailure("waitForExpectationsWithTimeout errored: \(error)")
            }
        }
    }

    func testBindKwRef() {
        let mentat = Mentat()
        let report = self.populateWithTypesSchema(mentat: mentat)!
        let bEntid = report.entidForTmpId(tmpId: "b")
        let query = "[:find ?e . :in ?ref :where [?e :foo/ref ?ref]]"
        let expect = expectation(description: "Query is executed")
        try! mentat.query(query: query)
            .bind(varName: "?ref", toReference: ":foo/string")
            .executeScalar { (value, error) in
                assert(error == nil, "Unexpected error: \(String(describing: error))")
                XCTAssertNotNil(value)
                assert(value?.asEntid() == bEntid)
                expect.fulfill()
        }
        waitForExpectations(timeout: 1) { error in
            if let error = error {
                assertionFailure("waitForExpectationsWithTimeout errored: \(error)")
            }
        }
    }

    func testBindKw() {
        let mentat = Mentat()
        let report = self.populateWithTypesSchema(mentat: mentat)!
        let aEntid = report.entidForTmpId(tmpId: "a")
        let query = "[:find ?e . :in ?kw :where [?e :foo/keyword ?kw]]"
        let expect = expectation(description: "Query is executed")
        try! mentat.query(query: query)
            .bind(varName: "?kw", toKeyword: ":foo/string")
            .executeScalar { (value, error) in
                assert(error == nil, "Unexpected error: \(String(describing: error))")
                XCTAssertNotNil(value)
                assert(value?.asEntid() == aEntid)
                expect.fulfill()
        }
        waitForExpectations(timeout: 1) { error in
            if let error = error {
                assertionFailure("waitForExpectationsWithTimeout errored: \(error)")
            }
        }
    }

    func testBindDate() {
        let mentat = Mentat()
        let report = self.populateWithTypesSchema(mentat: mentat)!
        let aEntid = report.entidForTmpId(tmpId: "a")
        let query = "[:find [?e ?d] :in ?now :where [?e :foo/instant ?d] [(< ?d ?now)]]"
        let expect = expectation(description: "Query is executed")
        try! mentat.query(query: query)
            .bind(varName: "?now", toDate: Date())
            .executeTuple { (row, error) in
                assert(error == nil, "Unexpected error: \(String(describing: error))")
                XCTAssertNotNil(row)
                assert(row?.asEntid(index: 0) == aEntid)
                expect.fulfill()
        }
        waitForExpectations(timeout: 1) { error in
            if let error = error {
                assertionFailure("waitForExpectationsWithTimeout errored: \(error)")
            }
        }
    }


    func testBindString() {
        let mentat = newStore()
        let query = "[:find ?n . :in ?name :where [(fulltext $ :community/name ?name) [[?e ?n]]]]"
        let expect = expectation(description: "Query is executed")
        try! mentat.query(query: query)
                   .bind(varName: "?name", toString: "Wallingford")
                   .executeScalar(callback: { (scalarResult, error) in
            assert(error == nil, "Unexpected error: \(String(describing: error))")
            guard let result = scalarResult?.asString() else {
                return assertionFailure("No String value received")
            }
            assert(result == "KOMO Communities - Wallingford")
            expect.fulfill()
        })
        waitForExpectations(timeout: 1) { error in
            if let error = error {
                assertionFailure("waitForExpectationsWithTimeout errored: \(error)")
            }
        }
    }

    func testBindUuid() {
        let mentat = Mentat()
        let report = self.populateWithTypesSchema(mentat: mentat)!
        let aEntid = report.entidForTmpId(tmpId: "a")
        let query = "[:find ?e . :in ?uuid :where [?e :foo/uuid ?uuid]]"
        let uuid = UUID(uuidString: "550e8400-e29b-41d4-a716-446655440000")!
        let expect = expectation(description: "Query is executed")
        try! mentat.query(query: query)
            .bind(varName: "?uuid", toUuid: uuid)
            .executeScalar { (value, error) in
                assert(error == nil, "Unexpected error: \(String(describing: error))")
                XCTAssertNotNil(value)
                assert(value?.asEntid() == aEntid)
                expect.fulfill()
        }
        waitForExpectations(timeout: 1) { error in
            if let error = error {
                assertionFailure("waitForExpectationsWithTimeout errored: \(error)")
            }
        }
    }

    func testBindBoolean() {
        let mentat = Mentat()
        let report = self.populateWithTypesSchema(mentat: mentat)!
        let aEntid = report.entidForTmpId(tmpId: "a")
        let query = "[:find ?e . :in ?bool :where [?e :foo/boolean ?bool]]"
        let expect = expectation(description: "Query is executed")
        try! mentat.query(query: query)
            .bind(varName: "?bool", toBoolean: true)
            .executeScalar { (value, error) in
                assert(error == nil, "Unexpected error: \(String(describing: error))")
                XCTAssertNotNil(value)
                assert(value?.asEntid() == aEntid)
                expect.fulfill()
        }
        waitForExpectations(timeout: 1) { error in
            if let error = error {
                assertionFailure("waitForExpectationsWithTimeout errored: \(error)")
            }
        }
    }

    func testBindDouble() {
        let mentat = Mentat()
        let report = self.populateWithTypesSchema(mentat: mentat)!
        let aEntid = report.entidForTmpId(tmpId: "a")
        let query = "[:find ?e . :in ?double :where [?e :foo/double ?double]]"
        let expect = expectation(description: "Query is executed")
        try! mentat.query(query: query)
            .bind(varName: "?double", toDouble: 11.23)
            .executeScalar { (value, error) in
                assert(error == nil, "Unexpected error: \(String(describing: error))")
                XCTAssertNotNil(value)
                assert(value?.asEntid() == aEntid)
                expect.fulfill()
        }
        waitForExpectations(timeout: 1) { error in
            if let error = error {
                assertionFailure("waitForExpectationsWithTimeout errored: \(error)")
            }
        }
    }

    func testTypedValueAsLong() {
        let mentat = Mentat()
        let report = self.populateWithTypesSchema(mentat: mentat)!
        let aEntid = report.entidForTmpId(tmpId: "a")!
        let query = "[:find ?v . :in ?e :where [?e :foo/long ?v]]"
        let expect = expectation(description: "Query is executed")
        try! mentat.query(query: query)
            .bind(varName: "?e", toReference: aEntid)
            .executeScalar { (value, error) in
                assert(error == nil, "Unexpected error: \(String(describing: error))")
                XCTAssertNotNil(value)
                assert(value?.asLong() == 25)
                expect.fulfill()
        }
        waitForExpectations(timeout: 1) { error in
            if let error = error {
                assertionFailure("waitForExpectationsWithTimeout errored: \(error)")
            }
        }
    }

    func testTypedValueAsRef() {
        let mentat = Mentat()
        let report = self.populateWithTypesSchema(mentat: mentat)!
        let aEntid = report.entidForTmpId(tmpId: "a")!
        let query = "[:find ?e . :where [?e :foo/long 25]]"
        let expect = expectation(description: "Query is executed")
        try! mentat.query(query: query)
            .executeScalar { (value, error) in
                assert(error == nil, "Unexpected error: \(String(describing: error))")
                XCTAssertNotNil(value)
                assert(value?.asEntid() == aEntid)
                expect.fulfill()
        }
        waitForExpectations(timeout: 1) { error in
            if let error = error {
                assertionFailure("waitForExpectationsWithTimeout errored: \(error)")
            }
        }
    }

    func testTypedValueAsKw() {
        let mentat = Mentat()
        let report = self.populateWithTypesSchema(mentat: mentat)!
        let aEntid = report.entidForTmpId(tmpId: "a")!
        let query = "[:find ?v . :in ?e :where [?e :foo/keyword ?v]]"
        let expect = expectation(description: "Query is executed")
        try! mentat.query(query: query)
            .bind(varName: "?e", toReference: aEntid)
            .executeScalar { (value, error) in
                assert(error == nil, "Unexpected error: \(String(describing: error))")
                XCTAssertNotNil(value)
                assert(value?.asKeyword() == ":foo/string")
                expect.fulfill()
        }
        waitForExpectations(timeout: 1) { error in
            if let error = error {
                assertionFailure("waitForExpectationsWithTimeout errored: \(error)")
            }
        }
    }

    func testTypedValueAsBoolean() {
        let mentat = Mentat()
        let report = self.populateWithTypesSchema(mentat: mentat)!
        let aEntid = report.entidForTmpId(tmpId: "a")!
        let query = "[:find ?v . :in ?e :where [?e :foo/boolean ?v]]"
        let expect = expectation(description: "Query is executed")
        try! mentat.query(query: query)
            .bind(varName: "?e", toReference: aEntid)
            .executeScalar { (value, error) in
                assert(error == nil, "Unexpected error: \(String(describing: error))")
                XCTAssertNotNil(value)
                assert(value?.asBool() == true)
                expect.fulfill()
        }
        waitForExpectations(timeout: 1) { error in
            if let error = error {
                assertionFailure("waitForExpectationsWithTimeout errored: \(error)")
            }
        }
    }

    func testTypedValueAsDouble() {
        let mentat = Mentat()
        let report = self.populateWithTypesSchema(mentat: mentat)!
        let aEntid = report.entidForTmpId(tmpId: "a")!
        let query = "[:find ?v . :in ?e :where [?e :foo/double ?v]]"
        let expect = expectation(description: "Query is executed")
        try! mentat.query(query: query)
            .bind(varName: "?e", toReference: aEntid)
            .executeScalar { (value, error) in
                assert(error == nil, "Unexpected error: \(String(describing: error))")
                XCTAssertNotNil(value)
                assert(value?.asDouble() == 11.23)
                expect.fulfill()
        }
        waitForExpectations(timeout: 1) { error in
            if let error = error {
                assertionFailure("waitForExpectationsWithTimeout errored: \(error)")
            }
        }
    }

    func testTypedValueAsDate() {
        let mentat = Mentat()
        let report = self.populateWithTypesSchema(mentat: mentat)!
        let aEntid = report.entidForTmpId(tmpId: "a")!
        let query = "[:find ?v . :in ?e :where [?e :foo/instant ?v]]"
        let expect = expectation(description: "Query is executed")

        let formatter = DateFormatter()
        formatter.dateFormat = "yyyy-MM-dd'T'HH:mm:ssZZZZZ"
        let expectedDate = formatter.date(from: "2017-01-01T11:00:00+00:00")

        try! mentat.query(query: query)
            .bind(varName: "?e", toReference: aEntid)
            .executeScalar { (value, error) in
                assert(error == nil, "Unexpected error: \(String(describing: error))")
                XCTAssertNotNil(value)
                assert(value?.asDate() == expectedDate)
                expect.fulfill()
        }
        waitForExpectations(timeout: 1) { error in
            if let error = error {
                assertionFailure("waitForExpectationsWithTimeout errored: \(error)")
            }
        }
    }

    func testTypedValueAsString() {
        let mentat = Mentat()
        let report = self.populateWithTypesSchema(mentat: mentat)!
        let aEntid = report.entidForTmpId(tmpId: "a")!
        let query = "[:find ?v . :in ?e :where [?e :foo/string ?v]]"
        let expect = expectation(description: "Query is executed")
        try! mentat.query(query: query)
            .bind(varName: "?e", toReference: aEntid)
            .executeScalar { (value, error) in
                assert(error == nil, "Unexpected error: \(String(describing: error))")
                XCTAssertNotNil(value)
                assert(value?.asString() == "The higher we soar the smaller we appear to those who cannot fly.")
                expect.fulfill()
        }
        waitForExpectations(timeout: 1) { error in
            if let error = error {
                assertionFailure("waitForExpectationsWithTimeout errored: \(error)")
            }
        }
    }

    func testTypedValueAsUuid() {
        let mentat = Mentat()
        let report = self.populateWithTypesSchema(mentat: mentat)!
        let aEntid = report.entidForTmpId(tmpId: "a")!
        let query = "[:find ?v . :in ?e :where [?e :foo/uuid ?v]]"
        let expectedUuid = UUID(uuidString: "550e8400-e29b-41d4-a716-446655440000")!
        let expect = expectation(description: "Query is executed")
        try! mentat.query(query: query)
            .bind(varName: "?e", toReference: aEntid)
            .executeScalar { (value, error) in
                assert(error == nil, "Unexpected error: \(String(describing: error))")
                XCTAssertNotNil(value)
                assert(value?.asUUID() == expectedUuid)
                expect.fulfill()
        }
        waitForExpectations(timeout: 1) { error in
            if let error = error {
                assertionFailure("waitForExpectationsWithTimeout errored: \(error)")
            }
        }
    }

    func testValueForAttributeOfEntity() {
        let mentat = Mentat()
        let report = self.populateWithTypesSchema(mentat: mentat)!
        let aEntid = report.entidForTmpId(tmpId: "a")!
        let value = mentat.value(forAttribute: ":foo/long", ofEntity: aEntid)
        XCTAssertNotNil(value)
        assert(value?.asLong() == 25)
    }

    func testEntidForAttribute() {
        let mentat = Mentat()
        let _ = self.populateWithTypesSchema(mentat: mentat)!
        let entid = mentat.entidForAttribute(attribute: ":foo/long")
        assert(entid == 65540)
    }

    func testSetLongForAttributeOfEntity() {
        let mentat = Mentat()
        let report = self.populateWithTypesSchema(mentat: mentat)!
        let aEntid = report.entidForTmpId(tmpId: "a")!
        let attr = ":foo/long"
        let pre = mentat.value(forAttribute: attr, ofEntity: aEntid)
        XCTAssertNotNil(pre)
        assert(pre?.asLong() == 25)
        XCTAssertNoThrow(try mentat.set(long: 100, forAttribute: attr, onEntity: aEntid))
        let post = mentat.value(forAttribute: attr, ofEntity: aEntid)
        XCTAssertNotNil(post)
        assert(post?.asLong() == 100)
    }

    func testSetBooleanForAttributeOfEntity() {
        let mentat = Mentat()
        let report = self.populateWithTypesSchema(mentat: mentat)!
        let aEntid = report.entidForTmpId(tmpId: "a")!
        let attr = ":foo/boolean"
        let pre = mentat.value(forAttribute: attr, ofEntity: aEntid)
        XCTAssertNotNil(pre)
        assert(pre?.asBool() == true)
        XCTAssertNoThrow(try mentat.set(boolean: false, forAttribute: attr, onEntity: aEntid))
        let post = mentat.value(forAttribute: attr, ofEntity: aEntid)
        XCTAssertNotNil(post)
        let p = post?.asBool()
        assert(p == false)
    }

    func testSetRefForAttributeOfEntity() {
        let mentat = Mentat()
        let report = self.populateWithTypesSchema(mentat: mentat)!
        let aEntid = report.entidForTmpId(tmpId: "a")!
        let bEntid = report.entidForTmpId(tmpId: "b")!
        let attr = ":foo/ref"
        let pre = mentat.value(forAttribute: attr, ofEntity: aEntid)
        XCTAssertNil(pre)
        XCTAssertNoThrow(try mentat.set(reference: bEntid, forAttribute: attr, onEntity: aEntid))
        let post = mentat.value(forAttribute: attr, ofEntity: aEntid)
        XCTAssertNotNil(post)
        assert(post?.asEntid() == bEntid)
    }

    func testSetRefKwForAttributeOfEntity() {
        let mentat = Mentat()
        let report = self.populateWithTypesSchema(mentat: mentat)!
        let aEntid = report.entidForTmpId(tmpId: "a")!
        let attr = ":foo/ref"
        let pre = mentat.value(forAttribute: attr, ofEntity: aEntid)
        XCTAssertNil(pre)
        XCTAssertNoThrow(try mentat.setKeywordReference(value: ":foo/long", forAttribute: attr, onEntity: aEntid))
        let post = mentat.value(forAttribute: attr, ofEntity: aEntid)
        XCTAssertNotNil(post)
        assert(post?.asEntid() == 65540)
    }

    func testSetDateForAttributeOfEntity() {
        let mentat = Mentat()
        let report = self.populateWithTypesSchema(mentat: mentat)!
        let aEntid = report.entidForTmpId(tmpId: "a")!
        let attr = ":foo/instant"

        let formatter = DateFormatter()
        formatter.dateFormat = "yyyy-MM-dd'T'HH:mm:ssZZZZZ"
        let previousDate = formatter.date(from: "2017-01-01T11:00:00+00:00")

        let pre = mentat.value(forAttribute: attr, ofEntity: aEntid)
        XCTAssertNotNil(pre)
        assert(pre?.asDate() == previousDate)

        let now = Date()
        XCTAssertNoThrow(try mentat.set(date: now, forAttribute: attr, onEntity: aEntid))
        let post = mentat.value(forAttribute: attr, ofEntity: aEntid)
        XCTAssertNotNil(post)
        let p = post?.asDate()

        assert(formatter.string(from: p!) == formatter.string(from: now))
    }

    func testSetDoubleForAttributeOfEntity() {
        let mentat = Mentat()
        let report = self.populateWithTypesSchema(mentat: mentat)!
        let aEntid = report.entidForTmpId(tmpId: "a")!
        let attr = ":foo/double"
        let pre = mentat.value(forAttribute: attr, ofEntity: aEntid)
        XCTAssertNotNil(pre)
        assert(pre?.asDouble() == 11.23)
        XCTAssertNoThrow(try mentat.set(double: 22.0, forAttribute: attr, onEntity: aEntid))
        let post = mentat.value(forAttribute: attr, ofEntity: aEntid)
        XCTAssertNotNil(post)
        assert(post?.asDouble() == 22.0)
    }

    func testSetStringForAttributeOfEntity() {
        let mentat = Mentat()
        let report = self.populateWithTypesSchema(mentat: mentat)!
        let aEntid = report.entidForTmpId(tmpId: "a")!
        let attr = ":foo/string"
        let pre = mentat.value(forAttribute: attr, ofEntity: aEntid)
        XCTAssertNotNil(pre)
        assert(pre?.asString() == "The higher we soar the smaller we appear to those who cannot fly.")
        XCTAssertNoThrow(try mentat.set(string: "Become who you are!", forAttribute: attr, onEntity: aEntid))
        let post = mentat.value(forAttribute: attr, ofEntity: aEntid)
        XCTAssertNotNil(post)
        assert(post?.asString() == "Become who you are!")
    }

    func testSetUuidForAttributeOfEntity() {
        let mentat = Mentat()
        let report = self.populateWithTypesSchema(mentat: mentat)!
        let aEntid = report.entidForTmpId(tmpId: "a")!
        let previousUuid = UUID(uuidString: "550e8400-e29b-41d4-a716-446655440000")!
        let attr = ":foo/uuid"
        let pre = mentat.value(forAttribute: attr, ofEntity: aEntid)
        XCTAssertNotNil(pre)
        assert(pre?.asUUID() == previousUuid)

        let newUuid = UUID()
        XCTAssertNoThrow(try mentat.set(uuid: newUuid, forAttribute: attr, onEntity: aEntid))
        let post = mentat.value(forAttribute: attr, ofEntity: aEntid)
        XCTAssertNotNil(post)
        assert(post?.asUUID() == newUuid)
    }
}
