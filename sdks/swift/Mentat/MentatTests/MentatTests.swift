/* Copyright 2018 Mozilla
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License. */

import XCTest

@testable import Mentat

class MentatTests: XCTestCase {

    var citiesSchema: String?
    var seattleData: String?
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
        XCTAssertNotNil(try Mentat.open().raw)
    }

    // test that a store can be opened in a specific location
    func testOpenStoreInLocation() {
        let paths = FileManager.default.urls(for: .documentDirectory, in: .userDomainMask)
        let documentsURL = paths[0]
        let storeURI = documentsURL.appendingPathComponent("test.db", isDirectory: false).absoluteString
    
        XCTAssertNotNil(try Mentat.open(storeURI: storeURI).raw)
    }

    func readFile(forResource resource: String, withExtension ext: String, subdirectory: String ) throws -> String {
        let bundle = Bundle(for: type(of: self))
        let schemaUrl = bundle.url(forResource: resource, withExtension: ext, subdirectory: subdirectory)!
        let contents = try String(contentsOf: schemaUrl)
        return contents
    }

    func readCitiesSchema() throws -> String {
        guard let schema = self.citiesSchema else {
            self.citiesSchema = try self.readFile(forResource: "cities", withExtension: "schema", subdirectory: "fixtures")
            return self.citiesSchema!
        }

        return schema
    }

    func readSeattleData() throws -> String {
        guard let data = self.seattleData else {
            self.seattleData = try self.readFile(forResource: "all_seattle", withExtension: "edn", subdirectory: "fixtures")
            return self.seattleData!
        }

        return data
    }

    func transactCitiesSchema(mentat: Mentat) throws -> TxReport {
        let vocab = try readCitiesSchema()
        let report = try mentat.transact(transaction: vocab)
        return report
    }

    func transactSeattleData(mentat: Mentat) throws -> TxReport {
        let data = try readSeattleData()
        let report = try mentat.transact(transaction: data)
        return report
    }

    func openAndInitializeCitiesStore() -> Mentat {
        guard let mentat = self.store else {
            let mentat = try! Mentat.open()
            let _ = try! self.transactCitiesSchema(mentat: mentat)
            let _ = try! self.transactSeattleData(mentat: mentat)
            self.store = mentat
            return mentat
        }

        return mentat
    }

    func populateWithTypesSchema(mentat: Mentat) -> (TxReport?, TxReport?) {
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
            let transaction = try mentat.beginTransaction();
            let report = try transaction.transact(transaction: schema)
            let stringEntid = report.entid(forTempId: "s")!

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
                [:db/add "b" :foo/keyword :foo/string]
                [:db/add "b" :foo/long 50]
                [:db/add "b" :foo/instant #inst "2018-01-01T11:00:00.000Z"]
                [:db/add "b" :foo/double 22.46]
                [:db/add "b" :foo/string "Silence is worse; all truths that are kept silent become poisonous."]
                [:db/add "b" :foo/uuid #uuid "4cb3f828-752d-497a-90c9-b1fd516d5644"]
            ]
            """
            let dataReport = try transaction.transact(transaction: data)
            try transaction.commit();
            return (report, dataReport)
        } catch {
            assertionFailure(error.localizedDescription)
        }
        return (nil, nil)
    }

    func test1TransactVocabulary() {
        do {
            let mentat = try Mentat.open()
            let vocab = try readCitiesSchema()
            let report = try mentat.transact(transaction: vocab)
            XCTAssertNotNil(report)
            assert(report.txId > 0)
        } catch {
            assertionFailure(error.localizedDescription)
        }
    }

    func test2TransactEntities() {
        do {
            let mentat = try Mentat.open()
            let vocab = try readCitiesSchema()
            let _ = try mentat.transact(transaction: vocab)
            let data = try readSeattleData()
            let report = try mentat.transact(transaction: data)
            XCTAssertNotNil(report)
            assert(report.txId > 0)
            let entid = report.entid(forTempId: "a17592186045438")
            assert(entid == 65566)
        } catch {
            assertionFailure(error.localizedDescription)
        }
    }

    func testQueryScalar() {
        let mentat = openAndInitializeCitiesStore()
        let query = "[:find ?n . :in ?name :where [(fulltext $ :community/name ?name) [[?e ?n]]]]"
        let expect = expectation(description: "Query is executed")
        XCTAssertNoThrow(try mentat.query(query: query).bind(varName: "?name", toString: "Wallingford").runScalar(callback: { scalarResult in
            guard let result = scalarResult?.asString() else {
                return assertionFailure("No String value received")
            }
            assert(result == "KOMO Communities - Wallingford")
            expect.fulfill()
        }))
        waitForExpectations(timeout: 1) { error in
            if let error = error {
                assertionFailure("waitForExpectationsWithTimeout errored: \(error)")
            }
        }
    }

    func testQueryColl() {
        let mentat = openAndInitializeCitiesStore()
        let query = "[:find [?when ...] :where [_ :db/txInstant ?when] :order (asc ?when)]"
        let expect = expectation(description: "Query is executed")
        XCTAssertNoThrow(try mentat.query(query: query).runColl(callback: { collResult in
            guard let rows = collResult else {
                return assertionFailure("No results received")
            }
            // we are expecting 3 results
            for i in 0..<3 {
                let _ = rows.asDate(index: i)
                assert(true)
            }
            expect.fulfill()
        }))
        waitForExpectations(timeout: 1) { error in
            if let error = error {
                assertionFailure("waitForExpectationsWithTimeout errored: \(error)")
            }
        }
    }

    func testQueryCollResultIterator() {
        let mentat = openAndInitializeCitiesStore()
        let query = "[:find [?when ...] :where [_ :db/txInstant ?when] :order (asc ?when)]"
        let expect = expectation(description: "Query is executed")
        XCTAssertNoThrow(try mentat.query(query: query).runColl(callback: { collResult in
            guard let rows = collResult else {
                return assertionFailure("No results received")
            }

            rows.forEach({ (value) in
                assert(value.valueType.rawValue == 2)
            })
            expect.fulfill()
        }))
        waitForExpectations(timeout: 1) { error in
            if let error = error {
                assertionFailure("waitForExpectationsWithTimeout errored: \(error)")
            }
        }
    }

    func testQueryTuple() {
        let mentat = openAndInitializeCitiesStore()
        let query = """
        [:find [?name ?cat]
        :where
        [?c :community/name ?name]
        [?c :community/type :community.type/website]
        [(fulltext $ :community/category "food") [[?c ?cat]]]]
        """
        let expect = expectation(description: "Query is executed")
        XCTAssertNoThrow(try mentat.query(query: query).runTuple(callback: { tupleResult in
            guard let tuple = tupleResult else {
                return assertionFailure("expecting a result")
            }
            let name = tuple.asString(index: 0)
            let category = tuple.asString(index: 1)
            assert(name == "Community Harvest of Southwest Seattle")
            assert(category == "sustainable food")
            expect.fulfill()
        }))
        waitForExpectations(timeout: 1) { error in
            if let error = error {
                assertionFailure("waitForExpectationsWithTimeout errored: \(error)")
            }
        }
    }

    func testQueryRel() {
        let mentat = openAndInitializeCitiesStore()
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
        XCTAssertNoThrow(try mentat.query(query: query).run(callback: { relResult in
            guard let rows = relResult else {
                return assertionFailure("No results received")
            }

            for (i, row) in rows.enumerated() {
                let (name, category) = expectedResults[i]
                assert( row.asString(index: 0) == name)
                assert(row.asString(index: 1) == category)
            }
            expect.fulfill()
        }))
        waitForExpectations(timeout: 1) { error in
            if let error = error {
                assertionFailure("waitForExpectationsWithTimeout errored: \(error)")
            }
        }
    }

    func testQueryRelResultIterator() {
        let mentat = openAndInitializeCitiesStore()
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
        XCTAssertNoThrow(try mentat.query(query: query).run(callback: { relResult in
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
        }))
        waitForExpectations(timeout: 1) { error in
            if let error = error {
                assertionFailure("waitForExpectationsWithTimeout errored: \(error)")
            }
        }
    }

    func testBindLong() {
        let mentat = try! Mentat.open()
        let (_, report) = self.populateWithTypesSchema(mentat: mentat)
        let aEntid = report!.entid(forTempId: "a")
        let query = "[:find ?e . :in ?long :where [?e :foo/long ?long]]"
        let expect = expectation(description: "Query is executed")
        XCTAssertNoThrow(try mentat.query(query: query)
              .bind(varName: "?long", toLong: 25)
              .runScalar { value in
                XCTAssertNotNil(value)
                assert(value?.asEntid() == aEntid)
                expect.fulfill()
        })
        waitForExpectations(timeout: 1) { error in
            if let error = error {
                assertionFailure("waitForExpectationsWithTimeout errored: \(error)")
            }
        }
    }

    func testBindRef() {
        let mentat = try! Mentat.open()
        let (_, report) = self.populateWithTypesSchema(mentat: mentat)
        let stringEntid = mentat.entidForAttribute(attribute: ":foo/string")
        let bEntid = report!.entid(forTempId: "b")
        let query = "[:find ?e . :in ?ref :where [?e :foo/ref ?ref]]"
        let expect = expectation(description: "Query is executed")
        XCTAssertNoThrow(try mentat.query(query: query)
            .bind(varName: "?ref", toReference: stringEntid)
            .runScalar { value in
                XCTAssertNotNil(value)
                assert(value?.asEntid() == bEntid)
                expect.fulfill()
        })
        waitForExpectations(timeout: 1) { error in
            if let error = error {
                assertionFailure("waitForExpectationsWithTimeout errored: \(error)")
            }
        }
    }

    func testBindKwRef() {
        let mentat = try! Mentat.open()
        let (_, report) = self.populateWithTypesSchema(mentat: mentat)
        let bEntid = report!.entid(forTempId: "b")
        let query = "[:find ?e . :in ?ref :where [?e :foo/ref ?ref]]"
        let expect = expectation(description: "Query is executed")
        XCTAssertNoThrow(try mentat.query(query: query)
            .bind(varName: "?ref", toReference: ":foo/string")
            .runScalar { value in
                XCTAssertNotNil(value)
                assert(value?.asEntid() == bEntid)
                expect.fulfill()
        })
        waitForExpectations(timeout: 1) { error in
            if let error = error {
                assertionFailure("waitForExpectationsWithTimeout errored: \(error)")
            }
        }
    }

    func testBindKw() {
        let mentat = try! Mentat.open()
        let (_, report) = self.populateWithTypesSchema(mentat: mentat)
        let aEntid = report!.entid(forTempId: "a")
        let query = "[:find ?e . :in ?kw :where [?e :foo/keyword ?kw]]"
        let expect = expectation(description: "Query is executed")
        XCTAssertNoThrow(try mentat.query(query: query)
            .bind(varName: "?kw", toKeyword: ":foo/string")
            .runScalar { value in
                XCTAssertNotNil(value)
                assert(value?.asEntid() == aEntid)
                expect.fulfill()
        })
        waitForExpectations(timeout: 1) { error in
            if let error = error {
                assertionFailure("waitForExpectationsWithTimeout errored: \(error)")
            }
        }
    }

    func testBindDate() {
        let mentat = try! Mentat.open()
        let (_, report) = self.populateWithTypesSchema(mentat: mentat)
        let aEntid = report!.entid(forTempId: "a")
        let query = "[:find [?e ?d] :in ?now :where [?e :foo/instant ?d] [(< ?d ?now)]]"
        let expect = expectation(description: "Query is executed")

        let formatter = DateFormatter()
        formatter.dateFormat = "yyyy-MM-dd'T'HH:mm:ssZZZZZ"
        let boundDate = formatter.date(from: "2018-04-16T16:39:18+00:00")!

        XCTAssertNoThrow(try mentat.query(query: query)
            .bind(varName: "?now", toDate: boundDate)
            .runTuple { row in
                XCTAssertNotNil(row)
                assert(row?.asEntid(index: 0) == aEntid)
                expect.fulfill()
        })
        waitForExpectations(timeout: 1) { error in
            if let error = error {
                assertionFailure("waitForExpectationsWithTimeout errored: \(error)")
            }
        }
    }


    func testBindString() {
        let mentat = openAndInitializeCitiesStore()
        let query = "[:find ?n . :in ?name :where [(fulltext $ :community/name ?name) [[?e ?n]]]]"
        let expect = expectation(description: "Query is executed")
        XCTAssertNoThrow(try mentat.query(query: query)
                   .bind(varName: "?name", toString: "Wallingford")
                   .runScalar(callback: { scalarResult in
            guard let result = scalarResult?.asString() else {
                return assertionFailure("No String value received")
            }
            assert(result == "KOMO Communities - Wallingford")
            expect.fulfill()
        }))
        waitForExpectations(timeout: 1) { error in
            if let error = error {
                assertionFailure("waitForExpectationsWithTimeout errored: \(error)")
            }
        }
    }

    func testBindUuid() {
        let mentat = try! Mentat.open()
        let (_, report) = self.populateWithTypesSchema(mentat: mentat)
        let aEntid = report!.entid(forTempId: "a")
        let query = "[:find ?e . :in ?uuid :where [?e :foo/uuid ?uuid]]"
        let uuid = UUID(uuidString: "550e8400-e29b-41d4-a716-446655440000")!
        let expect = expectation(description: "Query is rund")
        XCTAssertNoThrow(try mentat.query(query: query)
            .bind(varName: "?uuid", toUuid: uuid)
            .runScalar { value in
                XCTAssertNotNil(value)
                assert(value?.asEntid() == aEntid)
                expect.fulfill()
        })
        waitForExpectations(timeout: 1) { error in
            if let error = error {
                assertionFailure("waitForExpectationsWithTimeout errored: \(error)")
            }
        }
    }

    func testBindBoolean() {
        let mentat = try! Mentat.open()
        let (_, report) = self.populateWithTypesSchema(mentat: mentat)
        let aEntid = report!.entid(forTempId: "a")
        let query = "[:find ?e . :in ?bool :where [?e :foo/boolean ?bool]]"
        let expect = expectation(description: "Query is executed")
        XCTAssertNoThrow(try mentat.query(query: query)
            .bind(varName: "?bool", toBoolean: true)
            .runScalar { value in
                XCTAssertNotNil(value)
                assert(value?.asEntid() == aEntid)
                expect.fulfill()
        })
        waitForExpectations(timeout: 1) { error in
            if let error = error {
                assertionFailure("waitForExpectationsWithTimeout errored: \(error)")
            }
        }
    }

    func testBindDouble() {
        let mentat = try! Mentat.open()
        let (_, report) = self.populateWithTypesSchema(mentat: mentat)
        let aEntid = report!.entid(forTempId: "a")
        let query = "[:find ?e . :in ?double :where [?e :foo/double ?double]]"
        let expect = expectation(description: "Query is executed")
        XCTAssertNoThrow(try mentat.query(query: query)
            .bind(varName: "?double", toDouble: 11.23)
            .runScalar { value in
                XCTAssertNotNil(value)
                assert(value?.asEntid() == aEntid)
                expect.fulfill()
        })
        waitForExpectations(timeout: 1) { error in
            if let error = error {
                assertionFailure("waitForExpectationsWithTimeout errored: \(error)")
            }
        }
    }

    func testTypedValueAsLong() {
        let mentat = try! Mentat.open()
        let (_, report) = self.populateWithTypesSchema(mentat: mentat)
        let aEntid = report!.entid(forTempId: "a")!
        let query = "[:find ?v . :in ?e :where [?e :foo/long ?v]]"
        let expect = expectation(description: "Query is executed")
        XCTAssertNoThrow(try mentat.query(query: query)
            .bind(varName: "?e", toReference: aEntid)
            .runScalar { value in
                XCTAssertNotNil(value)
                assert(value?.asLong() == 25)
                assert(value?.asLong() == 25)
                expect.fulfill()
        })
        waitForExpectations(timeout: 1) { error in
            if let error = error {
                assertionFailure("waitForExpectationsWithTimeout errored: \(error)")
            }
        }
    }

    func testTypedValueAsRef() {
        let mentat = try! Mentat.open()
        let (_, report) = self.populateWithTypesSchema(mentat: mentat)
        let aEntid = report!.entid(forTempId: "a")!
        let query = "[:find ?e . :where [?e :foo/long 25]]"
        let expect = expectation(description: "Query is executed")
        XCTAssertNoThrow(try mentat.query(query: query)
            .runScalar { value in
                XCTAssertNotNil(value)
                assert(value?.asEntid() == aEntid)
                assert(value?.asEntid() == aEntid)
                expect.fulfill()
        })
        waitForExpectations(timeout: 1) { error in
            if let error = error {
                assertionFailure("waitForExpectationsWithTimeout errored: \(error)")
            }
        }
    }

    func testTypedValueAsKw() {
        let mentat = try! Mentat.open()
        let (_, report) = self.populateWithTypesSchema(mentat: mentat)
        let aEntid = report!.entid(forTempId: "a")!
        let query = "[:find ?v . :in ?e :where [?e :foo/keyword ?v]]"
        let expect = expectation(description: "Query is executed")
        XCTAssertNoThrow(try mentat.query(query: query)
            .bind(varName: "?e", toReference: aEntid)
            .runScalar { value in
                XCTAssertNotNil(value)
                assert(value?.asKeyword() == ":foo/string")
                assert(value?.asKeyword() == ":foo/string")
                expect.fulfill()
        })
        waitForExpectations(timeout: 1) { error in
            if let error = error {
                assertionFailure("waitForExpectationsWithTimeout errored: \(error)")
            }
        }
    }

    func testTypedValueAsBoolean() {
        let mentat = try! Mentat.open()
        let (_, report) = self.populateWithTypesSchema(mentat: mentat)
        let aEntid = report!.entid(forTempId: "a")!
        let query = "[:find ?v . :in ?e :where [?e :foo/boolean ?v]]"
        let expect = expectation(description: "Query is executed")
        XCTAssertNoThrow(try mentat.query(query: query)
            .bind(varName: "?e", toReference: aEntid)
            .runScalar { value in
                XCTAssertNotNil(value)
                assert(value?.asBool() == true)
                assert(value?.asBool() == true)
                expect.fulfill()
        })
        waitForExpectations(timeout: 1) { error in
            if let error = error {
                assertionFailure("waitForExpectationsWithTimeout errored: \(error)")
            }
        }
    }

    func testTypedValueAsDouble() {
        let mentat = try! Mentat.open()
        let (_, report) = self.populateWithTypesSchema(mentat: mentat)
        let aEntid = report!.entid(forTempId: "a")!
        let query = "[:find ?v . :in ?e :where [?e :foo/double ?v]]"
        let expect = expectation(description: "Query is executed")
        XCTAssertNoThrow(try mentat.query(query: query)
            .bind(varName: "?e", toReference: aEntid)
            .runScalar { value in
                XCTAssertNotNil(value)
                assert(value?.asDouble() == 11.23)
                assert(value?.asDouble() == 11.23)
                expect.fulfill()
        })
        waitForExpectations(timeout: 1) { error in
            if let error = error {
                assertionFailure("waitForExpectationsWithTimeout errored: \(error)")
            }
        }
    }

    func testTypedValueAsDate() {
        let mentat = try! Mentat.open()
        let (_, report) = self.populateWithTypesSchema(mentat: mentat)
        let aEntid = report!.entid(forTempId: "a")!
        let query = "[:find ?v . :in ?e :where [?e :foo/instant ?v]]"
        let expect = expectation(description: "Query is executed")

        let formatter = DateFormatter()
        formatter.dateFormat = "yyyy-MM-dd'T'HH:mm:ssZZZZZ"
        let expectedDate = formatter.date(from: "2017-01-01T11:00:00+00:00")

        XCTAssertNoThrow(try mentat.query(query: query)
            .bind(varName: "?e", toReference: aEntid)
            .runScalar { value in
                XCTAssertNotNil(value)
                assert(value?.asDate() == expectedDate)
                assert(value?.asDate() == expectedDate)
                expect.fulfill()
        })
        waitForExpectations(timeout: 1) { error in
            if let error = error {
                assertionFailure("waitForExpectationsWithTimeout errored: \(error)")
            }
        }
    }

    func testTypedValueAsString() {
        let mentat = try! Mentat.open()
        let (_, report) = self.populateWithTypesSchema(mentat: mentat)
        let aEntid = report!.entid(forTempId: "a")!
        let query = "[:find ?v . :in ?e :where [?e :foo/string ?v]]"
        let expect = expectation(description: "Query is executed")
        XCTAssertNoThrow(try mentat.query(query: query)
            .bind(varName: "?e", toReference: aEntid)
            .runScalar { value in
                XCTAssertNotNil(value)
                assert(value?.asString() == "The higher we soar the smaller we appear to those who cannot fly.")
                assert(value?.asString() == "The higher we soar the smaller we appear to those who cannot fly.")
                expect.fulfill()
        })
        waitForExpectations(timeout: 1) { error in
            if let error = error {
                assertionFailure("waitForExpectationsWithTimeout errored: \(error)")
            }
        }
    }

    func testTypedValueAsUuid() {
        let mentat = try! Mentat.open()
        let (_, report) = self.populateWithTypesSchema(mentat: mentat)
        let aEntid = report!.entid(forTempId: "a")!
        let query = "[:find ?v . :in ?e :where [?e :foo/uuid ?v]]"
        let expectedUuid = UUID(uuidString: "550e8400-e29b-41d4-a716-446655440000")!
        let expect = expectation(description: "Query is executed")
        XCTAssertNoThrow(try mentat.query(query: query)
            .bind(varName: "?e", toReference: aEntid)
            .runScalar { value in
                XCTAssertNotNil(value)
                assert(value?.asUUID() == expectedUuid)
                assert(value?.asUUID() == expectedUuid)
                expect.fulfill()
        })
        waitForExpectations(timeout: 1) { error in
            if let error = error {
                assertionFailure("waitForExpectationsWithTimeout errored: \(error)")
            }
        }
    }

    func testValueForAttributeOfEntity() {
        let mentat = try! Mentat.open()
        let (_, report) = self.populateWithTypesSchema(mentat: mentat)
        let aEntid = report!.entid(forTempId: "a")!
        var value: TypedValue? = nil;
        XCTAssertNoThrow(value = try mentat.value(forAttribute: ":foo/long", ofEntity: aEntid))
        XCTAssertNotNil(value)
        assert(value?.asLong() == 25)
    }

    func testEntidForAttribute() {
        let mentat = try! Mentat.open()
        let _ = self.populateWithTypesSchema(mentat: mentat)
        let entid = mentat.entidForAttribute(attribute: ":foo/long")
        assert(entid == 65540)
    }

    func testMultipleQueries() {
        let mentat = try! Mentat.open()
        let _ = self.populateWithTypesSchema(mentat: mentat)
        let q1 = mentat.query(query: "[:find ?x :where [?x _ _]]")

        let q1Expect = expectation(description: "Query 1 is executed")
        XCTAssertNoThrow(try q1.run { results in
            XCTAssertNotNil(results)
            q1Expect.fulfill()
        })

        let q2 = mentat.query(query: "[:find ?x :where [_ _ ?x]]")
        let q2Expect = expectation(description: "Query 2 is executed")
        XCTAssertNoThrow(try q2.run { results in
            XCTAssertNotNil(results)
            q2Expect.fulfill()
        })

        waitForExpectations(timeout: 1) { error in
            if let error = error {
                assertionFailure("waitForExpectationsWithTimeout errored: \(error)")
            }
        }
    }

    func testNestedQueries() {
        let mentat = try! Mentat.open()
        let _ = self.populateWithTypesSchema(mentat: mentat)
        let q1 = mentat.query(query: "[:find ?x :where [?x _ _]]")
        let q2 = mentat.query(query: "[:find ?x :where [_ _ ?x]]")

        let expect = expectation(description: "Query 1 is executed")
        XCTAssertNoThrow(try q1.run { results in
            XCTAssertNotNil(results)
            try? q2.run { results in
                XCTAssertNotNil(results)
                expect.fulfill()
            }
        })

        waitForExpectations(timeout: 1) { error in
            if let error = error {
                assertionFailure("waitForExpectationsWithTimeout errored: \(error)")
            }
        }
    }

    func test3InProgressTransact() {
        let mentat = try! Mentat.open()
        let (_, report) = self.populateWithTypesSchema(mentat: mentat)
        XCTAssertNotNil(report)
    }

    func testInProgressRollback() {
        let mentat = try! Mentat.open()
        let (_, report) = self.populateWithTypesSchema(mentat: mentat)
        XCTAssertNotNil(report)
        let aEntid = report!.entid(forTempId: "a")!

        let preLongValue = try! mentat.value(forAttribute: ":foo/long", ofEntity: aEntid)
        XCTAssertEqual(25, preLongValue?.asLong())

        let inProgress = try! mentat.beginTransaction()
        XCTAssertNoThrow(try inProgress.transact(transaction: "[[:db/add \(aEntid) :foo/long 22]]"))
        XCTAssertNoThrow(try inProgress.rollback())

        let postLongValue = try! mentat.value(forAttribute: ":foo/long", ofEntity: aEntid)
        XCTAssertEqual(25, postLongValue?.asLong())

    }

    func testInProgressEntityBuilder() {
        let mentat = try! Mentat.open()
        let (schemaReport, dataReport) = self.populateWithTypesSchema(mentat: mentat)
        let bEntid = dataReport!.entid(forTempId: "b")!
        let longEntid = schemaReport!.entid(forTempId: "l")!
        let stringEntid = schemaReport!.entid(forTempId: "s")!
        // test that the values are as expected
        let query = """
                    [:find [?b ?i ?u ?l ?d ?s ?k ?r]
                     :in ?e
                     :where [?e :foo/boolean ?b]
                            [?e :foo/instant ?i]
                            [?e :foo/uuid ?u]
                            [?e :foo/long ?l]
                            [?e :foo/double ?d]
                            [?e :foo/string ?s]
                            [?e :foo/keyword ?k]
                            [?e :foo/ref ?r]]
                    """
        let formatter = DateFormatter()
        formatter.dateFormat = "yyyy-MM-dd'T'HH:mm:ssZZZZZ"
        XCTAssertNoThrow(try mentat.query(query: query).bind(varName: "?e", toReference: bEntid).runTuple { (result) in
            XCTAssertNotNil(result)
            XCTAssertEqual(false, result?.asBool(index: 0))

            let previousDate = formatter.date(from: "2018-01-01T11:00:00+00:00")
            XCTAssertEqual(previousDate, result?.asDate(index: 1))

            let previousUuid = UUID(uuidString: "4cb3f828-752d-497a-90c9-b1fd516d5644")!
            XCTAssertEqual(previousUuid, result?.asUUID(index: 2))

            XCTAssertEqual(50, result?.asLong(index: 3))
            XCTAssertEqual(22.46, result?.asDouble(index: 4))
            XCTAssertEqual("Silence is worse; all truths that are kept silent become poisonous.", result?.asString(index: 5))
            XCTAssertEqual(":foo/string", result?.asKeyword(index: 6))
            XCTAssertEqual(stringEntid, result?.asEntid(index: 7))
        })

        let builder = try! mentat.entityBuilder()
        XCTAssertNoThrow(try builder.add(entid: bEntid, keyword: ":foo/boolean", boolean: true))
        let newDate = Date()
        XCTAssertNoThrow(try builder.add(entid: bEntid, keyword: ":foo/instant", date: newDate))
        let newUUID = UUID()
        XCTAssertNoThrow(try builder.add(entid: bEntid, keyword: ":foo/uuid", uuid: newUUID))
        XCTAssertNoThrow(try builder.add(entid: bEntid, keyword: ":foo/long", long: 75))
        XCTAssertNoThrow(try builder.add(entid: bEntid, keyword: ":foo/double", double: 81.3))
        XCTAssertNoThrow(try builder.add(entid: bEntid, keyword: ":foo/string", string: "Become who you are!"))
        XCTAssertNoThrow(try builder.add(entid: bEntid, keyword: ":foo/keyword", keyword: ":foo/long"))
        XCTAssertNoThrow(try builder.add(entid: bEntid, keyword: ":foo/ref", reference: longEntid))
        XCTAssertNoThrow(try builder.commit())

        // test that the values have changed
        XCTAssertNoThrow(try mentat.query(query: query).bind(varName: "?e", toReference: bEntid).runTuple { (result) in
            XCTAssertNotNil(result)
            XCTAssertEqual(true, result?.asBool(index: 0))
            XCTAssertEqual(formatter.string(from: newDate), formatter.string(from: result!.asDate(index: 1)))
            XCTAssertEqual(newUUID, result?.asUUID(index: 2))
            XCTAssertEqual(75, result?.asLong(index: 3))
            XCTAssertEqual(81.3, result?.asDouble(index: 4))
            XCTAssertEqual("Become who you are!", result?.asString(index: 5))
            XCTAssertEqual(":foo/long", result?.asKeyword(index: 6))
            XCTAssertEqual(longEntid, result?.asEntid(index: 7))
       })
    }

    func testEntityBuilderForEntid() {
        let mentat = try! Mentat.open()
        let (schemaReport, dataReport) = self.populateWithTypesSchema(mentat: mentat)
        let bEntid = dataReport!.entid(forTempId: "b")!
        let longEntid = schemaReport!.entid(forTempId: "l")!
        let stringEntid = schemaReport!.entid(forTempId: "s")!
        // test that the values are as expected
        let query = """
                    [:find [?b ?i ?u ?l ?d ?s ?k ?r]
                     :in ?e
                     :where [?e :foo/boolean ?b]
                            [?e :foo/instant ?i]
                            [?e :foo/uuid ?u]
                            [?e :foo/long ?l]
                            [?e :foo/double ?d]
                            [?e :foo/string ?s]
                            [?e :foo/keyword ?k]
                            [?e :foo/ref ?r]]
                    """
        let formatter = DateFormatter()
        formatter.dateFormat = "yyyy-MM-dd'T'HH:mm:ssZZZZZ"
        XCTAssertNoThrow(try mentat.query(query: query).bind(varName: "?e", toReference: bEntid).runTuple { (result) in
            XCTAssertNotNil(result)
            XCTAssertEqual(false, result?.asBool(index: 0))

            let previousDate = formatter.date(from: "2018-01-01T11:00:00+00:00")
            XCTAssertEqual(previousDate, result?.asDate(index: 1))

            let previousUuid = UUID(uuidString: "4cb3f828-752d-497a-90c9-b1fd516d5644")!
            XCTAssertEqual(previousUuid, result?.asUUID(index: 2))

            XCTAssertEqual(50, result?.asLong(index: 3))
            XCTAssertEqual(22.46, result?.asDouble(index: 4))
            XCTAssertEqual("Silence is worse; all truths that are kept silent become poisonous.", result?.asString(index: 5))
            XCTAssertEqual(":foo/string", result?.asKeyword(index: 6))
            XCTAssertEqual(stringEntid, result?.asEntid(index: 7))
        })

        let builder = try! mentat.entityBuilder(forEntid: bEntid)
        XCTAssertNoThrow(try builder.add(keyword: ":foo/boolean", boolean: true))
        let newDate = Date()
        XCTAssertNoThrow(try builder.add(keyword: ":foo/instant", date: newDate))
        let newUUID = UUID()
        XCTAssertNoThrow(try builder.add(keyword: ":foo/uuid", uuid: newUUID))
        XCTAssertNoThrow(try builder.add(keyword: ":foo/long", long: 75))
        XCTAssertNoThrow(try builder.add(keyword: ":foo/double", double: 81.3))
        XCTAssertNoThrow(try builder.add(keyword: ":foo/string", string: "Become who you are!"))
        XCTAssertNoThrow(try builder.add(keyword: ":foo/keyword", keyword: ":foo/long"))
        XCTAssertNoThrow(try builder.add(keyword: ":foo/ref", reference: longEntid))
        XCTAssertNoThrow(try builder.commit())

        // test that the values have changed
        XCTAssertNoThrow(try mentat.query(query: query).bind(varName: "?e", toReference: bEntid).runTuple { (result) in
            XCTAssertNotNil(result)
            XCTAssertEqual(true, result?.asBool(index: 0))
            XCTAssertEqual(formatter.string(from: newDate), formatter.string(from: result!.asDate(index: 1)))
            XCTAssertEqual(newUUID, result?.asUUID(index: 2))
            XCTAssertEqual(75, result?.asLong(index: 3))
            XCTAssertEqual(81.3, result?.asDouble(index: 4))
            XCTAssertEqual("Become who you are!", result?.asString(index: 5))
            XCTAssertEqual(":foo/long", result?.asKeyword(index: 6))
            XCTAssertEqual(longEntid, result?.asEntid(index: 7))
        })
    }

    func testEntityBuilderForTempid() {
        let mentat = try! Mentat.open()
        let (schemaReport, _) = self.populateWithTypesSchema(mentat: mentat)
        let longEntid = schemaReport!.entid(forTempId: "l")!
        // test that the values are as expected
        let query = """
                    [:find [?b ?i ?u ?l ?d ?s ?k ?r]
                     :in ?e
                     :where [?e :foo/boolean ?b]
                            [?e :foo/instant ?i]
                            [?e :foo/uuid ?u]
                            [?e :foo/long ?l]
                            [?e :foo/double ?d]
                            [?e :foo/string ?s]
                            [?e :foo/keyword ?k]
                            [?e :foo/ref ?r]]
                    """
        let formatter = DateFormatter()
        formatter.dateFormat = "yyyy-MM-dd'T'HH:mm:ssZZZZZ"

        let builder = try! mentat.entityBuilder(forTempId: "c")
        XCTAssertNoThrow(try builder.add(keyword: ":foo/boolean", boolean: true))
        let newDate = Date()
        XCTAssertNoThrow(try builder.add(keyword: ":foo/instant", date: newDate))
        let newUUID = UUID()
        XCTAssertNoThrow(try builder.add(keyword: ":foo/uuid", uuid: newUUID))
        XCTAssertNoThrow(try builder.add(keyword: ":foo/long", long: 75))
        XCTAssertNoThrow(try builder.add(keyword: ":foo/double", double: 81.3))
        XCTAssertNoThrow(try builder.add(keyword: ":foo/string", string: "Become who you are!"))
        XCTAssertNoThrow(try builder.add(keyword: ":foo/keyword", keyword: ":foo/long"))
        XCTAssertNoThrow(try builder.add(keyword: ":foo/ref", reference: longEntid))
        let report = try! builder.commit()
        let cEntid = report.entid(forTempId: "c")!
        // test that the values have changed
        XCTAssertNoThrow(try mentat.query(query: query).bind(varName: "?e", toReference: cEntid).runTuple { (result) in
            XCTAssertNotNil(result)
            XCTAssertEqual(true, result?.asBool(index: 0))
            XCTAssertEqual(formatter.string(from: newDate), formatter.string(from: result!.asDate(index: 1)))
            XCTAssertEqual(newUUID, result?.asUUID(index: 2))
            XCTAssertEqual(75, result?.asLong(index: 3))
            XCTAssertEqual(81.3, result?.asDouble(index: 4))
            XCTAssertEqual("Become who you are!", result?.asString(index: 5))
            XCTAssertEqual(":foo/long", result?.asKeyword(index: 6))
            XCTAssertEqual(longEntid, result?.asEntid(index: 7))
        })
    }

    func testInProgressBuilderTransact() {
        let mentat = try! Mentat.open()
        let (schemaReport, dataReport) = self.populateWithTypesSchema(mentat: mentat)
        let aEntid = dataReport!.entid(forTempId: "a")!
        let bEntid = dataReport!.entid(forTempId: "b")!
        let longEntid = schemaReport!.entid(forTempId: "l")!
        // test that the values are as expected
        let query = """
                    [:find [?b ?i ?u ?l ?d ?s ?k ?r]
                     :in ?e
                     :where [?e :foo/boolean ?b]
                            [?e :foo/instant ?i]
                            [?e :foo/uuid ?u]
                            [?e :foo/long ?l]
                            [?e :foo/double ?d]
                            [?e :foo/string ?s]
                            [?e :foo/keyword ?k]
                            [?e :foo/ref ?r]]
                    """
        let formatter = DateFormatter()
        formatter.dateFormat = "yyyy-MM-dd'T'HH:mm:ssZZZZZ"

        let builder = try! mentat.entityBuilder()
        XCTAssertNoThrow(try builder.add(entid: bEntid, keyword: ":foo/boolean", boolean: true))
        let newDate = Date()
        XCTAssertNoThrow(try builder.add(entid: bEntid, keyword: ":foo/instant", date: newDate))
        let newUUID = UUID()
        XCTAssertNoThrow(try builder.add(entid: bEntid, keyword: ":foo/uuid", uuid: newUUID))
        XCTAssertNoThrow(try builder.add(entid: bEntid, keyword: ":foo/long", long: 75))
        XCTAssertNoThrow(try builder.add(entid: bEntid, keyword: ":foo/double", double: 81.3))
        XCTAssertNoThrow(try builder.add(entid: bEntid, keyword: ":foo/string", string: "Become who you are!"))
        XCTAssertNoThrow(try builder.add(entid: bEntid, keyword: ":foo/keyword", keyword: ":foo/long"))
        XCTAssertNoThrow(try builder.add(entid: bEntid, keyword: ":foo/ref", reference: longEntid))
        let (inProgress, report) = try! builder.transact()
        XCTAssertNotNil(inProgress)
        XCTAssertNotNil(report)
        XCTAssertNoThrow(try inProgress.transact(transaction: "[[:db/add \(aEntid) :foo/long 22]]"))
        XCTAssertNoThrow(try inProgress.commit())

        // test that the values have changed
        XCTAssertNoThrow(try mentat.query(query: query).bind(varName: "?e", toReference: bEntid).runTuple { (result) in
            XCTAssertNotNil(result)
            XCTAssertEqual(true, result?.asBool(index: 0))
            XCTAssertEqual(formatter.string(from: newDate), formatter.string(from: result!.asDate(index: 1)))
            XCTAssertEqual(newUUID, result?.asUUID(index: 2))
            XCTAssertEqual(75, result?.asLong(index: 3))
            XCTAssertEqual(81.3, result?.asDouble(index: 4))
            XCTAssertEqual("Become who you are!", result?.asString(index: 5))
            XCTAssertEqual(":foo/long", result?.asKeyword(index: 6))
            XCTAssertEqual(longEntid, result?.asEntid(index: 7))
        })

        let longValue = try! mentat.value(forAttribute: ":foo/long", ofEntity: aEntid)
        XCTAssertEqual(22, longValue?.asLong())
    }

    func testEntityBuilderTransact() {
        let mentat = try! Mentat.open()
        let (schemaReport, dataReport) = self.populateWithTypesSchema(mentat: mentat)
        let aEntid = dataReport!.entid(forTempId: "a")!
        let bEntid = dataReport!.entid(forTempId: "b")!
        let longEntid = schemaReport!.entid(forTempId: "l")!
        // test that the values are as expected
        let query = """
                    [:find [?b ?i ?u ?l ?d ?s ?k ?r]
                     :in ?e
                     :where [?e :foo/boolean ?b]
                            [?e :foo/instant ?i]
                            [?e :foo/uuid ?u]
                            [?e :foo/long ?l]
                            [?e :foo/double ?d]
                            [?e :foo/string ?s]
                            [?e :foo/keyword ?k]
                            [?e :foo/ref ?r]]
                    """
        let formatter = DateFormatter()
        formatter.dateFormat = "yyyy-MM-dd'T'HH:mm:ssZZZZZ"

        let builder = try! mentat.entityBuilder(forEntid: bEntid)
        XCTAssertNoThrow(try builder.add(keyword: ":foo/boolean", boolean: true))
        let newDate = Date()
        XCTAssertNoThrow(try builder.add(keyword: ":foo/instant", date: newDate))
        let newUUID = UUID()
        XCTAssertNoThrow(try builder.add(keyword: ":foo/uuid", uuid: newUUID))
        XCTAssertNoThrow(try builder.add(keyword: ":foo/long", long: 75))
        XCTAssertNoThrow(try builder.add(keyword: ":foo/double", double: 81.3))
        XCTAssertNoThrow(try builder.add(keyword: ":foo/string", string: "Become who you are!"))
        XCTAssertNoThrow(try builder.add(keyword: ":foo/keyword", keyword: ":foo/long"))
        XCTAssertNoThrow(try builder.add(keyword: ":foo/ref", reference: longEntid))
        let (inProgress, report) = try! builder.transact()
        XCTAssertNotNil(inProgress)
        XCTAssertNotNil(report)
        XCTAssertNoThrow(try inProgress.transact(transaction: "[[:db/add \(aEntid) :foo/long 22]]"))
        XCTAssertNoThrow(try inProgress.commit())

        // test that the values have changed
        XCTAssertNoThrow(try mentat.query(query: query).bind(varName: "?e", toReference: bEntid).runTuple { (result) in
            XCTAssertNotNil(result)
            XCTAssertEqual(true, result?.asBool(index: 0))
            XCTAssertEqual(formatter.string(from: newDate), formatter.string(from: result!.asDate(index: 1)))
            XCTAssertEqual(newUUID, result?.asUUID(index: 2))
            XCTAssertEqual(75, result?.asLong(index: 3))
            XCTAssertEqual(81.3, result?.asDouble(index: 4))
            XCTAssertEqual("Become who you are!", result?.asString(index: 5))
            XCTAssertEqual(":foo/long", result?.asKeyword(index: 6))
            XCTAssertEqual(longEntid, result?.asEntid(index: 7))
        })

        let longValue = try! mentat.value(forAttribute: ":foo/long", ofEntity: aEntid)
        XCTAssertEqual(22, longValue?.asLong())
    }

    func testEntityBuilderRetract() {
        let mentat = try! Mentat.open()
        let (schemaReport, dataReport) = self.populateWithTypesSchema(mentat: mentat)
        let bEntid = dataReport!.entid(forTempId: "b")!
        let stringEntid = schemaReport!.entid(forTempId: "s")!
        // test that the values are as expected
        let query = """
                    [:find [?b ?i ?u ?l ?d ?s ?k ?r]
                     :in ?e
                     :where [?e :foo/boolean ?b]
                            [?e :foo/instant ?i]
                            [?e :foo/uuid ?u]
                            [?e :foo/long ?l]
                            [?e :foo/double ?d]
                            [?e :foo/string ?s]
                            [?e :foo/keyword ?k]
                            [?e :foo/ref ?r]]
                    """
        let formatter = DateFormatter()
        formatter.dateFormat = "yyyy-MM-dd'T'HH:mm:ssZZZZZ"
        let previousDate = formatter.date(from: "2018-01-01T11:00:00+00:00")!
        let previousUuid = UUID(uuidString: "4cb3f828-752d-497a-90c9-b1fd516d5644")!
        XCTAssertNoThrow(try mentat.query(query: query).bind(varName: "?e", toReference: bEntid).runTuple { (result) in
            XCTAssertNotNil(result)
            XCTAssertEqual(false, result?.asBool(index: 0))
            XCTAssertEqual(previousDate, result?.asDate(index: 1))
            XCTAssertEqual(previousUuid, result?.asUUID(index: 2))
            XCTAssertEqual(50, result?.asLong(index: 3))
            XCTAssertEqual(22.46, result?.asDouble(index: 4))
            XCTAssertEqual("Silence is worse; all truths that are kept silent become poisonous.", result?.asString(index: 5))
            XCTAssertEqual(":foo/string", result?.asKeyword(index: 6))
            XCTAssertEqual(stringEntid, result?.asEntid(index: 7))
        })

        let builder = try! mentat.entityBuilder(forEntid: bEntid)
        XCTAssertNoThrow(try builder.retract(keyword: ":foo/boolean", boolean: false))
        XCTAssertNoThrow(try builder.retract(keyword: ":foo/instant", date: previousDate))
        XCTAssertNoThrow(try builder.retract(keyword: ":foo/uuid", uuid: previousUuid))
        XCTAssertNoThrow(try builder.retract(keyword: ":foo/long", long: 50))
        XCTAssertNoThrow(try builder.retract(keyword: ":foo/double", double: 22.46))
        XCTAssertNoThrow(try builder.retract(keyword: ":foo/string", string: "Silence is worse; all truths that are kept silent become poisonous."))
        XCTAssertNoThrow(try builder.retract(keyword: ":foo/keyword", keyword: ":foo/string"))
        XCTAssertNoThrow(try builder.retract(keyword: ":foo/ref", reference: stringEntid))
        XCTAssertNoThrow(try builder.commit())

        XCTAssertNoThrow(try mentat.query(query: query).bind(varName: "?e", toReference: bEntid).runTuple { (result) in
            XCTAssertNil(result)
        })
    }

    func testInProgressEntityBuilderRetract() {
        let mentat = try! Mentat.open()
        let (schemaReport, dataReport) = self.populateWithTypesSchema(mentat: mentat)
        let bEntid = dataReport!.entid(forTempId: "b")!
        let stringEntid = schemaReport!.entid(forTempId: "s")!
        // test that the values are as expected
        let query = """
                    [:find [?b ?i ?u ?l ?d ?s ?k ?r]
                     :in ?e
                     :where [?e :foo/boolean ?b]
                            [?e :foo/instant ?i]
                            [?e :foo/uuid ?u]
                            [?e :foo/long ?l]
                            [?e :foo/double ?d]
                            [?e :foo/string ?s]
                            [?e :foo/keyword ?k]
                            [?e :foo/ref ?r]]
                    """
        let formatter = DateFormatter()
        formatter.dateFormat = "yyyy-MM-dd'T'HH:mm:ssZZZZZ"
        let previousDate = formatter.date(from: "2018-01-01T11:00:00+00:00")!
        let previousUuid = UUID(uuidString: "4cb3f828-752d-497a-90c9-b1fd516d5644")!
        XCTAssertNoThrow(try mentat.query(query: query).bind(varName: "?e", toReference: bEntid).runTuple { (result) in
            XCTAssertNotNil(result)
            XCTAssertEqual(false, result?.asBool(index: 0))
            XCTAssertEqual(previousDate, result?.asDate(index: 1))
            XCTAssertEqual(previousUuid, result?.asUUID(index: 2))
            XCTAssertEqual(50, result?.asLong(index: 3))
            XCTAssertEqual(22.46, result?.asDouble(index: 4))
            XCTAssertEqual("Silence is worse; all truths that are kept silent become poisonous.", result?.asString(index: 5))
            XCTAssertEqual(":foo/string", result?.asKeyword(index: 6))
            XCTAssertEqual(stringEntid, result?.asEntid(index: 7))
        })

        let builder = try! mentat.entityBuilder()
        XCTAssertNoThrow(try builder.retract(entid: bEntid, keyword: ":foo/boolean", boolean: false))
        XCTAssertNoThrow(try builder.retract(entid: bEntid, keyword: ":foo/instant", date: previousDate))
        XCTAssertNoThrow(try builder.retract(entid: bEntid, keyword: ":foo/uuid", uuid: previousUuid))
        XCTAssertNoThrow(try builder.retract(entid: bEntid, keyword: ":foo/long", long: 50))
        XCTAssertNoThrow(try builder.retract(entid: bEntid, keyword: ":foo/double", double: 22.46))
        XCTAssertNoThrow(try builder.retract(entid: bEntid, keyword: ":foo/string", string: "Silence is worse; all truths that are kept silent become poisonous."))
        XCTAssertNoThrow(try builder.retract(entid: bEntid, keyword: ":foo/keyword", keyword: ":foo/string"))
        XCTAssertNoThrow(try builder.retract(entid: bEntid, keyword: ":foo/ref", reference: stringEntid))
        XCTAssertNoThrow(try builder.commit())

        XCTAssertNoThrow(try mentat.query(query: query).bind(varName: "?e", toReference: bEntid).runTuple { (result) in
            XCTAssertNil(result)
        })
    }

    func testCaching() {
        let query = """
                [:find ?district :where
                [?neighborhood :neighborhood/name \"Beacon Hill\"]
                [?neighborhood :neighborhood/district ?d]
                [?d :district/name ?district]]
                """

        let mentat = openAndInitializeCitiesStore()

        struct QueryTimer {
            private var _start: UInt64
            private var _end: UInt64

            init() {
                self._start = 0
                self._end = 0
            }

            private func currentTimeNanos() -> UInt64 {
                var info = mach_timebase_info()
                guard mach_timebase_info(&info) == KERN_SUCCESS else { return 0 }
                let currentTime = mach_absolute_time()
                return currentTime * UInt64(info.numer) / UInt64(info.denom)
            }


            mutating func start() {
                self._start = self.currentTimeNanos()
            }

            mutating func end() {
                self._end = self.currentTimeNanos()
            }

            func duration() -> UInt64 {
                return self._end - self._start
            }
        }

        var uncachedTimer = QueryTimer()
        uncachedTimer.start()

        XCTAssertNoThrow(try mentat.query(query: query).run { (result) in
            uncachedTimer.end()
            XCTAssertNotNil(result)
        })

        XCTAssertNoThrow(try mentat.cache(attribute: ":neighborhood/name", direction: CacheDirection.reverse))
        XCTAssertNoThrow(try mentat.cache(attribute: ":neighborhood/district", direction: CacheDirection.forward))

        var cachedTimer = QueryTimer()
        cachedTimer.start()

        XCTAssertNoThrow(try mentat.query(query: query).run { (result) in
            cachedTimer.end()
            XCTAssertNotNil(result)
        })

        let timingDifference = uncachedTimer.duration() - cachedTimer.duration()
        print("Cached query is \(timingDifference) nanoseconds faster than the uncached query")

        XCTAssertLessThan(cachedTimer.duration(), uncachedTimer.duration())
    }

    // TODO: Add tests for transaction observation
}
