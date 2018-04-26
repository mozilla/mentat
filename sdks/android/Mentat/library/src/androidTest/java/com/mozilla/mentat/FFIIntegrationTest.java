/* -*- Mode: Java; c-basic-offset: 4; tab-width: 20; indent-tabs-mode: nil; -*-
 * Copyright 2018 Mozilla
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License. */

package com.mozilla.mentat;

import android.content.Context;
import android.content.res.AssetManager;
import android.support.test.InstrumentationRegistry;
import android.support.test.runner.AndroidJUnit4;

import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.UUID;

import static org.junit.Assert.*;

/**
 * Instrumentation test, which will execute on an Android device.
 */
@RunWith(AndroidJUnit4.class)
public class FFIIntegrationTest {

    class DBSetupResult {
        TxReport schemaReport;
        TxReport dataReport;

        public DBSetupResult(TxReport schemaReport, TxReport dataReport) {
            this.schemaReport = schemaReport;
            this.dataReport = dataReport;
        }
    }

    Mentat mentat = null;

    @Test
    public void openInMemoryStoreSucceeds() throws Exception {
        Mentat mentat = new Mentat();
        assertNotNull(mentat);
    }

    @Test
    public void openStoreInLocationSucceeds() throws Exception {
        Context context = InstrumentationRegistry.getTargetContext();
        String path = context.getDatabasePath("test.db").getAbsolutePath();
        Mentat mentat = new Mentat(path);
        assertNotNull(mentat);
    }

    public String readFile(String fileName) {
        Context testContext = InstrumentationRegistry.getInstrumentation().getContext();
        AssetManager assetManager = testContext.getAssets();
        try {
            InputStream inputStream = assetManager.open(fileName);
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
            StringBuilder out = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                out.append(line + "\n");
            }
            return out.toString();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return null;
    }

    public TxReport transactCitiesSchema(Mentat mentat) {
        String citiesSchema = this.readFile("cities.schema");
        return mentat.transact(citiesSchema);
    }

    public TxReport transactSeattleData(Mentat mentat) {
        String seattleData = this.readFile("all_seattle.edn");
        return mentat.transact(seattleData);
    }

    public Mentat openAndInitializeCitiesStore() {
        if (this.mentat == null) {
            this.mentat = new Mentat();
            this.transactCitiesSchema(mentat);
            this.transactSeattleData(mentat);
        }

        return this.mentat;
    }

    public DBSetupResult populateWithTypesSchema(Mentat mentat) {
        InProgress transaction = mentat.beginTransaction();
        String schema = "[\n" +
                "                [:db/add \"b\" :db/ident :foo/boolean]\n" +
                "                [:db/add \"b\" :db/valueType :db.type/boolean]\n" +
                "                [:db/add \"b\" :db/cardinality :db.cardinality/one]\n" +
                "                [:db/add \"l\" :db/ident :foo/long]\n" +
                "                [:db/add \"l\" :db/valueType :db.type/long]\n" +
                "                [:db/add \"l\" :db/cardinality :db.cardinality/one]\n" +
                "                [:db/add \"r\" :db/ident :foo/ref]\n" +
                "                [:db/add \"r\" :db/valueType :db.type/ref]\n" +
                "                [:db/add \"r\" :db/cardinality :db.cardinality/one]\n" +
                "                [:db/add \"i\" :db/ident :foo/instant]\n" +
                "                [:db/add \"i\" :db/valueType :db.type/instant]\n" +
                "                [:db/add \"i\" :db/cardinality :db.cardinality/one]\n" +
                "                [:db/add \"d\" :db/ident :foo/double]\n" +
                "                [:db/add \"d\" :db/valueType :db.type/double]\n" +
                "                [:db/add \"d\" :db/cardinality :db.cardinality/one]\n" +
                "                [:db/add \"s\" :db/ident :foo/string]\n" +
                "                [:db/add \"s\" :db/valueType :db.type/string]\n" +
                "                [:db/add \"s\" :db/cardinality :db.cardinality/one]\n" +
                "                [:db/add \"k\" :db/ident :foo/keyword]\n" +
                "                [:db/add \"k\" :db/valueType :db.type/keyword]\n" +
                "                [:db/add \"k\" :db/cardinality :db.cardinality/one]\n" +
                "                [:db/add \"u\" :db/ident :foo/uuid]\n" +
                "                [:db/add \"u\" :db/valueType :db.type/uuid]\n" +
                "                [:db/add \"u\" :db/cardinality :db.cardinality/one]\n" +
                "            ]";
        TxReport report = transaction.transact(schema);
        Long stringEntid = report.getEntidForTempId("s");

        String data = "[\n" +
                "                [:db/add \"a\" :foo/boolean true]\n" +
                "                [:db/add \"a\" :foo/long 25]\n" +
                "                [:db/add \"a\" :foo/instant #inst \"2017-01-01T11:00:00.000Z\"]\n" +
                "                [:db/add \"a\" :foo/double 11.23]\n" +
                "                [:db/add \"a\" :foo/string \"The higher we soar the smaller we appear to those who cannot fly.\"]\n" +
                "                [:db/add \"a\" :foo/keyword :foo/string]\n" +
                "                [:db/add \"a\" :foo/uuid #uuid \"550e8400-e29b-41d4-a716-446655440000\"]\n" +
                "                [:db/add \"b\" :foo/boolean false]\n" +
                "                [:db/add \"b\" :foo/ref "+ stringEntid +"]\n" +
                "                [:db/add \"b\" :foo/keyword :foo/string]\n" +
                "                [:db/add \"b\" :foo/long 50]\n" +
                "                [:db/add \"b\" :foo/instant #inst \"2018-01-01T11:00:00.000Z\"]\n" +
                "                [:db/add \"b\" :foo/double 22.46]\n" +
                "                [:db/add \"b\" :foo/string \"Silence is worse; all truths that are kept silent become poisonous.\"]\n" +
                "                [:db/add \"b\" :foo/uuid #uuid \"4cb3f828-752d-497a-90c9-b1fd516d5644\"]\n" +
                "            ]";
        TxReport dataReport = transaction.transact(data);
        transaction.commit();
        return new DBSetupResult(report, dataReport);
    }

    @Test
    public void transactingVocabularySucceeds() {
        Mentat mentat = new Mentat();
        TxReport schemaReport = this.transactCitiesSchema(mentat);
        assertNotNull(schemaReport);
        assertTrue(schemaReport.getTxId() > 0);
    }

    @Test
    public void transactingEntitiesSucceeds() {
        Mentat mentat = new Mentat();
        this.transactCitiesSchema(mentat);
        TxReport dataReport = this.transactSeattleData(mentat);
        assertNotNull(dataReport);
        assertTrue(dataReport.getTxId() > 0);
        Long entid = dataReport.getEntidForTempId("a17592186045605");
        assertEquals(65733, entid.longValue());
    }

    @Test
    public void runScalarSucceeds() throws InterruptedException {
        Mentat mentat = openAndInitializeCitiesStore();
        String query = "[:find ?n . :in ?name :where [(fulltext $ :community/name ?name) [[?e ?n]]]]";
        final Expectation expectation = new Expectation();
        mentat.query(query).bind("?name", "Wallingford").run(new ScalarResultHandler() {
            @Override
            public void handleValue(TypedValue value) {
                assertNotNull(value);
                assertEquals("KOMO Communities - Wallingford", value.asString());
                expectation.fulfill();
            }
        });
        synchronized (expectation) {
            expectation.wait(1000);
        }
        assertTrue(expectation.isFulfilled);
    }

    @Test
    public void runCollSucceeds() throws InterruptedException {
        Mentat mentat = openAndInitializeCitiesStore();
        String query = "[:find [?when ...] :where [_ :db/txInstant ?when] :order (asc ?when)]";
        final Expectation expectation = new Expectation();
        mentat.query(query).run(new CollResultHandler() {
            @Override
            public void handleList(CollResult list) {
                assertNotNull(list);
                for (int i = 0; i < 3; ++i) {
                    assertNotNull(list.asDate(i));
                }
                expectation.fulfill();
            }
        });
        synchronized (expectation) {
            expectation.wait(1000);
        }
        assertTrue(expectation.isFulfilled);
    }

    @Test
    public void runCollResultIteratorSucceeds() throws InterruptedException {
        Mentat mentat = openAndInitializeCitiesStore();
        String query = "[:find [?when ...] :where [_ :db/txInstant ?when] :order (asc ?when)]";
        final Expectation expectation = new Expectation();
        mentat.query(query).run(new CollResultHandler() {
            @Override
            public void handleList(CollResult list) {
                assertNotNull(list);

                for(TypedValue value: list) {
                    assertNotNull(value.asDate());
                }
                expectation.fulfill();
            }
        });
        synchronized (expectation) {
            expectation.wait(1000);
        }
        assertTrue(expectation.isFulfilled);
    }

    @Test
    public void runTupleSucceeds() throws InterruptedException {
        Mentat mentat = openAndInitializeCitiesStore();
        String query = "[:find [?name ?cat]\n" +
                "        :where\n" +
                "        [?c :community/name ?name]\n" +
                "        [?c :community/type :community.type/website]\n" +
                "        [(fulltext $ :community/category \"food\") [[?c ?cat]]]]";
        final Expectation expectation = new Expectation();
        mentat.query(query).run(new TupleResultHandler() {
            @Override
            public void handleRow(TupleResult row) {
                assertNotNull(row);
                String name = row.asString(0);
                String category = row.asString(1);
                assert(name == "Community Harvest of Southwest Seattle");
                assert(category == "sustainable food");
                expectation.fulfill();
            }
        });
        synchronized (expectation) {
            expectation.wait(1000);
        }
        assertTrue(expectation.isFulfilled);
    }

    @Test
    public void runRelIteratorSucceeds() throws InterruptedException {
        Mentat mentat = openAndInitializeCitiesStore();
        String query = "[:find ?name ?cat\n" +
                "        :where\n" +
                "        [?c :community/name ?name]\n" +
                "        [?c :community/type :community.type/website]\n" +
                "        [(fulltext $ :community/category \"food\") [[?c ?cat]]]]";

        final LinkedHashMap expectedResults = new LinkedHashMap<String, String>();
        expectedResults.put("InBallard", "food");
        expectedResults.put("Seattle Chinatown Guide", "food");
        expectedResults.put("Community Harvest of Southwest Seattle", "sustainable food");
        expectedResults.put("University District Food Bank", "food bank");
        final Expectation expectation = new Expectation();
        mentat.query(query).run(new RelResultHandler() {
            @Override
            public void handleRows(RelResult rows) {
                assertNotNull(rows);
                int index = 0;
                for (TupleResult row: rows) {
                    String name = row.asString(0);
                    assertNotNull(name);
                    String category = row.asString(1);
                    assertNotNull(category);
                    String expectedCategory = expectedResults.get(name).toString();
                    assertNotNull(expectedCategory);
                    assertEquals(expectedCategory, category);
                    ++index;
                }
                assertEquals(expectedResults.size(), index);
                expectation.fulfill();
            }
        });
        synchronized (expectation) {
            expectation.wait(1000);
        }
        assertTrue(expectation.isFulfilled);
    }

    @Test
    public void runRelSucceeds() throws InterruptedException {
        Mentat mentat = openAndInitializeCitiesStore();
        String query = "[:find ?name ?cat\n" +
                "        :where\n" +
                "        [?c :community/name ?name]\n" +
                "        [?c :community/type :community.type/website]\n" +
                "        [(fulltext $ :community/category \"food\") [[?c ?cat]]]]";

        final LinkedHashMap expectedResults = new LinkedHashMap<String, String>();
        expectedResults.put("InBallard", "food");
        expectedResults.put("Seattle Chinatown Guide", "food");
        expectedResults.put("Community Harvest of Southwest Seattle", "sustainable food");
        expectedResults.put("University District Food Bank", "food bank");
        final Expectation expectation = new Expectation();
        mentat.query(query).run(new RelResultHandler() {
            @Override
            public void handleRows(RelResult rows) {
                assertNotNull(rows);
                for (int i = 0; i < expectedResults.size(); ++i) {
                    TupleResult row = rows.rowAtIndex(i);
                    assertNotNull(row);
                    String name = row.asString(0);
                    assertNotNull(name);
                    String category = row.asString(1);
                    assertNotNull(category);
                    String expectedCategory = expectedResults.get(name).toString();
                    assertNotNull(expectedCategory);
                    assertEquals(expectedCategory, category);
                }
                expectation.fulfill();
            }
        });
        synchronized (expectation) {
            expectation.wait(1000);
        }
        assertTrue(expectation.isFulfilled);
    }

    @Test
    public void bindingLongValueSucceeds() throws InterruptedException {
        Mentat mentat = new Mentat();
        TxReport report = this.populateWithTypesSchema(mentat).dataReport;
        final Long aEntid = report.getEntidForTempId("a");
        String query = "[:find ?e . :in ?long :where [?e :foo/long ?long]]";
        final Expectation expectation = new Expectation();
        mentat.query(query).bind("?long", 25).run(new ScalarResultHandler() {
            @Override
            public void handleValue(TypedValue value) {
                assertNotNull(value);
                assertEquals(aEntid, value.asEntid());
                expectation.fulfill();
            }
        });
        synchronized (expectation) {
            expectation.wait(1000);
        }
        assertTrue(expectation.isFulfilled);
    }

    @Test
    public void bindingRefValueSucceeds() throws InterruptedException {
        Mentat mentat = new Mentat();
        TxReport report = this.populateWithTypesSchema(mentat).dataReport;
        long stringEntid = mentat.entIdForAttribute(":foo/string");
        final Long bEntid = report.getEntidForTempId("b");
        String query = "[:find ?e . :in ?ref :where [?e :foo/ref ?ref]]";
        final Expectation expectation = new Expectation();
        mentat.query(query).bindEntidReference("?ref", stringEntid).run(new ScalarResultHandler() {
            @Override
            public void handleValue(TypedValue value) {
                assertNotNull(value);
                assertEquals(bEntid, value.asEntid());
                expectation.fulfill();
            }
        });
        synchronized (expectation) {
            expectation.wait(1000);
        }
        assertTrue(expectation.isFulfilled);
    }

    @Test
    public void bindingRefKwValueSucceeds() throws InterruptedException {
        Mentat mentat = new Mentat();
        TxReport report = this.populateWithTypesSchema(mentat).dataReport;
        String refKeyword = ":foo/string";
        final Long bEntid = report.getEntidForTempId("b");
        String query = "[:find ?e . :in ?ref :where [?e :foo/ref ?ref]]";
        final Expectation expectation = new Expectation();
        mentat.query(query).bindKeywordReference("?ref", refKeyword).run(new ScalarResultHandler() {
            @Override
            public void handleValue(TypedValue value) {
                assertNotNull(value);
                assertEquals(bEntid, value.asEntid());
                expectation.fulfill();
            }
        });
        synchronized (expectation) {
            expectation.wait(1000);
        }
        assertTrue(expectation.isFulfilled);
    }

    @Test
    public void bindingKwValueSucceeds() throws InterruptedException {
        Mentat mentat = new Mentat();
        TxReport report = this.populateWithTypesSchema(mentat).dataReport;
        final Long aEntid = report.getEntidForTempId("a");
        String query = "[:find ?e . :in ?kw :where [?e :foo/keyword ?kw]]";
        final Expectation expectation = new Expectation();
        mentat.query(query).bindKeyword("?kw", ":foo/string").run(new ScalarResultHandler() {
            @Override
            public void handleValue(TypedValue value) {
                assertNotNull(value);
                assertEquals(aEntid, value.asEntid());
                expectation.fulfill();
            }
        });
        synchronized (expectation) {
            expectation.wait(1000);
        }
        assertTrue(expectation.isFulfilled);
    }

    @Test
    public void bindingDateValueSucceeds() throws InterruptedException, ParseException {
        Mentat mentat = new Mentat();
        TxReport report = this.populateWithTypesSchema(mentat).dataReport;
        final Long aEntid = report.getEntidForTempId("a");

        Date date = new Date(1523896758000L);
        String query = "[:find [?e ?d] :in ?now :where [?e :foo/instant ?d] [(< ?d ?now)]]";
        final Expectation expectation = new Expectation();
        mentat.query(query).bind("?now", date).run(new TupleResultHandler() {
            @Override
            public void handleRow(TupleResult row) {
                assertNotNull(row);
                TypedValue value = row.get(0);
                assertNotNull(value);
                assertEquals(aEntid, value.asEntid());
                expectation.fulfill();
            }
        });
        synchronized (expectation) {
            expectation.wait(1000);
        }
        assertTrue(expectation.isFulfilled);
    }

    @Test
    public void bindingStringValueSucceeds() throws InterruptedException {
        Mentat mentat = this.openAndInitializeCitiesStore();
        String query = "[:find ?n . :in ?name :where [(fulltext $ :community/name ?name) [[?e ?n]]]]";
        final Expectation expectation = new Expectation();
        mentat.query(query).bind("?name", "Wallingford").run(new ScalarResultHandler() {
            @Override
            public void handleValue(TypedValue value) {
                assertNotNull(value);
                assertEquals("KOMO Communities - Wallingford", value.asString());
                expectation.fulfill();
            }
        });
        synchronized (expectation) {
            expectation.wait(1000);
        }
        assertTrue(expectation.isFulfilled);
    }

    @Test
    public void bindingUuidValueSucceeds() throws InterruptedException {
        Mentat mentat = new Mentat();
        TxReport report = this.populateWithTypesSchema(mentat).dataReport;
        final Long aEntid = report.getEntidForTempId("a");
        String query = "[:find ?e . :in ?uuid :where [?e :foo/uuid ?uuid]]";
        UUID uuid = UUID.fromString("550e8400-e29b-41d4-a716-446655440000");
        final Expectation expectation = new Expectation();
        mentat.query(query).bind("?uuid", uuid).run(new ScalarResultHandler() {
            @Override
            public void handleValue(TypedValue value) {
                assertNotNull(value);
                assertEquals(aEntid, value.asEntid());
                expectation.fulfill();
            }
        });
        synchronized (expectation) {
            expectation.wait(1000);
        }
        assertTrue(expectation.isFulfilled);
    }

    @Test
    public void bindingBooleanValueSucceeds() throws InterruptedException {
        Mentat mentat = new Mentat();
        TxReport report = this.populateWithTypesSchema(mentat).dataReport;
        final Long aEntid = report.getEntidForTempId("a");
        String query = "[:find ?e . :in ?bool :where [?e :foo/boolean ?bool]]";
        final Expectation expectation = new Expectation();
        mentat.query(query).bind("?bool", true).run(new ScalarResultHandler() {
            @Override
            public void handleValue(TypedValue value) {
                assertNotNull(value);
                assertEquals(aEntid, value.asEntid());
                expectation.fulfill();
            }
        });

        synchronized (expectation) {
            expectation.wait(1000);
        }
        assertTrue(expectation.isFulfilled);
    }

    @Test
    public void bindingDoubleValueSucceeds() throws InterruptedException {
        Mentat mentat = new Mentat();
        TxReport report = this.populateWithTypesSchema(mentat).dataReport;
        final Long aEntid = report.getEntidForTempId("a");
        String query = "[:find ?e . :in ?double :where [?e :foo/double ?double]]";
        final Expectation expectation = new Expectation();
        mentat.query(query).bind("?double", 11.23).run(new ScalarResultHandler() {
            @Override
            public void handleValue(TypedValue value) {
                assertNotNull(value);
                assertEquals(aEntid, value.asEntid());
                expectation.fulfill();
            }
        });
        synchronized (expectation) {
            expectation.wait(1000);
        }
        assertTrue(expectation.isFulfilled);
    }

    @Test
    public void typedValueConvertsToLong() throws InterruptedException {
        Mentat mentat = new Mentat();
        TxReport report = this.populateWithTypesSchema(mentat).dataReport;
        final Long aEntid = report.getEntidForTempId("a");
        String query = "[:find ?v . :in ?e :where [?e :foo/long ?v]]";
        final Expectation expectation = new Expectation();
        mentat.query(query).bindEntidReference("?e", aEntid).run(new ScalarResultHandler() {
            @Override
            public void handleValue(TypedValue value) {
                assertNotNull(value);
                assertEquals(25, value.asLong().longValue());
                assertEquals(25, value.asLong().longValue());
                expectation.fulfill();
            }
        });
        synchronized (expectation) {
            expectation.wait(1000);
        }
        assertTrue(expectation.isFulfilled);
    }

    @Test
    public void typedValueConvertsToRef() throws InterruptedException {
        Mentat mentat = new Mentat();
        TxReport report = this.populateWithTypesSchema(mentat).dataReport;
        final Long aEntid = report.getEntidForTempId("a");
        String query = "[:find ?e . :where [?e :foo/long 25]]";
        final Expectation expectation = new Expectation();
        mentat.query(query).run(new ScalarResultHandler() {
            @Override
            public void handleValue(TypedValue value) {
                assertNotNull(value);
                assertEquals(aEntid, value.asEntid());
                assertEquals(aEntid, value.asEntid());
                expectation.fulfill();
            }
        });
        synchronized (expectation) {
            expectation.wait(1000);
        }
        assertTrue(expectation.isFulfilled);
    }

    @Test
    public void typedValueConvertsToKeyword() throws InterruptedException {
        Mentat mentat = new Mentat();
        TxReport report = this.populateWithTypesSchema(mentat).dataReport;
        final Long aEntid = report.getEntidForTempId("a");
        String query = "[:find ?v . :in ?e :where [?e :foo/keyword ?v]]";
        final Expectation expectation = new Expectation();
        mentat.query(query).bindEntidReference("?e", aEntid).run(new ScalarResultHandler() {
            @Override
            public void handleValue(TypedValue value) {
                assertNotNull(value);
                assertEquals(":foo/string", value.asKeyword());
                assertEquals(":foo/string", value.asKeyword());
                expectation.fulfill();
            }
        });
        synchronized (expectation) {
            expectation.wait(1000);
        }
        assertTrue(expectation.isFulfilled);
    }

    @Test
    public void typedValueConvertsToBoolean() throws InterruptedException {
        Mentat mentat = new Mentat();
        TxReport report = this.populateWithTypesSchema(mentat).dataReport;
        final Long aEntid = report.getEntidForTempId("a");
        String query = "[:find ?v . :in ?e :where [?e :foo/boolean ?v]]";
        final Expectation expectation = new Expectation();
        mentat.query(query).bindEntidReference("?e", aEntid).run(new ScalarResultHandler() {
            @Override
            public void handleValue(TypedValue value) {
                assertNotNull(value);
                assertEquals(true, value.asBoolean());
                assertEquals(true, value.asBoolean());
                expectation.fulfill();
            }
        });
        synchronized (expectation) {
            expectation.wait(1000);
        }
        assertTrue(expectation.isFulfilled);
    }

    @Test
    public void typedValueConvertsToDouble() throws InterruptedException {
        Mentat mentat = new Mentat();
        TxReport report = this.populateWithTypesSchema(mentat).dataReport;
        final Long aEntid = report.getEntidForTempId("a");
        String query = "[:find ?v . :in ?e :where [?e :foo/double ?v]]";
        final Expectation expectation = new Expectation();
        mentat.query(query).bindEntidReference("?e", aEntid).run(new ScalarResultHandler() {
            @Override
            public void handleValue(TypedValue value) {
                assertNotNull(value);
                assertEquals(new Double(11.23), value.asDouble());
                assertEquals(new Double(11.23), value.asDouble());
                expectation.fulfill();
            }
        });
        synchronized (expectation) {
            expectation.wait(1000);
        }
        assertTrue(expectation.isFulfilled);
    }

    @Test
    public void typedValueConvertsToDate() throws InterruptedException, ParseException {
        Mentat mentat = new Mentat();
        TxReport report = this.populateWithTypesSchema(mentat).dataReport;
        final Long aEntid = report.getEntidForTempId("a");
        String query = "[:find ?v . :in ?e :where [?e :foo/instant ?v]]";
        final Expectation expectation = new Expectation();
        DateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZZZZZ", Locale.ENGLISH);
        format.parse("2017-01-01T11:00:00+00:00");
        final Calendar expectedDate = format.getCalendar();
        mentat.query(query).bindEntidReference("?e", aEntid).run(new ScalarResultHandler() {
            @Override
            public void handleValue(TypedValue value) {
                assertNotNull(value);
                assertEquals(expectedDate.getTime(), value.asDate());
                assertEquals(expectedDate.getTime(), value.asDate());
                expectation.fulfill();
            }
        });
        synchronized (expectation) {
            expectation.wait(1000);
        }
        assertTrue(expectation.isFulfilled);
    }

    @Test
    public void typedValueConvertsToString() throws InterruptedException {
        Mentat mentat = new Mentat();
        TxReport report = this.populateWithTypesSchema(mentat).dataReport;
        final Long aEntid = report.getEntidForTempId("a");
        String query = "[:find ?v . :in ?e :where [?e :foo/string ?v]]";
        final Expectation expectation = new Expectation();
        mentat.query(query).bindEntidReference("?e", aEntid).run(new ScalarResultHandler() {
            @Override
            public void handleValue(TypedValue value) {
                assertNotNull(value);
                assertEquals("The higher we soar the smaller we appear to those who cannot fly.", value.asString());
                assertEquals("The higher we soar the smaller we appear to those who cannot fly.", value.asString());
                expectation.fulfill();
            }
        });
        synchronized (expectation) {
            expectation.wait(1000);
        }
        assertTrue(expectation.isFulfilled);
    }

    @Test
    public void typedValueConvertsToUUID() throws InterruptedException {
        Mentat mentat = new Mentat();
        TxReport report = this.populateWithTypesSchema(mentat).dataReport;
        final Long aEntid = report.getEntidForTempId("a");
        String query = "[:find ?v . :in ?e :where [?e :foo/uuid ?v]]";
        final UUID expectedUUID = UUID.fromString("550e8400-e29b-41d4-a716-446655440000");
        final Expectation expectation = new Expectation();
        mentat.query(query).bindEntidReference("?e", aEntid).run(new ScalarResultHandler() {
            @Override
            public void handleValue(TypedValue value) {
                assertNotNull(value);
                assertEquals(expectedUUID, value.asUUID());
                assertEquals(expectedUUID, value.asUUID());
                expectation.fulfill();
            }
        });
        synchronized (expectation) {
            expectation.wait(1000);
        }
        assertTrue(expectation.isFulfilled);
    }

    @Test
    public void valueForAttributeOfEntitySucceeds() throws InterruptedException {
        Mentat mentat = new Mentat();
        TxReport report = this.populateWithTypesSchema(mentat).dataReport;
        final Long aEntid = report.getEntidForTempId("a");
        TypedValue value = mentat.valueForAttributeOfEntity(":foo/long", aEntid);
        assertNotNull(value);
        assertEquals(25, value.asLong().longValue());
    }

    @Test
    public void entidForAttributeSucceeds() {
        Mentat mentat = new Mentat();
        this.populateWithTypesSchema(mentat);
        long entid = mentat.entIdForAttribute(":foo/long");
        assertEquals(65540, entid);
    }

    @Test
    public void testInProgressTransact() {
        Mentat mentat = new Mentat();
        TxReport report = this.populateWithTypesSchema(mentat).dataReport;
        assertNotNull(report);

    }

    @Test
    public void testInProgressRollback() {
        Mentat mentat = new Mentat();
        TxReport report = this.populateWithTypesSchema(mentat).dataReport;
        assertNotNull(report);
        long aEntid = report.getEntidForTempId("a");
        TypedValue preLongValue = mentat.valueForAttributeOfEntity(":foo/long", aEntid);
        assertEquals(25, preLongValue.asLong().longValue());

        InProgress inProgress = mentat.beginTransaction();
        report = inProgress.transact("[[:db/add "+ aEntid +" :foo/long 22]]");
        assertNotNull(report);
        inProgress.rollback();

        TypedValue postLongValue = mentat.valueForAttributeOfEntity(":foo/long", aEntid);
        assertEquals(25, postLongValue.asLong().longValue());
    }

    @Test
    public void testInProgressEntityBuilder() throws InterruptedException {
        Mentat mentat = new Mentat();
        DBSetupResult reports = this.populateWithTypesSchema(mentat);
        long bEntid = reports.dataReport.getEntidForTempId("b");
        final long longEntid = reports.schemaReport.getEntidForTempId("l");
        final long stringEntid = reports.schemaReport.getEntidForTempId("s");

        // test that the values are as expected
        String query = "[:find [?b ?i ?u ?l ?d ?s ?k ?r]\n" +
                "                     :in ?e\n" +
                "                :where [?e :foo/boolean ?b]\n" +
                "                            [?e :foo/instant ?i]\n" +
                "                            [?e :foo/uuid ?u]\n" +
                "                            [?e :foo/long ?l]\n" +
                "                            [?e :foo/double ?d]\n" +
                "                            [?e :foo/string ?s]\n" +
                "                            [?e :foo/keyword ?k]\n" +
                "                            [?e :foo/ref ?r]]";

        final Expectation expectation1 = new Expectation();
        mentat.query(query).bindEntidReference("?e", bEntid).run(new TupleResultHandler() {
            @Override
            public void handleRow(TupleResult row) {
                assertNotNull(row);
                assertEquals(false, row.asBool(0));
                assertEquals(new Date(1514804400000l), row.asDate(1));
                assertEquals(UUID.fromString("4cb3f828-752d-497a-90c9-b1fd516d5644"), row.asUUID(2));
                assertEquals(50, row.asLong(3).longValue());
                assertEquals(new Double(22.46), row.asDouble(4));
                assertEquals("Silence is worse; all truths that are kept silent become poisonous.", row.asString(5));
                assertEquals(":foo/string", row.asKeyword(6));
                assertEquals(stringEntid, row.asEntid(7).longValue());
                expectation1.fulfill();
            }
        });

        synchronized (expectation1) {
            expectation1.wait(1000);
        }
        assertTrue(expectation1.isFulfilled);

        InProgressBuilder builder = mentat.entityBuilder();
        builder.add(bEntid, ":foo/boolean", true);
        final Date newDate = new Date(1524743301000l);
        builder.add(bEntid, ":foo/instant", newDate);
        final UUID newUUID = UUID.randomUUID();
        builder.add(bEntid, ":foo/uuid", newUUID);
        builder.add(bEntid, ":foo/long", 75);
        builder.add(bEntid, ":foo/double", 81.3);
        builder.add(bEntid, ":foo/string", "Become who you are!");
        builder.addKeyword(bEntid, ":foo/keyword", ":foo/long");
        builder.addRef(bEntid, ":foo/ref", longEntid);
        builder.commit();


        final Expectation expectation2 = new Expectation();
        mentat.query(query).bindEntidReference("?e", bEntid).run(new TupleResultHandler() {
            @Override
            public void handleRow(TupleResult row) {
                assertNotNull(row);
                assertEquals(true, row.asBool(0));
                System.out.println(row.asDate(1).getTime());
                assertEquals(newDate, row.asDate(1));
                assertEquals(newUUID, row.asUUID(2));
                assertEquals(75, row.asLong(3).longValue());
                assertEquals(new Double(81.3), row.asDouble(4));
                assertEquals("Become who you are!", row.asString(5));
                assertEquals(":foo/long", row.asKeyword(6));
                assertEquals(longEntid, row.asEntid(7).longValue());
                expectation2.fulfill();
            }
        });

        synchronized (expectation2) {
            expectation2.wait(1000);
        }
        assertTrue(expectation2.isFulfilled);
    }

    @Test
    public void testEntityBuilderForEntid() throws InterruptedException {
        Mentat mentat = new Mentat();
        DBSetupResult reports = this.populateWithTypesSchema(mentat);
        long bEntid = reports.dataReport.getEntidForTempId("b");
        final long longEntid = reports.schemaReport.getEntidForTempId("l");
        final long stringEntid = reports.schemaReport.getEntidForTempId("s");

        // test that the values are as expected
        String query = "[:find [?b ?i ?u ?l ?d ?s ?k ?r]\n" +
                "                     :in ?e\n" +
                "                :where [?e :foo/boolean ?b]\n" +
                "                            [?e :foo/instant ?i]\n" +
                "                            [?e :foo/uuid ?u]\n" +
                "                            [?e :foo/long ?l]\n" +
                "                            [?e :foo/double ?d]\n" +
                "                            [?e :foo/string ?s]\n" +
                "                            [?e :foo/keyword ?k]\n" +
                "                            [?e :foo/ref ?r]]";

        final Expectation expectation1 = new Expectation();
        mentat.query(query).bindEntidReference("?e", bEntid).run(new TupleResultHandler() {
            @Override
            public void handleRow(TupleResult row) {
                assertNotNull(row);
                assertEquals(false, row.asBool(0));
                assertEquals(new Date(1514804400000l), row.asDate(1));
                assertEquals(UUID.fromString("4cb3f828-752d-497a-90c9-b1fd516d5644"), row.asUUID(2));
                assertEquals(50, row.asLong(3).longValue());
                assertEquals(new Double(22.46), row.asDouble(4));
                assertEquals("Silence is worse; all truths that are kept silent become poisonous.", row.asString(5));
                assertEquals(":foo/string", row.asKeyword(6));
                assertEquals(stringEntid, row.asEntid(7).longValue());
                expectation1.fulfill();
            }
        });

        synchronized (expectation1) {
            expectation1.wait(1000);
        }
        assertTrue(expectation1.isFulfilled);

        EntityBuilder builder = mentat.entityBuilder(bEntid);
        builder.add(":foo/boolean", true);
        final Date newDate = new Date(1524743301000l);
        builder.add(":foo/instant", newDate);
        final UUID newUUID = UUID.randomUUID();
        builder.add(":foo/uuid", newUUID);
        builder.add(":foo/long", 75);
        builder.add(":foo/double", 81.3);
        builder.add(":foo/string", "Become who you are!");
        builder.addKeyword(":foo/keyword", ":foo/long");
        builder.addRef(":foo/ref", longEntid);
        builder.commit();


        final Expectation expectation2 = new Expectation();
        mentat.query(query).bindEntidReference("?e", bEntid).run(new TupleResultHandler() {
            @Override
            public void handleRow(TupleResult row) {
                assertNotNull(row);
                assertEquals(true, row.asBool(0));
                System.out.println(row.asDate(1).getTime());
                assertEquals(newDate, row.asDate(1));
                assertEquals(newUUID, row.asUUID(2));
                assertEquals(75, row.asLong(3).longValue());
                assertEquals(new Double(81.3), row.asDouble(4));
                assertEquals("Become who you are!", row.asString(5));
                assertEquals(":foo/long", row.asKeyword(6));
                assertEquals(longEntid, row.asEntid(7).longValue());
                expectation2.fulfill();
            }
        });

        synchronized (expectation2) {
            expectation2.wait(1000);
        }
        assertTrue(expectation2.isFulfilled);
    }

    @Test
    public void testEntityBuilderForTempid() throws InterruptedException {
        Mentat mentat = new Mentat();
        DBSetupResult reports = this.populateWithTypesSchema(mentat);
        final long longEntid = reports.schemaReport.getEntidForTempId("l");

        EntityBuilder builder = mentat.entityBuilder("c");
        builder.add(":foo/boolean", true);
        final Date newDate = new Date(1524743301000l);
        builder.add(":foo/instant", newDate);
        final UUID newUUID = UUID.randomUUID();
        builder.add(":foo/uuid", newUUID);
        builder.add(":foo/long", 75);
        builder.add(":foo/double", 81.3);
        builder.add(":foo/string", "Become who you are!");
        builder.addKeyword(":foo/keyword", ":foo/long");
        builder.addRef(":foo/ref", longEntid);
        TxReport report = builder.commit();
        long cEntid = report.getEntidForTempId("c");

        // test that the values are as expected
        String query = "[:find [?b ?i ?u ?l ?d ?s ?k ?r]\n" +
                "                     :in ?e\n" +
                "                :where [?e :foo/boolean ?b]\n" +
                "                            [?e :foo/instant ?i]\n" +
                "                            [?e :foo/uuid ?u]\n" +
                "                            [?e :foo/long ?l]\n" +
                "                            [?e :foo/double ?d]\n" +
                "                            [?e :foo/string ?s]\n" +
                "                            [?e :foo/keyword ?k]\n" +
                "                            [?e :foo/ref ?r]]";

        final Expectation expectation = new Expectation();
        mentat.query(query).bindEntidReference("?e", cEntid).run(new TupleResultHandler() {
            @Override
            public void handleRow(TupleResult row) {
                assertNotNull(row);
                assertEquals(true, row.asBool(0));
                System.out.println(row.asDate(1).getTime());
                assertEquals(newDate, row.asDate(1));
                assertEquals(newUUID, row.asUUID(2));
                assertEquals(75, row.asLong(3).longValue());
                assertEquals(new Double(81.3), row.asDouble(4));
                assertEquals("Become who you are!", row.asString(5));
                assertEquals(":foo/long", row.asKeyword(6));
                assertEquals(longEntid, row.asEntid(7).longValue());
                expectation.fulfill();
            }
        });

        synchronized (expectation) {
            expectation.wait(1000);
        }
        assertTrue(expectation.isFulfilled);
    }

    @Test
    public void testInProgressBuilderTransact() throws InterruptedException {
        Mentat mentat = new Mentat();
        DBSetupResult reports = this.populateWithTypesSchema(mentat);
        long aEntid = reports.dataReport.getEntidForTempId("a");
        long bEntid = reports.dataReport.getEntidForTempId("b");
        final long longEntid = reports.schemaReport.getEntidForTempId("l");
        InProgressBuilder builder = mentat.entityBuilder();
        builder.add(bEntid, ":foo/boolean", true);
        final Date newDate = new Date(1524743301000l);
        builder.add(bEntid, ":foo/instant", newDate);
        final UUID newUUID = UUID.randomUUID();
        builder.add(bEntid, ":foo/uuid", newUUID);
        builder.add(bEntid, ":foo/long", 75);
        builder.add(bEntid, ":foo/double", 81.3);
        builder.add(bEntid, ":foo/string", "Become who you are!");
        builder.addKeyword(bEntid, ":foo/keyword", ":foo/long");
        builder.addRef(bEntid, ":foo/ref", longEntid);
        InProgressTransactionResult result = builder.transact();
        assertNotNull(result);
        assertNotNull(result.getInProgress());
        assertNotNull(result.getReport());
        result.getInProgress().transact("[[:db/add "+ aEntid +" :foo/long 22]]");
        result.getInProgress().commit();

        // test that the values are as expected
        String query = "[:find [?b ?i ?u ?l ?d ?s ?k ?r]\n" +
                "                     :in ?e\n" +
                "                :where [?e :foo/boolean ?b]\n" +
                "                            [?e :foo/instant ?i]\n" +
                "                            [?e :foo/uuid ?u]\n" +
                "                            [?e :foo/long ?l]\n" +
                "                            [?e :foo/double ?d]\n" +
                "                            [?e :foo/string ?s]\n" +
                "                            [?e :foo/keyword ?k]\n" +
                "                            [?e :foo/ref ?r]]";

        final Expectation expectation = new Expectation();
        mentat.query(query).bindEntidReference("?e", bEntid).run(new TupleResultHandler() {
            @Override
            public void handleRow(TupleResult row) {
                assertNotNull(row);
                assertEquals(true, row.asBool(0));
                System.out.println(row.asDate(1).getTime());
                assertEquals(newDate, row.asDate(1));
                assertEquals(newUUID, row.asUUID(2));
                assertEquals(75, row.asLong(3).longValue());
                assertEquals(new Double(81.3), row.asDouble(4));
                assertEquals("Become who you are!", row.asString(5));
                assertEquals(":foo/long", row.asKeyword(6));
                assertEquals(longEntid, row.asEntid(7).longValue());
                expectation.fulfill();
            }
        });

        synchronized (expectation) {
            expectation.wait(1000);
        }
        assertTrue(expectation.isFulfilled);

        TypedValue longValue = mentat.valueForAttributeOfEntity(":foo/long", aEntid);
        assertEquals(22, longValue.asLong().longValue());
    }

    @Test
    public void testEntityBuilderTransact() throws InterruptedException {
        Mentat mentat = new Mentat();
        DBSetupResult reports = this.populateWithTypesSchema(mentat);
        long aEntid = reports.dataReport.getEntidForTempId("a");
        long bEntid = reports.dataReport.getEntidForTempId("b");
        final long longEntid = reports.schemaReport.getEntidForTempId("l");

        EntityBuilder builder = mentat.entityBuilder(bEntid);
        builder.add(":foo/boolean", true);
        final Date newDate = new Date(1524743301000l);
        builder.add(":foo/instant", newDate);
        final UUID newUUID = UUID.randomUUID();
        builder.add(":foo/uuid", newUUID);
        builder.add(":foo/long", 75);
        builder.add(":foo/double", 81.3);
        builder.add(":foo/string", "Become who you are!");
        builder.addKeyword(":foo/keyword", ":foo/long");
        builder.addRef(":foo/ref", longEntid);
        InProgressTransactionResult result = builder.transact();
        assertNotNull(result);
        assertNotNull(result.getInProgress());
        assertNotNull(result.getReport());
        result.getInProgress().transact("[[:db/add "+ aEntid +" :foo/long 22]]");
        result.getInProgress().commit();

        // test that the values are as expected
        String query = "[:find [?b ?i ?u ?l ?d ?s ?k ?r]\n" +
                "                     :in ?e\n" +
                "                :where [?e :foo/boolean ?b]\n" +
                "                            [?e :foo/instant ?i]\n" +
                "                            [?e :foo/uuid ?u]\n" +
                "                            [?e :foo/long ?l]\n" +
                "                            [?e :foo/double ?d]\n" +
                "                            [?e :foo/string ?s]\n" +
                "                            [?e :foo/keyword ?k]\n" +
                "                            [?e :foo/ref ?r]]";

        final Expectation expectation = new Expectation();
        mentat.query(query).bindEntidReference("?e", bEntid).run(new TupleResultHandler() {
            @Override
            public void handleRow(TupleResult row) {
                assertNotNull(row);
                assertEquals(true, row.asBool(0));
                System.out.println(row.asDate(1).getTime());
                assertEquals(newDate, row.asDate(1));
                assertEquals(newUUID, row.asUUID(2));
                assertEquals(75, row.asLong(3).longValue());
                assertEquals(new Double(81.3), row.asDouble(4));
                assertEquals("Become who you are!", row.asString(5));
                assertEquals(":foo/long", row.asKeyword(6));
                assertEquals(longEntid, row.asEntid(7).longValue());
                expectation.fulfill();
            }
        });

        synchronized (expectation) {
            expectation.wait(1000);
        }
        assertTrue(expectation.isFulfilled);

        TypedValue longValue = mentat.valueForAttributeOfEntity(":foo/long", aEntid);
        assertEquals(22, longValue.asLong().longValue());
    }

    @Test
    public void testEntityBuilderRetract() throws InterruptedException {
        Mentat mentat = new Mentat();
        DBSetupResult reports = this.populateWithTypesSchema(mentat);
        long bEntid = reports.dataReport.getEntidForTempId("b");
        final long longEntid = reports.schemaReport.getEntidForTempId("l");
        final long stringEntid = reports.schemaReport.getEntidForTempId("s");

        // test that the values are as expected
        String query = "[:find [?b ?i ?u ?l ?d ?s ?k ?r]\n" +
                "                     :in ?e\n" +
                "                :where [?e :foo/boolean ?b]\n" +
                "                            [?e :foo/instant ?i]\n" +
                "                            [?e :foo/uuid ?u]\n" +
                "                            [?e :foo/long ?l]\n" +
                "                            [?e :foo/double ?d]\n" +
                "                            [?e :foo/string ?s]\n" +
                "                            [?e :foo/keyword ?k]\n" +
                "                            [?e :foo/ref ?r]]";

        final Expectation expectation1 = new Expectation();
        final Date previousDate = new Date(1514804400000l);
        final UUID previousUuid = UUID.fromString("4cb3f828-752d-497a-90c9-b1fd516d5644");
        mentat.query(query).bindEntidReference("?e", bEntid).run(new TupleResultHandler() {
            @Override
            public void handleRow(TupleResult row) {
                assertNotNull(row);
                assertEquals(false, row.asBool(0));
                assertEquals(previousDate, row.asDate(1));
                assertEquals(previousUuid, row.asUUID(2));
                assertEquals(50, row.asLong(3).longValue());
                assertEquals(new Double(22.46), row.asDouble(4));
                assertEquals("Silence is worse; all truths that are kept silent become poisonous.", row.asString(5));
                assertEquals(":foo/string", row.asKeyword(6));
                assertEquals(stringEntid, row.asEntid(7).longValue());
                expectation1.fulfill();
            }
        });

        synchronized (expectation1) {
            expectation1.wait(1000);
        }

        EntityBuilder builder = mentat.entityBuilder(bEntid);
        builder.retract(":foo/boolean", false);
        builder.retract(":foo/instant", previousDate);
        builder.retract(":foo/uuid", previousUuid);
        builder.retract(":foo/long", 50);
        builder.retract(":foo/double", 22.46);
        builder.retract(":foo/string", "Silence is worse; all truths that are kept silent become poisonous.");
        builder.retractKeyword(":foo/keyword", ":foo/string");
        builder.retractRef(":foo/ref", stringEntid);
        builder.commit();

        final Expectation expectation2 = new Expectation();
        mentat.query(query).bindEntidReference("?e", bEntid).run(new TupleResultHandler() {
            @Override
            public void handleRow(TupleResult row) {
                assertNull(row);
                expectation2.fulfill();
            }
        });

        synchronized (expectation2) {
            expectation2.wait(1000);
        }
    }

    @Test
    public void testInProgressBuilderRetract() throws InterruptedException {
        Mentat mentat = new Mentat();
        DBSetupResult reports = this.populateWithTypesSchema(mentat);
        long bEntid = reports.dataReport.getEntidForTempId("b");
        final long longEntid = reports.schemaReport.getEntidForTempId("l");
        final long stringEntid = reports.schemaReport.getEntidForTempId("s");

        // test that the values are as expected
        String query = "[:find [?b ?i ?u ?l ?d ?s ?k ?r]\n" +
                "                     :in ?e\n" +
                "                :where [?e :foo/boolean ?b]\n" +
                "                            [?e :foo/instant ?i]\n" +
                "                            [?e :foo/uuid ?u]\n" +
                "                            [?e :foo/long ?l]\n" +
                "                            [?e :foo/double ?d]\n" +
                "                            [?e :foo/string ?s]\n" +
                "                            [?e :foo/keyword ?k]\n" +
                "                            [?e :foo/ref ?r]]";

        final Expectation expectation1 = new Expectation();
        final Date previousDate = new Date(1514804400000l);
        final UUID previousUuid = UUID.fromString("4cb3f828-752d-497a-90c9-b1fd516d5644");
        mentat.query(query).bindEntidReference("?e", bEntid).run(new TupleResultHandler() {
            @Override
            public void handleRow(TupleResult row) {
                assertNotNull(row);
                assertEquals(false, row.asBool(0));
                assertEquals(previousDate, row.asDate(1));
                assertEquals(previousUuid, row.asUUID(2));
                assertEquals(50, row.asLong(3).longValue());
                assertEquals(new Double(22.46), row.asDouble(4));
                assertEquals("Silence is worse; all truths that are kept silent become poisonous.", row.asString(5));
                assertEquals(":foo/string", row.asKeyword(6));
                assertEquals(stringEntid, row.asEntid(7).longValue());
                expectation1.fulfill();
            }
        });

        synchronized (expectation1) {
            expectation1.wait(1000);
        }

        InProgressBuilder builder = mentat.entityBuilder();
        builder.retract(bEntid, ":foo/boolean", false);
        builder.retract(bEntid, ":foo/instant", previousDate);
        builder.retract(bEntid, ":foo/uuid", previousUuid);
        builder.retract(bEntid, ":foo/long", 50);
        builder.retract(bEntid, ":foo/double", 22.46);
        builder.retract(bEntid, ":foo/string", "Silence is worse; all truths that are kept silent become poisonous.");
        builder.retractKeyword(bEntid, ":foo/keyword", ":foo/string");
        builder.retractRef(bEntid, ":foo/ref", stringEntid);
        builder.commit();

        final Expectation expectation2 = new Expectation();
        mentat.query(query).bindEntidReference("?e", bEntid).run(new TupleResultHandler() {
            @Override
            public void handleRow(TupleResult row) {
                assertNull(row);
                expectation2.fulfill();
            }
        });

        synchronized (expectation2) {
            expectation2.wait(1000);
        }
    }
}
