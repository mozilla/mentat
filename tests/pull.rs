// Copyright 2018 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

#[macro_use]
extern crate mentat;
extern crate mentat_core;
extern crate mentat_query_pull;

use std::collections::{
    BTreeMap,
    BTreeSet,
};

use std::path::{
    Path,
    PathBuf,
};

// TODO: re-export from Mentat.
use mentat_core::{
    Binding,
    StructuredMap,
    ValueRc,
};

use mentat::{
    Entid,
    HasSchema,
    IntoResult,
    Keyword,
    Pullable,
    Queryable,
    QueryInputs,
    Store,
    RelResult,
    TypedValue,
};

fn fixture_path(rest: &str) -> PathBuf {
    let fixtures = Path::new("fixtures/");
    fixtures.join(Path::new(rest))
}

#[test]
fn test_simple_pull() {
    let beacon;
    let capitol;
    let beacon_district;
    let capitol_district;

    let mut store = Store::open("").expect("opened");
    {
        let mut in_progress = store.begin_transaction().expect("began");
        in_progress.import(fixture_path("cities.schema")).expect("transacted schema");
        let report = in_progress.import(fixture_path("all_seattle.edn")).expect("transacted data");

        // These tempid strings come out of all_seattle.edn.
        beacon = *report.tempids.get(&"a17592186045471".to_string()).expect("beacon");
        beacon_district = *report.tempids.get(&"a17592186045450".to_string()).expect("beacon district");

        capitol = *report.tempids.get(&"a17592186045439".to_string()).expect("capitol");
        capitol_district = *report.tempids.get(&"a17592186045438".to_string()).expect("capitol district");

        in_progress.commit().expect("committed");
    }

    let schema = store.conn().current_schema();
    let district = schema.get_entid(&kw!(:neighborhood/district)).expect("district").0;
    let name = schema.get_entid(&kw!(:neighborhood/name)).expect("name").0;
    let attrs: BTreeSet<Entid> = vec![district, name].into_iter().collect();

    // Find Beacon Hill and Capitol Hill via query.
    let query = r#"[:find [?hood ...]
                    :where
                    (or [?hood :neighborhood/name "Beacon Hill"]
                        [?hood :neighborhood/name "Capitol Hill"])]"#;
    let hoods: BTreeSet<Entid> = store.q_once(query, None)
                                      .into_coll_result()
                                      .expect("hoods")
                                      .into_iter()
                                      .map(|b| {
                                          b.val().and_then(|tv| tv.into_entid()).expect("scalar")
                                      })
                                      .collect();

    println!("Hoods: {:?}", hoods);

    // The query and the tempids agree.
    assert_eq!(hoods, vec![beacon, capitol].into_iter().collect());

    // Pull attributes of those two neighborhoods.
    let pulled = store.begin_read().expect("read")
                      .pull_attributes_for_entities(hoods, attrs)
                      .expect("pulled");

    // Here's what we expect:
    let c: StructuredMap = vec![
        (kw!(:neighborhood/name), "Capitol Hill".into()),
        (kw!(:neighborhood/district), TypedValue::Ref(capitol_district)),
    ].into();
    let b: StructuredMap = vec![
        (kw!(:neighborhood/name), "Beacon Hill".into()),
        (kw!(:neighborhood/district), TypedValue::Ref(beacon_district)),
    ].into();

    let expected: BTreeMap<Entid, ValueRc<StructuredMap>>;
    expected = vec![(capitol, c.into()), (beacon, b.into())].into_iter().collect();

    assert_eq!(pulled, expected);

    // Now test pull inside the query itself.
    let query = r#"[:find ?hood (pull ?district [:district/name :district/region])
                    :where
                    (or [?hood :neighborhood/name "Beacon Hill"]
                        [?hood :neighborhood/name "Capitol Hill"])
                    [?hood :neighborhood/district ?district]
                    :order ?hood]"#;
    let results: RelResult<Binding> = store.begin_read().expect("read")
                                           .q_once(query, None)
                                           .into_rel_result()
                                           .expect("results");

    let beacon_district: Vec<(Keyword, TypedValue)> = vec![
        (kw!(:district/name), "Greater Duwamish".into()),
        (kw!(:district/region), schema.get_entid(&Keyword::namespaced("region", "se")).unwrap().into())
    ];
    let beacon_district: StructuredMap = beacon_district.into();
    let capitol_district: Vec<(Keyword, TypedValue)> = vec![
        (kw!(:district/name), "East".into()),
        (kw!(:district/region), schema.get_entid(&Keyword::namespaced("region", "e")).unwrap().into())
    ];
    let capitol_district: StructuredMap = capitol_district.into();

    let expected = RelResult {
                       width: 2,
                       values: vec![
                           TypedValue::Ref(capitol).into(), capitol_district.into(),
                           TypedValue::Ref(beacon).into(), beacon_district.into(),
                       ].into(),
                   };
    assert_eq!(results, expected.clone());

    // We can also prepare the query.
    let reader = store.begin_read().expect("read");
    let mut prepared = reader.q_prepare(query, None).expect("prepared");

    assert_eq!(prepared.run(None)
                       .into_rel_result()
                       .expect("results"),
               expected);

    // Execute a scalar query where the body is constant.
    // TODO: we shouldn't require `:where`; that makes this non-constant!
    let query = r#"[:find (pull ?hood [:neighborhood/name]) . :in ?hood
                    :where [?hood :neighborhood/district _]]"#;
    let result = reader.q_once(query, QueryInputs::with_value_sequence(vec![(var!(?hood), TypedValue::Ref(beacon))]))
                       .into_scalar_result()
                       .expect("success")
                       .expect("result");

    let expected: StructuredMap = vec![(kw!(:neighborhood/name), TypedValue::from("Beacon Hill"))].into();
    assert_eq!(result, expected.into());

    // Collect the names and regions of all districts.
    let query = r#"[:find [(pull ?district [:district/name :district/region]) ...]
                    :where [_ :neighborhood/district ?district]]"#;
    let results = reader.q_once(query, None)
                        .into_coll_result()
                        .expect("result");

    let expected: Vec<StructuredMap> = vec![
        vec![(kw!(:district/name), TypedValue::from("East")),
            (kw!(:district/region), TypedValue::Ref(65556))].into(),
        vec![(kw!(:district/name), TypedValue::from("Southwest")),
            (kw!(:district/region), TypedValue::Ref(65559))].into(),
        vec![(kw!(:district/name), TypedValue::from("Downtown")),
            (kw!(:district/region), TypedValue::Ref(65560))].into(),
        vec![(kw!(:district/name), TypedValue::from("Greater Duwamish")),
            (kw!(:district/region), TypedValue::Ref(65557))].into(),
        vec![(kw!(:district/name), TypedValue::from("Ballard")),
            (kw!(:district/region), TypedValue::Ref(65561))].into(),
        vec![(kw!(:district/name), TypedValue::from("Northeast")),
            (kw!(:district/region), TypedValue::Ref(65555))].into(),
        vec![(kw!(:district/name), TypedValue::from("Southeast")),
            (kw!(:district/region), TypedValue::Ref(65557))].into(),
        vec![(kw!(:district/name), TypedValue::from("Northwest")),
            (kw!(:district/region), TypedValue::Ref(65561))].into(),
        vec![(kw!(:district/name), TypedValue::from("Central")),
            (kw!(:district/region), TypedValue::Ref(65556))].into(),
        vec![(kw!(:district/name), TypedValue::from("Delridge")),
            (kw!(:district/region), TypedValue::Ref(65559))].into(),
        vec![(kw!(:district/name), TypedValue::from("Lake Union")),
            (kw!(:district/region), TypedValue::Ref(65560))].into(),
        vec![(kw!(:district/name), TypedValue::from("Magnolia/Queen Anne")),
            (kw!(:district/region), TypedValue::Ref(65560))].into(),
        vec![(kw!(:district/name), TypedValue::from("North")),
            (kw!(:district/region), TypedValue::Ref(65555))].into(),
        ];

    let expected: Vec<Binding> = expected.into_iter().map(|m| m.into()).collect();
    assert_eq!(results, expected);

}

// TEST:
// - Constant query bodies in pull.
// - Values that are present in the cache (=> constant pull, too).
// - Pull for each kind of find spec.
// - That the keys in each map are ValueRc::ptr_eq.
// - Entity presence/absence when the pull expressions don't match anything.
// - Aliases. (No parser support yet.)
