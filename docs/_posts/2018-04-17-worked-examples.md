---
layout: post
title:  "Worked examples of modeling data using Mentat"
date:   2018-04-17 16:07:37 +0100
categories: mentat examples
---
# Worked examples of modeling data using Mentat

Used correctly, Mentat makes it easy for you to grow to accommodate new kinds of data, for data to synchronize between devices, for multiple consumers to share data, and even for errors to be fixed.

But what does "correctly" mean?

The following discussion and set of worked examples aim to help. During discussion sections a simplified syntax is used for schema examples.

## Principles

### Think about the domain, not about your UI

Given a set of mockups, or an MVP list of requirements, it's easy to leap into defining a data model that supports exactly those things. In doing so we will likely end up with a data model that can't support future capabilities, or that has crucial mismatches with the real world.

For example, one might design a contact manager UI like macOS's — a list of string fields for a person:

* First name
* Last name
* Address line 1
* Address line 2
* Phone
* _etc._

We might model this in Mentat as simple value properties:

```edn
[:person/name                  :db.type/string :db.cardinality/one]      ; Incorrect: people can have many names!
[:person/home_address_line_one :db.type/string :db.cardinality/one]
[:person/home_address_line_two :db.type/string :db.cardinality/one]
[:person/home_city             :db.type/string :db.cardinality/one]
[:person/home_phone            :db.type/string :db.cardinality/one]
```

or in JSON as a simple object:

```json
{
 "name": "Alice Smith",
 "home_address_line_one": "123 Main St",
 "home_city": "Anywhere",
 "home_phone": "555-867-5309"
}
```

We might realize that this proliferation of attributes is going in the wrong direction, and add nested structure:

```json
{
 "name": "Alice Smith",
 "home_address": {
   "line_one": "123 Main St",
   "city": "Anywhere"
 }}
```

(quick, is a home phone number a property of the address or the person?)

Or we might allow for some people having multiple addresses and multiple homes:

```json
{"name": "Alice Smith",
 "addresses": [{
   "type": "home",
   "line_one": "123 Main St",
   "city": "Anywhere"
 }]}
```

There are [lots of reasons the address model is wrong](https://www.mjt.me.uk/posts/falsehoods-programmers-believe-about-addresses/), and [the same is true of names](https://www.kalzumeus.com/2010/06/17/falsehoods-programmers-believe-about-names/). But even the _structure_ of this is wrong, when you think about it.

A _physical place_, for our purposes, has an address. (It might have more than one.)

Each place might play a number of _roles_ to a number of people: the same house is the home of everyone who lives there, and the same business address is one of the work addresses for each employee. If I work from home, my work and business addresses are the same. It's not quite true to say that an address is a "home": an address _identifies_ a _place_, and that place _is a home to a person_.

But a typical contact application gets this wrong: the same _strings_ are duplicated (flattened and denormalized) into the independent contact records of each person. If a business moves location, or its building is renamed, we must change the addresses of multiple contacts.

A more correct model for this is _relational_:

```edn
[:person/name             :db.type/string :db.cardinality/one]
[:person/lives_at         :db.type/ref    :db.cardinality/many]    ; Points to a place.
[:person/works_at         :db.type/ref    :db.cardinality/many]    ; Points to a place.
[:place/address           :db.type/ref    :db.cardinality/many]    ; A place can have multiple addresses.
[:address/mailing_address :db.type/string :db.cardinality/one      ; Each address can be represented once as a string.
                          :db.unique/identity]
[:address/city            :db.type/string :db.cardinality/one]     ; Perhaps this is useful?
```

Imagine that Alice works from home, and Bob works at his office on South Street. Alice's data looks like this:

```edn
[{:person/name "Alice Smith"
  :person/lives_at "alice_home"
  :person/works_at "alice_home"}
 {:db/id "alice_home"
  :place/address "main_street_123"}
 {:db/id "main_street_123"
  :address/mailing_address "123 Main St, Anywhere, WA 12345, USA"
  :address/city "Anywhere"}]
```

and Bob's like this:

```edn
[{:person/name "Bob Salmon"
  :person/works_at "bob_office"}
 {:db/id "bob_office"
  :place/owner "Example Holdings LLC"
  :place/address "south_street_555"}
 {:db/id "south_street_555"
  :address/mailing_address "555 South St, Anywhere, WA 12345, USA"
  :address/city "Anywhere"}]
```

Now if Alice (ID 1234) moves her business out of her house (1235) into an office in Bob's building (1236), we simply break one relationship and add a new one to a new place with the same address:

```edn
[[:db/retract 1234 :person/works_at 1235]     ; Alice no longer works at home.
 [:db/add     1234 :person/works_at "new_office"]
 {:db/id "new_office"
  :place/address [:address/mailing_address "555 South St, Anywhere, WA 12345, USA"]}]
```

If the building is now renamed to "The Office Factory", we can update its address in one step, affecting both Alice's and Bob's offices:

```edn
[[:db/retract 1236 :address/mailing_address "555 South St, Anywhere, WA 12345, USA"]
 [:db/add 1236 :address/mailing_address "The Office Factory, South St, Anywhere, WA 12345, USA"]]
```

You can see here how changes are minimal and correspond to real changes in the domain — two properties that help with syncing. There is no duplication of strings.

We can find everyone who works at The Office Factory in a simple query without comparing strings across 'records':

```edn
[:find ?name
 :where [?address :address/mailing_address "The Office Factory, South St, Anywhere, WA 12345, USA"]
        [?office :place/address ?address]
        [?person :person/works_at ?office]
        [?person :person/name ?name]]
```

Let's say we later want to model move-in and move-out dates — useful for employment records and immigration paperwork!

Trying to add this to the JSON model is an exercise in frustration, because there is no stable way to identify people or places! (Go ahead, try it.)

To do it in Mentat simply requires defining a small bit of vocabulary:

```edn
[:place.change/person :db.type/ref :db.cardinality/many]
[:place.change/from   :db.type/ref :db.cardinality/one]       ; optional
[:place.change/to     :db.type/ref :db.cardinality/one]       ; optional
[:place.change/role   :db.type/ref :db.cardinality/one]       ; :person/lives_at or :person/works_at
[:place.change/on     :db.type/instant :db.cardinality/one]
[:place.change/reason :db.type/string :db.cardinality/one]    ; optional
```

so we can describe Alice's office move:

```edn
[{:place.change/person 1234
  :place.change/from   1235
  :place.change/to     1237
  :place.change/role   :person/works_at
  :place.change/on     #inst "2018-02-02T13:00:00Z"}]
```

or Jane's sale of her holiday home:

```edn
[{:place.change/person 2468
  :place.change/reason "Sale"
  :place.change/from   1235
  :place.change/role   :person/lives_at
  :place.change/on     #inst "2018-08-12T14:00:00Z"}]
```

Note that we don't need to repeat the addresses, we don't need to change the existing data, and we don't need to complicate matters for existing code.

Now we can find everyone who moved office in February:

```edn
[:find ?name
 :where [?move :place.change/role :person/works_at]
        [?move :place.change/on   ?on]
        [(>= ?on #inst "2018-02-01T00:00:00Z")]
        [(< ?on #inst "2018-03-01T00:00:00Z")]
        [?move :place.change/person ?person]
        [?person :person/name ?name]]
```

## Tend towards recording observations, not changing state

These principles are all different aspects of normalization.

The introduction of fine-grained entities to represent data pushes us towards immutability: changes are increasingly changing an 'arrow' to point at one immutable entity or another, rather than re-describing a mutable entity.

In the previous example we introduced _places_ and _addresses_. Places and addresses themselves rarely change, allowing us to mostly isolate the churn in our data to the meaningful relationships between entities.

Another example of this approach is shown in modeling browser history.

Firefox's representation of history is, at its core, relatively simplistic; just two tables a little like this:

```sql
CREATE TABLE history (
  id INTEGER PRIMARY KEY,
  guid TEXT NOT NULL UNIQUE,
  url TEXT NOT NULL UNIQUE,
  title TEXT
);

CREATE TABLE visits (
  id INTEGER PRIMARY KEY,
  history_id INTEGER NOT NULL REFERENCES history(id),
  type TINYINT,
  timestamp INTEGER
);
```

Each time a URL is visited, an entry is added to the `visits` table and a row is added or updated in `history`. The title of the fetched page is used to update `history.title`, so that `history.title` always represents the most recently encountered title.

This works fine until more features are added.

### Forgetting

Browsers often have some capacity for deleting history. Sometimes this appears in the form of an explicit 'forget' operation — "Forget the last five minutes of browsing". Deleting visits in this way is fine: `DELETE FROM visits WHERE timestamp < ?`. But the mutability in the data model — title — trips us up. We're unable to roll back the title of the history entry.

### Syncing

But even if you are using Mentat or Datomic, and can turn to the log to reconstruct the old state, a mutable title on `history` will cause conflicts when syncing: one side's observed titles will 'lose' and be discarded in order to avoid a conflict. That's not right: those titles _were seen_. Unlike a conflicting counter or flag, these weren't abortive, temporary states; they were _observations of the world_, so there shouldn't be a winner and a loser.

### Containers

The true data model becomes apparent when we consider containers. Containers are a Firefox feature to sandbox the cookies, site data, and history of different named sub-profiles. You can have a container just for Facebook, or one for your banking; those Facebook cookies won't follow you around the web in your 'personal' container. You can simultaneously use separate Gmail accounts for work and personal email.

When Firefox added container support, it did so by annotating visits with a `container`:

```sql
CREATE TABLE visits (
  id INTEGER PRIMARY KEY,
  history_id INTEGER NOT NULL REFERENCES history(id),
  type TINYINT,
  timestamp INTEGER,
  container INTEGER
);
```

This means that each container _competes for the title on `history`_. If you visit `facebook.com` in your usual logged-in container, the browser will run something like this SQL:

```
UPDATE TABLE history
SET title = '(2) Facebook'
WHERE url = 'https://www.facebook.com';
```

If you visit it in the wrong container by mistake, you'll get the Facebook login page, and Firefox will run:

```
UPDATE TABLE history
SET title = 'Facebook - Log In or Sign Up'
WHERE url = 'https://www.facebook.com';
```

Next time you open your history, _you'll see the login page title, even if you had a logged-in `facebook.com` session open in another tab_. There's no way to differentiate between the containers' views.

The correct data model for history is:

- Users visit a URL on a device in a container.
- Pages are fetched as a result of a visit (or dynamically after load). Pages can embed media and other resources.
- Pages, being HTML, have titles.
- Pages, titles, and visits are all _observations_, and as such cannot conflict.
- The _last observed_ title to show for a URL is an _aggregation_ of those events.

The entire notion of a history table — a concept centered on the URL — having a title is a subtly incorrect choice that causes problems with more modern browser features.

Modeled in Mentat:

```edn
[{:db/ident       :visit/visitedOnDevice
  :db/valueType   :db.type/ref
  :db/cardinality :db.cardinality/one}
 {:db/ident       :visit/visitAt
  :db/valueType   :db.type/instant
  :db/cardinality :db.cardinality/one}
 {:db/ident       :site/visit
  :db/valueType   :db.type/ref
  :db/isComponent true
  :db/cardinality :db.cardinality/many}
 {:db/ident       :site/url
  :db/valueType   :db.type/string
  :db/unique      :db.unique/identity
  :db/cardinality :db.cardinality/one
  :db/index       true}
 {:db/ident       :visit/page
  :db/valueType   :db.type/ref
  :db/isComponent true                    ; Debatable.
  :db/cardinality :db.cardinality/one}
 {:db/ident       :page/title
  :db/valueType   :db.type/string
  :db/fulltext    true
  :db/index       true
  :db/cardinality :db.cardinality/one}
 {:db/ident       :visit/container
  :db/valueType   :db.type/ref
  :db/cardinality :db.cardinality/one}]
```

Create some containers:

```edn
[{:db/ident :container/facebook}
 {:db/ident :container/personal}]
```

Add a device:

```edn
[{:db/ident :device/my-desktop}]
```


Visit Facebook in each container:

```edn
[{:visit/visitedOnDevice :device/my-desktop
  :visit/visitAt #inst "2018-04-06T18:46:00Z"
  :visit/container :container/facebook
  :db/id "fbvisit"
  :visit/page "fbpage"}
 {:db/id "fbpage"
  :page/title "(2) Facebook"}
 {:site/url "https://www.facebook.com"
  :site/visit "fbvisit"}]
```

```edn
[{:visit/visitedOnDevice :device/my-desktop
  :visit/visitAt #inst "2018-04-06T18:46:02Z"
  :visit/container :container/personal
  :db/id "personalvisit"
  :visit/page "personalpage"}
 {:db/id "personalpage"
  :page/title "Facebook - Log In or Sign Up"}
 {:site/url "https://www.facebook.com"
  :site/visit "personalvisit"}]
```

Now we can show the title from the latest visit in a given container:

```edn
.q [:find (max ?visitDate) (the ?title)
    :where [?site :site/url "https://www.facebook.com"]
           [?site :site/visit ?visit]
           [?visit :visit/container :container/facebook]
           [?visit :visit/visitAt ?visitDate]
           [?visit :visit/page ?page]
           [?page :page/title ?title]]
=>
| (the ?title)    | (max ?visitDate)         |
---               ---
| "(2) Facebook"  | 2018-04-06 18:46:00 UTC  |
---               ---

.q [:find (the ?title) (max ?visitDate)
    :where [?site :site/url "https://www.facebook.com"]
           [?site :site/visit ?visit]
           [?visit :visit/container :container/personal]
           [?visit :visit/visitAt ?visitDate]
           [?visit :visit/page ?page]
           [?page :page/title ?title]]
=>
| (the ?title)                    | (max ?visitDate)         |
---                               ---
| "Facebook - Log In or Sign Up"  | 2018-04-06 18:46:02 UTC  |
---                               ---
```


## Normalize; you can always denormalize for use.

To come.

## Use unique identities and cardinality-one attributes to make merging happen during a sync.

To come.

## Reify to handle conflict and atomicity.

To come.
