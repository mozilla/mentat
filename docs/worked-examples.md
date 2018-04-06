# Worked examples of modeling data using Mentat

Used correctly, Mentat makes it easy for you to grow to accommodate new kinds of data, for data to synchronize between devices, for multiple consumers to share data, and even for errors to be fixed.

But what does "correctly" mean?

The following discussion and set of worked examples aim to help. During discussion sections a simplified syntax is used for schema examples.

## Principles

### Think about the domain, not about your UI

Given a set of mockups, or an MVP list of requirements, it's easy to leap into defining a data model that supports exactly those things. In doing so we will likely end up with a data model that can't support future capabilities, or that has crucial mismatches with the real world.

For example, one might design a contact manager UI like Apple's — a list of string fields for a person:

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

But a typical contact application gets this wrong: the same _strings_ are duplicated (flattened and denormalized) into the independent contact records of each person. If a business moves location (or its building is renamed) we must change the addresses of multiple contacts.

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

You can see here how changes are minimal and correspond to real changes in the domain, which helps with syncing. There is no duplication of strings.

We can find everyone who works at The Office Factory in a simple query without comparing strings across 'records':

```edn
[:find ?name
 :where [?address :address/mailing_address "The Office Factory, South St, Anywhere, WA 12345, USA"]
        [?office :place/address ?address]
        [?person :person/works_at ?office]
        [?person :person/name ?name]]
```

Let's say we later want to model move-in and move-out dates — useful for employment records and immigration paperwork!

Trying to add this to the JSON model is an exercise in frustration: there's no stable way to identify people or places! (Go ahead, try it.)

But to do it in Mentat simply requires defining a small bit of vocabulary:

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

## Normalize; you can always denormalize for use.
## Tend towards recording events, not changing state.
## Use unique identities and cardinality-one attributes to make merging happen during a sync.
## Reify to handle conflict and atomicity.
