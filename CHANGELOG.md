# 0.11 (2018-07-31)

* sdks/android compiled against:
  * Kotlin standard library 1.2.41

* **sdks/android**: `Mentat()` constructor replaced with `open` factory method.

* [Commits](https://github.com/mozilla/mentat/compare/v0.10.0...v0.11.0)

# 0.10 (2018-07-26)

* sdks/android compiled against:
  * Kotlin standard library 1.2.41

* **API changes**:
  * `store_open{_encrypted}` now accepts an error parameter; corresponding constructors changed to be factory functions.

* [Commits](https://github.com/mozilla/mentat/compare/v0.9.0...v0.10.0)

# 0.9 (2018-07-25)

* sdks/android compiled against:
  * Kotlin standard library 1.2.41

* **API changes**:
  * Mentat partitions now enforce their integrity, denying entids that aren't already known.

* **sdks/android**: First version published to nalexander's personal bintray repository.
* Various bugfixes and refactorings (see commits below for details)
* [Commits](https://github.com/mozilla/mentat/compare/v0.8.1...v0.9.0)
