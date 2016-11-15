#!/bin/sh

set -e

(cat release-browser/wrapper.prefix; cat target/release-browser/datomish.bare.js; cat release-browser/wrapper.suffix) > target/release-browser/datomish.js

echo "Packed target/release-browser/datomish.js"
