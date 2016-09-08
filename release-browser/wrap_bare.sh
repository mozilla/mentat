#!/bin/sh

set -e

(cat release-browser/wrapper.prefix; cat release-browser/datomish.bare.js; cat release-browser/wrapper.suffix) > release-browser/datomish.js

echo "Packed release-browser/datomish.js"
