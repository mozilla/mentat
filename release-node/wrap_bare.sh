#!/bin/sh

set -e

(cat release-node/wrapper.prefix && cat release-node/datomish.bare.js && cat release-node/wrapper.suffix) > release-node/datomish.js

echo "Packed release-node/datomish.js"
