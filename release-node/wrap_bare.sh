#!/bin/sh

set -e

(cat release-node/wrapper.prefix && cat target/release-node/datomish.bare.js && cat release-node/wrapper.suffix) > target/release-node/datomish.js

echo "Packed target/release-node/datomish.js"
