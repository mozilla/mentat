#!/bin/bash

cargo test --verbose --all
cargo test --features edn/serde_support --verbose --all
# We can't pick individual features out with `cargo test --all` (At the time of this writing, this
# works but does the wrong thing because of a bug in cargo, but its fix will be to disallow doing
# this all-together, see https://github.com/rust-lang/cargo/issues/5364 for more information). To
# work around this, we run individual tests for each subcrate individually.
for manifest in $(find . -type f -name Cargo.toml); do
    cargo test --manifest-path $manifest --verbose --no-default-features --features sqlcipher
done
