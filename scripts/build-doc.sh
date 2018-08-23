#!/bin/bash

# if [[ "$TRAVIS_PULL_REQUEST" = "false" && "$TRAVIS_BRANCH" == "master" ]]; then
cargo doc --all --no-deps --release && cp -r target/doc/* docs/apis/latest/rust
gem install jazzy
jazzy --source-directory sdks/swift/Mentat/ -o docs/apis/latest/swift
# fi
