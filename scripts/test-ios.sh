#!/bin/bash

# Install iOS targets with Rustup. We need to install all targets as we are building a universal library.
rustup target add aarch64-apple-ios armv7-apple-ios armv7s-apple-ios x86_64-apple-ios i386-apple-ios

# The version of cargo-lipo in Crates.io doesn't contain any support for Cargo manifests in subdirectories and the build fail.
# We therefore need to use a Beta version that contains the right code.
cargo install --git https://github.com/TimNN/cargo-lipo --rev d347567ff337ee169ba46a624229a451dd6f6067

# If we don't list the devices available then, when we come to pick one during the test run, Travis doesn't
# think that there are any devices available and the build fails.
# TODO: See if there is a less time consuming way of doing this.
# instruments -s devices

# Build Mentat as a universal iOS library.
pushd ffi
cargo lipo --release
popd

# Run the iOS SDK tests using xcodebuild.
pushd sdks/swift/Mentat
xcodebuild -configuration Debug -scheme "Mentat Debug" -sdk iphonesimulator test -destination 'platform=iOS Simulator,name=iPhone X,OS=11.4'
popd
