cd ffi
cargo lipo --release
cd ..
cp target/universal/release/libmentat_ffi.a sdks/swift/Mentat/External-Dependencies/
