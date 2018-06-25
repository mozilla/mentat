cd ffi
cargo lipo --release
cd ..
mkdir -p sdks/swift/Mentat/External-Dependencies/
cp target/universal/release/libmentat_ffi.a sdks/swift/Mentat/External-Dependencies/
