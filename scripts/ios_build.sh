cd ffi
cargo lipo --release
cd ..
mkdir -p sdks/swift/Mentat/External-Dependencies/
ls -la target/release
cp target/universal/release/libmentat_ffi.a sdks/swift/Mentat/External-Dependencies/
