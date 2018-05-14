# This will eventually become a complete build script, not just for Android
cargo build -p mentat_ffi --target i686-linux-android --release
cargo build -p mentat_ffi --target armv7-linux-androideabi --release
cargo build -p mentat_ffi --target aarch64-linux-android --release
