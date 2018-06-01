# Android Mentat SDK

Exposes Mentat functionality to Android applications via the foreign function interface provided by `mentat_ffi`.

Note that `libmentat_ffi.so` binaries are symlinked. Use [the cross-compilation script](https://github.com/mozilla/mentat/tree/master/scripts/android_build.sh) to build them. Windows users: apologies if you're on FAT; symlinks are supported on NTFS (on Vista and later).

- See [sample projects](https://github.com/mozilla/mentat/tree/master/sdks/android/Mentat/samples) for how one might use the SDK.
- See [documentation](https://github.com/mozilla/mentat/tree/master/ffi/README.md) within the `mentat_ffi` crate for development flow.
