#!/bin/sh

set -eu

if [ -d NDK ]; then
    printf '\033[33;1mStandalone NDK already exists... Delete the NDK folder to make a new one.\033[0m\n\n'
    printf '  $ rm -rf NDK\n'
    exit 0
fi

if [ ! -d "${ANDROID_SDK_ROOT-}" ]; then
    ANDROID_SDK_ROOT=/usr/local/share/android-sdk
fi
if [ ! -d "${ANDROID_HOME-}" ]; then
    ANDROID_HOME="$ANDROID_SDK_ROOT"
fi
if [ ! -d "${ANDROID_NDK_HOME-}" ]; then
    ANDROID_NDK_HOME="$ANDROID_HOME/ndk-bundle"
fi
MAKER="${ANDROID_NDK_HOME}/build/tools/make_standalone_toolchain.py"

if [ -x "$MAKER" ]; then
    echo 'Creating standalone NDK...'
else
    printf '\033[91;1mPlease install Android NDK!\033[0m\n\n'
    printf '  $ sdkmanager ndk-bundle\n\n'
    printf "\033[33;1mnote\033[0m: file \033[34;4m$MAKER\033[0m not found.\n"
    printf 'If you have installed the NDK in non-standard location, please define the \033[1m$ANDROID_NDK_HOME\033[0m variable.\n'
    exit 1
fi

mkdir NDK

create_ndk() {
    echo "($1)..."
    "$MAKER" --api "$2" --arch "$1" --install-dir "NDK/$1"
}

create_ndk arm64 24
create_ndk arm 24
create_ndk x86 24

echo 'Updating cargo-config.toml...'

sed 's|$PWD|'"${PWD}"'|g' cargo-config.toml.template > cargo-config.toml
