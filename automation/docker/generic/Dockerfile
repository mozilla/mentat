# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

FROM mozillamobile/android-components:1.4

MAINTAINER Nick Alexander "nalexander@mozilla.com"

#----------------------------------------------------------------------------------------------------------------------
#-- Configuration -----------------------------------------------------------------------------------------------------
#----------------------------------------------------------------------------------------------------------------------

ENV ANDROID_NDK_VERSION "r17b"

#----------------------------------------------------------------------------------------------------------------------
#-- System ------------------------------------------------------------------------------------------------------------
#----------------------------------------------------------------------------------------------------------------------

RUN apt-get update -qq

#----------------------------------------------------------------------------------------------------------------------
#-- Android NDK (Android SDK comes from base `android-components` image) ----------------------------------------------
#----------------------------------------------------------------------------------------------------------------------

RUN mkdir -p /build
WORKDIR /build

ENV ANDROID_NDK_HOME /build/android-ndk

RUN curl -L https://dl.google.com/android/repository/android-ndk-${ANDROID_NDK_VERSION}-linux-x86_64.zip > ndk.zip \
	&& unzip ndk.zip -d /build \
	&& rm ndk.zip \
  && mv /build/android-ndk-${ANDROID_NDK_VERSION} ${ANDROID_NDK_HOME}

ENV ANDROID_NDK_TOOLCHAIN_DIR /build/android-ndk-toolchain
ENV ANDROID_NDK_API_VERSION 26

RUN set -eux; \
    python "$ANDROID_NDK_HOME/build/tools/make_standalone_toolchain.py" --arch="arm"   --api="$ANDROID_NDK_API_VERSION" --install-dir="$ANDROID_NDK_TOOLCHAIN_DIR/arm-$ANDROID_NDK_API_VERSION" --force; \
    python "$ANDROID_NDK_HOME/build/tools/make_standalone_toolchain.py" --arch="arm64" --api="$ANDROID_NDK_API_VERSION" --install-dir="$ANDROID_NDK_TOOLCHAIN_DIR/arm64-$ANDROID_NDK_API_VERSION" --force; \
    python "$ANDROID_NDK_HOME/build/tools/make_standalone_toolchain.py" --arch="x86"   --api="$ANDROID_NDK_API_VERSION" --install-dir="$ANDROID_NDK_TOOLCHAIN_DIR/x86-$ANDROID_NDK_API_VERSION" --force

#----------------------------------------------------------------------------------------------------------------------
#-- Rust (cribbed from https://github.com/rust-lang-nursery/docker-rust/blob/ced83778ec6fea7f63091a484946f95eac0ee611/1.27.1/stretch/Dockerfile)
#-- Rust is after the Android NDK since Rust rolls forward more frequently.  Both stable and beta for advanced consumers.
#----------------------------------------------------------------------------------------------------------------------

ENV RUSTUP_HOME=/usr/local/rustup \
    CARGO_HOME=/usr/local/cargo \
    PATH=/usr/local/cargo/bin:$PATH \
    RUST_VERSION=1.27.1

RUN set -eux; \
    rustArch='x86_64-unknown-linux-gnu'; rustupSha256='4d382e77fd6760282912d2d9beec5e260ec919efd3cb9bdb64fe1207e84b9d91'; \
    url="https://static.rust-lang.org/rustup/archive/1.12.0/${rustArch}/rustup-init"; \
    wget "$url"; \
    echo "${rustupSha256} *rustup-init" | sha256sum -c -; \
    chmod +x rustup-init; \
    ./rustup-init -y --no-modify-path --default-toolchain $RUST_VERSION; \
    rm rustup-init; \
    chmod -R a+w $RUSTUP_HOME $CARGO_HOME; \
    rustup --version; \
    cargo --version; \
    rustc --version; \
    rustup target add i686-linux-android; \
    rustup target add armv7-linux-androideabi; \
    rustup target add aarch64-linux-android

RUN set -eux; \
    rustup install beta; \
    rustup target add --toolchain beta i686-linux-android; \
    rustup target add --toolchain beta armv7-linux-androideabi; \
    rustup target add --toolchain beta aarch64-linux-android

#----------------------------------------------------------------------------------------------------------------------
# -- Cleanup ----------------------------------------------------------------------------------------------------------
#----------------------------------------------------------------------------------------------------------------------

WORKDIR /build

RUN apt-get clean
