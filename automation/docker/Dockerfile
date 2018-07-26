# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

FROM mozillamobile/android-components:1.4

MAINTAINER Nick Alexander "nalexander@mozilla.com"

#----------------------------------------------------------------------------------------------------------------------
#-- Configuration -----------------------------------------------------------------------------------------------------
#----------------------------------------------------------------------------------------------------------------------

ENV ANDROID_NDK_VERSION "r17b"
ENV PROJECT_REPOSITORY "https://github.com/mozilla/mentat.git"

#----------------------------------------------------------------------------------------------------------------------
#-- System ------------------------------------------------------------------------------------------------------------
#----------------------------------------------------------------------------------------------------------------------

RUN apt-get update -qq

#----------------------------------------------------------------------------------------------------------------------
#-- Android NDK (Android SDK comes from base `android-components` image) ----------------------------------------------
#----------------------------------------------------------------------------------------------------------------------

RUN mkdir -p /build
WORKDIR /build

# ENV ANDROID_HOME /build/android-sdk
# ENV ANDROID_SDK_HOME /build/android-sdk
ENV ANDROID_NDK_HOME /build/android-ndk
# ENV PATH ${PATH}:${ANDROID_NDK_HOME}/tools:${ANDROID_SDK_HOME}/tools/bin:${ANDROID_SDK_HOME}/platform-tools:/opt/tools:${ANDROID_SDK_HOME}/build-tools/${ANDROID_BUILD_TOOLS}

RUN curl -L https://dl.google.com/android/repository/android-ndk-${ANDROID_NDK_VERSION}-linux-x86_64.zip > ndk.zip \
	&& unzip ndk.zip -d /build \
	&& rm ndk.zip \
  && mv /build/android-ndk-${ANDROID_NDK_VERSION} ${ANDROID_NDK_HOME}

#----------------------------------------------------------------------------------------------------------------------
#-- Rust (cribbed from https://github.com/rust-lang-nursery/docker-rust/blob/ced83778ec6fea7f63091a484946f95eac0ee611/1.27.1/stretch/Dockerfile)
#-- Rust after the Android NDK since Rust rolls forward more frequently.
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
    rustup target add arm-linux-androideabi; \
    rustup target add aarch64-linux-android

#----------------------------------------------------------------------------------------------------------------------
#-- Project -----------------------------------------------------------------------------------------------------------
#----------------------------------------------------------------------------------------------------------------------

RUN git clone $PROJECT_REPOSITORY

WORKDIR /build/mentat

# Temporary.
RUN git fetch origin build-android-sdk && git checkout origin/build-android-sdk &&    git show-ref HEAD

# Populate dependencies.
RUN ./sdks/android/Mentat/gradlew --no-daemon -p sdks/android/Mentat tasks

# Cache toolchains.
RUN ./sdks/android/Mentat/gradlew --no-daemon -p sdks/android/Mentat generateToolchains

# Build Rust.
RUN ./sdks/android/Mentat/gradlew --no-daemon -p sdks/android/Mentat cargoBuild

# Actually build.  In the future, we might also test and lint (to cache additional dependencies).
RUN ./sdks/android/Mentat/gradlew --no-daemon -p sdks/android/Mentat assemble

# Drop built Rust artifacts.
RUN cargo clean

#----------------------------------------------------------------------------------------------------------------------
# -- Cleanup ----------------------------------------------------------------------------------------------------------
#----------------------------------------------------------------------------------------------------------------------

RUN apt-get clean
