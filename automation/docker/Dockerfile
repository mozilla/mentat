# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

FROM mozillamobile/rust-component:buildtools-27.0.3-ndk-r17b-ndk-version-26-rust-stable-rust-beta

MAINTAINER Nick Alexander "nalexander@mozilla.com"

#----------------------------------------------------------------------------------------------------------------------
#-- Project -----------------------------------------------------------------------------------------------------------
#----------------------------------------------------------------------------------------------------------------------

ENV PROJECT_REPOSITORY "https://github.com/mozilla/mentat.git"

RUN git clone $PROJECT_REPOSITORY

WORKDIR /build/mentat

# Temporary.
RUN git fetch origin master && git checkout origin/generic-automation-images && git show-ref HEAD

# Populate dependencies.
RUN ./sdks/android/Mentat/gradlew --no-daemon -p sdks/android/Mentat tasks

# Build Rust.
RUN ./sdks/android/Mentat/gradlew --no-daemon -p sdks/android/Mentat cargoBuild

# Actually build.  In the future, we might also lint (to cache additional dependencies).
RUN ./sdks/android/Mentat/gradlew --no-daemon -p sdks/android/Mentat assemble test

#----------------------------------------------------------------------------------------------------------------------
# -- Cleanup ----------------------------------------------------------------------------------------------------------
#----------------------------------------------------------------------------------------------------------------------

# Drop built Rust artifacts.
RUN cargo clean
