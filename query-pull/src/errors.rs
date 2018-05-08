// Copyright 2018 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use mentat_core::{
    Entid,
};

error_chain! {
    types {
        Error, ErrorKind, ResultExt, Result;
    }

    errors {
        UnnamedAttribute(id: Entid) {
            description("unnamed attribute")
            display("attribute {:?} has no name", id)
        }

        RepeatedDbId {
            description(":db/id repeated")
            display(":db/id repeated")
        }
    }

    links {
        DbError(::mentat_db::Error, ::mentat_db::ErrorKind);
    }
}
