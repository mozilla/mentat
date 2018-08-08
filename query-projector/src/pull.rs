// Copyright 2018 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use std::collections::{
    BTreeMap,
    BTreeSet,
};

use core_traits::{
    Binding,
    Entid,
    StructuredMap,
    TypedValue,
};

use mentat_core::{
    Schema,
    ValueRc,
};

use edn::query::{
    PullAttributeSpec,
};

use mentat_query_pull::{
    Puller,
};

use query_projector_traits::errors::Result;

use super::{
    Index,
    rusqlite,
};

#[derive(Clone, Debug)]
pub(crate) struct PullOperation(pub(crate) Vec<PullAttributeSpec>);

#[derive(Clone, Copy, Debug)]
pub(crate) struct PullIndices {
    pub(crate) sql_index: Index,                   // SQLite column index.
    pub(crate) output_index: usize,
}

impl PullIndices {
    fn zero() -> PullIndices {
        PullIndices {
            sql_index: 0,
            output_index: 0,
        }
    }
}

#[derive(Debug)]
pub(crate) struct PullTemplate {
    pub(crate) indices: PullIndices,
    pub(crate) op: PullOperation,
}

pub(crate) struct PullConsumer<'schema> {
    indices: PullIndices,
    schema: &'schema Schema,
    puller: Puller,
    entities: BTreeSet<Entid>,
    results: BTreeMap<Entid, ValueRc<StructuredMap>>,
}

impl<'schema> PullConsumer<'schema> {
    pub(crate) fn for_puller(puller: Puller, schema: &'schema Schema, indices: PullIndices) -> PullConsumer<'schema> {
        PullConsumer {
            indices: indices,
            schema: schema,
            puller: puller,
            entities: Default::default(),
            results: Default::default(),
        }
    }

    pub(crate) fn for_template(schema: &'schema Schema, template: &PullTemplate) -> Result<PullConsumer<'schema>> {
        let puller = Puller::prepare(schema, template.op.0.clone())?;
        Ok(PullConsumer::for_puller(puller, schema, template.indices))
    }

    pub(crate) fn for_operation(schema: &'schema Schema, operation: &PullOperation) -> Result<PullConsumer<'schema>> {
        let puller = Puller::prepare(schema, operation.0.clone())?;
        Ok(PullConsumer::for_puller(puller, schema, PullIndices::zero()))
    }

    pub(crate) fn collect_entity<'a, 'stmt>(&mut self, row: &rusqlite::Row<'a, 'stmt>) -> Entid {
        let entity = row.get(self.indices.sql_index);
        self.entities.insert(entity);
        entity
    }

    pub(crate) fn pull(&mut self, sqlite: &rusqlite::Connection) -> Result<()> {
        let entities: Vec<Entid> = self.entities.iter().cloned().collect();
        self.results = self.puller.pull(self.schema, sqlite, entities)?;
        Ok(())
    }

    pub(crate) fn expand(&self, bindings: &mut [Binding]) {
        if let Binding::Scalar(TypedValue::Ref(id)) = bindings[self.indices.output_index] {
            if let Some(pulled) = self.results.get(&id).cloned() {
                bindings[self.indices.output_index] = Binding::Map(pulled);
            } else {
                bindings[self.indices.output_index] = Binding::Map(ValueRc::new(Default::default()));
            }
        }
    }

    // TODO: do we need to include empty maps for entities that didn't match any pull?
    pub(crate) fn into_coll_results(self) -> Vec<Binding> {
        self.results.values().cloned().map(|vrc| Binding::Map(vrc)).collect()
    }
}
