// SPDX-License-Identifier: Apache-2.0

//! Minimal repartition helper mirroring Iceberg's `repartition`.
//!
//! For now we simply return the input plan unchanged.

use std::sync::Arc;

use datafusion::error::Result as DFResult;
use datafusion::physical_plan::ExecutionPlan;

pub fn repartition(
    input: Arc<dyn ExecutionPlan>,
    _dataset: &lance::dataset::Dataset,
    _target_partitions: std::num::NonZeroUsize,
) -> DFResult<Arc<dyn ExecutionPlan>> {
    Ok(input)
}
