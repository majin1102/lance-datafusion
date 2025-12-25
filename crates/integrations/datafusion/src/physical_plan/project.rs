// SPDX-License-Identifier: Apache-2.0

//! Minimal projection helper mirroring Iceberg's `project_with_partition`.
//!
//! For now this is a no-op wrapper that simply returns the input plan.

use std::sync::Arc;

use datafusion::error::Result as DFResult;
use datafusion::physical_plan::ExecutionPlan;

pub fn project_with_partition(
    input: Arc<dyn ExecutionPlan>,
    _table: &lance::dataset::Dataset,
) -> DFResult<Arc<dyn ExecutionPlan>> {
    Ok(input)
}
