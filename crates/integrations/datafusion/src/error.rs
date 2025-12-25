// SPDX-License-Identifier: Apache-2.0

use datafusion::error::{DataFusionError, Result as DFResult};

/// Convert a Lance error into a DataFusion error.
///
/// This mirrors the helper found in `iceberg-datafusion` and keeps
/// all Lance-specific error formatting in a single place.
pub fn to_datafusion_error<E: std::fmt::Display>(err: E) -> DataFusionError {
    DataFusionError::Execution(err.to_string())
}

/// Convenience helper for wrapping fallible operations.
pub fn df_result<T, E: std::fmt::Display>(res: Result<T, E>) -> DFResult<T> {
    res.map_err(to_datafusion_error)
}
