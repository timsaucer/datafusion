use std::sync::Arc;

use abi_stable::std_types::RVec;
use abi_stable::StableAbi;
use datafusion_physical_expr::Partitioning;
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;

use crate::physical_expr::FFI_PhysicalExpr;

#[repr(C)]
#[derive(Debug, StableAbi)]
#[allow(non_camel_case_types)]
pub enum FFI_Partitioning {
    /// Allocate batches using a round-robin algorithm and the specified number of partitions
    RoundRobinBatch(usize),

    /// Allocate rows based on a hash of one of more expressions and the specified number of
    /// partitions
    Hash(RVec<FFI_PhysicalExpr>, usize),

    /// Unknown partitioning scheme with a known number of partitions
    UnknownPartitioning(usize),
}

impl From<&Partitioning> for FFI_Partitioning {
    fn from(value: &Partitioning) -> Self {
        match value {
            Partitioning::RoundRobinBatch(size) => Self::RoundRobinBatch(*size),
            Partitioning::Hash(exprs, size) => {
                let exprs = exprs
                    .iter()
                    .map(Arc::clone)
                    .map(FFI_PhysicalExpr::from)
                    .collect();
                Self::Hash(exprs, *size)
            }
            Partitioning::UnknownPartitioning(size) => Self::UnknownPartitioning(*size),
        }
    }
}

impl From<&FFI_Partitioning> for Partitioning {
    fn from(value: &FFI_Partitioning) -> Self {
        match value {
            FFI_Partitioning::RoundRobinBatch(size) => {
                Partitioning::RoundRobinBatch(*size)
            }
            FFI_Partitioning::Hash(exprs, size) => {
                let exprs = exprs.iter().map(<Arc<dyn PhysicalExpr>>::from).collect();
                Self::Hash(exprs, *size)
            }
            FFI_Partitioning::UnknownPartitioning(size) => {
                Self::UnknownPartitioning(*size)
            }
        }
    }
}
