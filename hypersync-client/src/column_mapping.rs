use std::collections::BTreeMap;

use anyhow::{anyhow, Context, Result};
use hypersync_schema::ArrowChunk;
use polars_arrow::array::{
    Array, BinaryArray, Float32Array, Float64Array, Int32Array, Int64Array, MutablePrimitiveArray,
    PrimitiveArray, UInt32Array, UInt64Array,
};
use polars_arrow::compute::cast::CastOptions;
use polars_arrow::compute::{self, cast};
use polars_arrow::datatypes::{ArrowDataType, ArrowSchema as Schema, Field};
use polars_arrow::types::NativeType;
use rayon::iter::{IndexedParallelIterator, IntoParallelRefIterator, ParallelIterator};
use ruint::aliases::U256;
use serde::{Deserialize, Serialize};

use crate::ArrowBatch;

/// Column mapping for stream function output.
/// It lets you map columns you want into the DataTypes you want.
#[derive(Default, Debug, Clone, Serialize, Deserialize)]
pub struct ColumnMapping {
    /// Mapping for block data.
    #[serde(default)]
    pub block: BTreeMap<String, DataType>,
    /// Mapping for transaction data.
    #[serde(default)]
    pub transaction: BTreeMap<String, DataType>,
    /// Mapping for log data.
    #[serde(default)]
    pub log: BTreeMap<String, DataType>,
    /// Mapping for trace data.
    #[serde(default)]
    pub trace: BTreeMap<String, DataType>,
    /// Mapping for decoded log data.
    #[serde(default)]
    pub decoded_log: BTreeMap<String, DataType>,
}

#[allow(missing_docs)]
/// `DataType` is an enumeration representing the different data types that can be used in the column mapping.
/// Each variant corresponds to a specific data type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DataType {
    Float64,
    Float32,
    UInt64,
    UInt32,
    Int64,
    Int32,
}

impl From<DataType> for ArrowDataType {
    fn from(value: DataType) -> Self {
        match value {
            DataType::Float64 => Self::Float64,
            DataType::Float32 => Self::Float32,
            DataType::UInt64 => Self::UInt64,
            DataType::UInt32 => Self::UInt32,
            DataType::Int64 => Self::Int64,
            DataType::Int32 => Self::Int32,
        }
    }
}

pub fn apply_to_batch(
    batch: &ArrowBatch,
    mapping: &BTreeMap<String, DataType>,
) -> Result<ArrowBatch> {
    if mapping.is_empty() {
        return Ok(batch.clone());
    }

    let (fields, cols) = batch
        .chunk
        .columns()
        .par_iter()
        .zip(batch.schema.fields.par_iter())
        .map(|(col, field)| {
            let col = match mapping.get(&field.name) {
                Some(&dt) => {
                    if field.name == "l1_fee_scalar" {
                        map_l1_fee_scalar(&**col, dt)
                            .context(format!("apply cast to column '{}'", field.name))?
                    } else {
                        map_column(&**col, dt)
                            .context(format!("apply cast to colum '{}'", field.name))?
                    }
                }
                None => col.clone(),
            };

            Ok((
                Field::new(
                    field.name.clone(),
                    col.data_type().clone(),
                    field.is_nullable,
                ),
                col,
            ))
        })
        .collect::<Result<(Vec<_>, Vec<_>)>>()?;

    Ok(ArrowBatch {
        chunk: ArrowChunk::new(cols).into(),
        schema: Schema::from(fields).into(),
    })
}

pub fn map_l1_fee_scalar(
    col: &dyn Array,
    target_data_type: DataType,
) -> Result<Box<dyn Array + 'static>> {
    let col = col.as_any().downcast_ref::<BinaryArray<i32>>().unwrap();
    let col = Float64Array::from_iter(
        col.iter()
            .map(|v| v.map(|v| std::str::from_utf8(v).unwrap().parse().unwrap())),
    );

    let arr = compute::cast::cast(
        &*to_box(col),
        &target_data_type.into(),
        CastOptions {
            wrapped: false,
            partial: false,
        },
    )
    .with_context(|| anyhow!("failed to l1_fee_scalar to {:?}", target_data_type))?;

    Ok(arr)
}

fn to_box<T: Array>(arr: T) -> Box<dyn Array> {
    Box::new(arr)
}

fn map_column(col: &dyn Array, target_data_type: DataType) -> Result<Box<dyn Array + 'static>> {
    match target_data_type {
        DataType::Float64 => map_to_f64(col).map(to_box),
        DataType::Float32 => map_to_f32(col).map(to_box),
        DataType::UInt64 => map_to_uint64(col).map(to_box),
        DataType::UInt32 => map_to_uint32(col).map(to_box),
        DataType::Int64 => map_to_int64(col).map(to_box),
        DataType::Int32 => map_to_int32(col).map(to_box),
    }
}

fn map_to_f64(col: &dyn Array) -> Result<Float64Array> {
    match col.data_type() {
        &ArrowDataType::Binary => {
            binary_to_target_array(col.as_any().downcast_ref::<BinaryArray<i32>>().unwrap())
        }
        &ArrowDataType::UInt64 => Ok(cast::primitive_as_primitive(
            col.as_any().downcast_ref::<UInt64Array>().unwrap(),
            &ArrowDataType::Float64,
        )),
        dt => Err(anyhow!("Can't convert {:?} to f64", dt)),
    }
}

fn map_to_f32(col: &dyn Array) -> Result<Float32Array> {
    match col.data_type() {
        &ArrowDataType::Binary => {
            binary_to_target_array(col.as_any().downcast_ref::<BinaryArray<i32>>().unwrap())
        }
        &ArrowDataType::UInt64 => Ok(cast::primitive_as_primitive(
            col.as_any().downcast_ref::<UInt64Array>().unwrap(),
            &ArrowDataType::Float32,
        )),
        dt => Err(anyhow!("Can't convert {:?} to f32", dt)),
    }
}

fn map_to_uint64(col: &dyn Array) -> Result<UInt64Array> {
    match col.data_type() {
        &ArrowDataType::Binary => {
            binary_to_target_array(col.as_any().downcast_ref::<BinaryArray<i32>>().unwrap())
        }
        &ArrowDataType::UInt64 => Ok(cast::primitive_as_primitive(
            col.as_any().downcast_ref::<UInt64Array>().unwrap(),
            &ArrowDataType::UInt64,
        )),
        dt => Err(anyhow!("Can't convert {:?} to uint64", dt)),
    }
}

fn map_to_uint32(col: &dyn Array) -> Result<UInt32Array> {
    match col.data_type() {
        &ArrowDataType::Binary => {
            binary_to_target_array(col.as_any().downcast_ref::<BinaryArray<i32>>().unwrap())
        }
        &ArrowDataType::UInt64 => Ok(cast::primitive_as_primitive(
            col.as_any().downcast_ref::<UInt64Array>().unwrap(),
            &ArrowDataType::UInt32,
        )),
        dt => Err(anyhow!("Can't convert {:?} to uint32", dt)),
    }
}

fn map_to_int64(col: &dyn Array) -> Result<Int64Array> {
    match col.data_type() {
        &ArrowDataType::Binary => {
            binary_to_target_array(col.as_any().downcast_ref::<BinaryArray<i32>>().unwrap())
        }
        &ArrowDataType::UInt64 => Ok(cast::primitive_as_primitive(
            col.as_any().downcast_ref::<UInt64Array>().unwrap(),
            &ArrowDataType::Int64,
        )),
        dt => Err(anyhow!("Can't convert {:?} to int64", dt)),
    }
}

fn map_to_int32(col: &dyn Array) -> Result<Int32Array> {
    match col.data_type() {
        &ArrowDataType::Binary => {
            binary_to_target_array(col.as_any().downcast_ref::<BinaryArray<i32>>().unwrap())
        }
        &ArrowDataType::UInt64 => Ok(cast::primitive_as_primitive(
            col.as_any().downcast_ref::<UInt64Array>().unwrap(),
            &ArrowDataType::Int32,
        )),
        dt => Err(anyhow!("Can't convert {:?} to int32", dt)),
    }
}

fn binary_to_target_array<T: NativeType + TryFrom<U256>>(
    src: &BinaryArray<i32>,
) -> Result<PrimitiveArray<T>> {
    let mut out = MutablePrimitiveArray::with_capacity(src.len());

    for val in src.iter() {
        out.push(val.map(binary_to_target).transpose()?);
    }

    Ok(out.into())
}

fn binary_to_target<T: TryFrom<U256>>(src: &[u8]) -> Result<T> {
    let big_num = U256::from_be_slice(src);
    big_num
        .try_into()
        .map_err(|_e| anyhow!("failed to cast number to requested type"))
}
