use std::{collections::BTreeMap, sync::Arc};

use alloy_primitives::I256;
use anyhow::{anyhow, Context, Result};
use arrow::{
    array::{
        Array, ArrayRef, ArrowPrimitiveType, AsArray, BinaryArray, Decimal128Array,
        Decimal256Array, Decimal256Builder, Float32Array, Float64Array, Int32Array, Int64Array,
        PrimitiveArray, PrimitiveBuilder, RecordBatch, StringArray, StringBuilder, UInt32Array,
        UInt64Array,
    },
    compute,
    datatypes::{i256, DataType as ArrowDataType, Field, Schema},
};
use rayon::iter::{IndexedParallelIterator, IntoParallelRefIterator, ParallelIterator};
use ruint::aliases::U256;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Column mapping for stream function output.
/// It lets you map columns you want into the DataTypes you want.
#[derive(Default, Debug, Clone, Serialize, Deserialize, JsonSchema)]
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum DataType {
    Float64,
    Float32,
    UInt64,
    UInt32,
    Int64,
    Int32,
    IntStr,
    Decimal256,
    Decimal128,
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
            DataType::IntStr => Self::Utf8,
            DataType::Decimal256 => Self::Decimal256(76, 0),
            DataType::Decimal128 => Self::Decimal128(38, 0),
        }
    }
}

pub fn apply_to_batch(
    batch: &RecordBatch,
    mapping: &BTreeMap<String, DataType>,
) -> Result<RecordBatch> {
    if mapping.is_empty() {
        return Ok(batch.clone());
    }

    let (fields, cols) = batch
        .columns()
        .par_iter()
        .zip(batch.schema().fields().par_iter())
        .map(|(col, field)| {
            let col = match mapping.get(field.name()) {
                Some(&dt) => {
                    if field.name() == "l1_fee_scalar" {
                        map_l1_fee_scalar(&**col, dt)
                            .context(format!("apply cast to column '{}'", field.name()))?
                    } else {
                        map_column(&**col, dt)
                            .context(format!("apply cast to column '{}'", field.name()))?
                    }
                }
                None => col.clone(),
            };

            Ok((
                Field::new(
                    field.name().clone(),
                    col.data_type().clone(),
                    field.is_nullable(),
                ),
                col,
            ))
        })
        .collect::<Result<(Vec<_>, Vec<_>)>>()?;

    let schema = Arc::new(Schema::new(fields));

    Ok(RecordBatch::try_new(schema, cols).unwrap())
}

pub fn map_l1_fee_scalar(col: &dyn Array, target_data_type: DataType) -> Result<ArrayRef> {
    let col = col.as_any().downcast_ref::<BinaryArray>().unwrap();
    let col = Float64Array::from_iter(
        col.iter()
            .map(|v| v.map(|v| std::str::from_utf8(v).unwrap().parse().unwrap())),
    );

    let arrow_dt = ArrowDataType::from(target_data_type);

    let arr = compute::cast(&col, &arrow_dt)
        .with_context(|| anyhow!("failed to l1_fee_scalar to {:?}", target_data_type))?;

    Ok(arr)
}

fn to_array_ref<Arr: Array + 'static>(arr: Arr) -> ArrayRef {
    Arc::new(arr)
}

fn map_column(col: &dyn Array, target_data_type: DataType) -> Result<ArrayRef> {
    match target_data_type {
        DataType::Float64 => map_to_f64(col).map(to_array_ref),
        DataType::Float32 => map_to_f32(col).map(to_array_ref),
        DataType::UInt64 => map_to_uint64(col).map(to_array_ref),
        DataType::UInt32 => map_to_uint32(col).map(to_array_ref),
        DataType::Int64 => map_to_int64(col).map(to_array_ref),
        DataType::Int32 => map_to_int32(col).map(to_array_ref),
        DataType::IntStr => map_to_int_str(col).map(to_array_ref),
        DataType::Decimal256 => map_to_decimal(col).map(to_array_ref),
        DataType::Decimal128 => map_to_decimal128(col).map(to_array_ref),
    }
}

fn map_to_decimal(col: &dyn Array) -> Result<Decimal256Array> {
    match col.data_type() {
        &ArrowDataType::Binary => {
            binary_to_decimal_array(col.as_any().downcast_ref::<BinaryArray>().unwrap())
        }
        dt => Err(anyhow!("Can't convert {:?} to decimal", dt)),
    }
}

fn binary_to_decimal_array(arr: &BinaryArray) -> Result<Decimal256Array> {
    let mut out = Decimal256Builder::with_capacity(arr.len());

    for val in arr.iter() {
        out.append_option(val.map(binary_to_decimal).transpose()?);
    }

    Ok(out.finish())
}

fn binary_to_decimal(binary: &[u8]) -> Result<i256> {
    let big_num = I256::try_from_be_slice(binary).context("failed to parse number into I256")?;
    let decimal = i256::from_be_bytes(big_num.to_be_bytes::<32>());

    Ok(decimal)
}

fn map_to_int_str(col: &dyn Array) -> Result<StringArray> {
    match col.data_type() {
        &ArrowDataType::Binary => {
            binary_to_int_str_array(col.as_any().downcast_ref::<BinaryArray>().unwrap())
        }
        dt => Err(anyhow!("Can't convert {:?} to intstr", dt)),
    }
}

fn binary_to_int_str_array(arr: &BinaryArray) -> Result<StringArray> {
    let mut out = StringBuilder::new();

    for val in arr.iter() {
        out.append_option(val.map(binary_to_int_str).transpose()?);
    }

    Ok(out.finish())
}

fn binary_to_int_str(binary: &[u8]) -> Result<String> {
    let big_num = I256::try_from_be_slice(binary).context("failed to parse number into I256")?;
    Ok(format!("{big_num}"))
}

fn map_to_f64(col: &dyn Array) -> Result<Float64Array> {
    match col.data_type() {
        &ArrowDataType::Binary => binary_to_target_array(col.as_binary(), binary_to_f64),
        &ArrowDataType::UInt64 => Ok(compute::cast(col, &ArrowDataType::Float64)
            .context("arrow cast")?
            .as_primitive()
            .clone()),
        dt => Err(anyhow!("Can't convert {:?} to f64", dt)),
    }
}

fn map_to_f32(col: &dyn Array) -> Result<Float32Array> {
    match col.data_type() {
        &ArrowDataType::Binary => binary_to_target_array(col.as_binary(), binary_to_f32),
        &ArrowDataType::UInt64 => Ok(compute::cast(col, &ArrowDataType::Float32)
            .context("arrow cast")?
            .as_primitive()
            .clone()),
        dt => Err(anyhow!("Can't convert {:?} to f32", dt)),
    }
}

fn map_to_uint64(col: &dyn Array) -> Result<UInt64Array> {
    match col.data_type() {
        &ArrowDataType::Binary => {
            binary_to_target_array(col.as_binary(), signed_binary_to_target::<u64>)
        }
        &ArrowDataType::UInt64 => Ok(col.as_primitive().clone()),
        dt => Err(anyhow!("Can't convert {:?} to uint64", dt)),
    }
}

fn map_to_uint32(col: &dyn Array) -> Result<UInt32Array> {
    match col.data_type() {
        &ArrowDataType::Binary => {
            binary_to_target_array(col.as_binary(), signed_binary_to_target::<u32>)
        }
        &ArrowDataType::UInt64 => Ok(compute::cast(col, &ArrowDataType::UInt32)
            .context("arrow cast")?
            .as_primitive()
            .clone()),
        dt => Err(anyhow!("Can't convert {:?} to uint32", dt)),
    }
}

fn map_to_decimal128(col: &dyn Array) -> Result<Decimal128Array> {
    match col.data_type() {
        &ArrowDataType::Binary => {
            binary_to_target_array(col.as_binary(), signed_binary_to_target::<i128>)
        }
        &ArrowDataType::UInt64 => Ok(compute::cast(col, &ArrowDataType::Decimal128(38, 0))
            .context("arrow cast")?
            .as_primitive()
            .clone()),
        dt => Err(anyhow!("Can't convert {:?} to int64", dt)),
    }
}

fn map_to_int64(col: &dyn Array) -> Result<Int64Array> {
    match col.data_type() {
        &ArrowDataType::Binary => {
            binary_to_target_array(col.as_binary(), signed_binary_to_target::<i64>)
        }
        &ArrowDataType::UInt64 => Ok(compute::cast(col, &ArrowDataType::Int64)
            .context("arrow cast")?
            .as_primitive()
            .clone()),
        dt => Err(anyhow!("Can't convert {:?} to int64", dt)),
    }
}

fn map_to_int32(col: &dyn Array) -> Result<Int32Array> {
    match col.data_type() {
        &ArrowDataType::Binary => {
            binary_to_target_array(col.as_binary(), signed_binary_to_target::<i32>)
        }
        &ArrowDataType::UInt64 => Ok(compute::cast(col, &ArrowDataType::Int32)
            .context("arrow cast")?
            .as_primitive()
            .clone()),
        dt => Err(anyhow!("Can't convert {:?} to int32", dt)),
    }
}

fn binary_to_target_array<T: ArrowPrimitiveType>(
    src: &BinaryArray,
    convert: fn(&[u8]) -> Result<T::Native>,
) -> Result<PrimitiveArray<T>> {
    let mut out = PrimitiveBuilder::<T>::with_capacity(src.len());

    for val in src.iter() {
        out.append_option(val.map(convert).transpose()?);
    }

    Ok(out.finish())
}

fn signed_binary_to_target<T: TryFrom<I256>>(src: &[u8]) -> Result<T> {
    let big_num = I256::try_from_be_slice(src).context("failed to parse number into I256")?;

    big_num
        .try_into()
        .map_err(|_e| anyhow!("failed to cast number to requested signed type"))
}

// Special case for float because floats don't implement TryFrom<I256>
fn binary_to_f64(src: &[u8]) -> Result<f64> {
    let big_num = I256::try_from_be_slice(src).context("failed to parse number into I256")?;

    let x = f64::from(U256::try_from(big_num.abs()).unwrap());

    if !big_num.is_negative() {
        Ok(x)
    } else {
        Ok(-x)
    }
}

// Special case for float because floats don't implement TryFrom<I256>
fn binary_to_f32(src: &[u8]) -> Result<f32> {
    let big_num = I256::try_from_be_slice(src).context("failed to parse number into I256")?;

    let x = f32::from(U256::try_from(big_num.abs()).unwrap());

    if !big_num.is_negative() {
        Ok(x)
    } else {
        Ok(-x)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_signed_binary_to_target() {
        const RAW_INPUT: &[i64] = &[-69, 0, 69, -1, 1, i64::MAX, i64::MIN];

        for &input_num in RAW_INPUT {
            let input = I256::try_from(input_num).unwrap();
            let input_bytes = input.to_be_bytes::<32>();
            let input_bytes = input_bytes.as_slice();
            let output = signed_binary_to_target::<i64>(input_bytes).unwrap();
            assert_eq!(i64::try_from(input).unwrap(), output);

            let float_output = binary_to_f64(input_bytes).unwrap();
            assert_eq!(I256::try_from(float_output as i64).unwrap(), input);

            let string_output = binary_to_int_str(input_bytes).unwrap();
            assert_eq!(string_output, format!("{}", input_num));

            let decimal_output = binary_to_decimal(input_bytes).unwrap();
            assert_eq!(decimal_output.to_be_bytes(), input_bytes);
            assert_eq!(format!("{}", decimal_output), format!("{}", input));
        }
    }
}
