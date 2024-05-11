use std::{
    collections::{BTreeMap, VecDeque},
    path::PathBuf,
    sync::Arc,
    time::Instant,
};

use alloy_dyn_abi::{DynSolType, DynSolValue, Specifier};
use alloy_json_abi::EventParam;
use anyhow::{anyhow, Context, Result};
use hypersync_net_types::Query;
use hypersync_schema::{concat_chunks, empty_chunk};
use polars_arrow::{
    array::{Array, BinaryViewArray, MutableArray, MutableBooleanArray, Utf8ViewArray},
    datatypes::{ArrowDataType as DataType, ArrowSchema as Schema, Field},
};
use polars_arrow::{
    array::{ArrayFromIter, BinaryArray, MutableBinaryViewArray, Utf8Array},
    legacy::error::PolarsError,
};
use polars_parquet::parquet::write::FileStreamer;
use polars_parquet::{
    read::ParquetError,
    write::{
        array_to_columns, to_parquet_schema, to_parquet_type, transverse, CompressedPage, DynIter,
        DynStreamingIterator, Encoding, FallibleStreamingIterator, RowGroupIter, WriteOptions,
    },
};
use rayon::prelude::*;
use tokio::{sync::mpsc, task::JoinHandle};
use tokio_util::compat::TokioAsyncReadCompatExt;

use crate::{column_mapping, rayon_async, ArrowBatch, ArrowChunk, Client};

pub fn hex_encode_batch(batch: &ArrowBatch) -> anyhow::Result<ArrowBatch> {
    let mut cols = Vec::with_capacity(batch.chunk.columns().len());
    let mut fields = Vec::with_capacity(batch.schema.fields.len());

    for (col, field) in batch.chunk.columns().iter().zip(batch.schema.fields.iter()) {
        let col = match col.data_type() {
            DataType::BinaryView => Box::new(hex_encode(col.as_any().downcast_ref().unwrap())),
            _ => col.clone(),
        };

        fields.push(Field::new(
            field.name.clone(),
            col.data_type().clone(),
            field.is_nullable,
        ));
        cols.push(col);
    }

    Ok(ArrowBatch {
        chunk: ArrowChunk::new(cols),
        schema: Schema::from(fields),
    })
}

fn hex_encode(input: &BinaryViewArray) -> Utf8ViewArray {
    let mut arr = MutableBinaryViewArray::<str>::new();

    for buf in input.iter() {
        arr.push(buf.map(faster_hex::hex_string));
    }

    arr.into()
}

pub fn decode_logs_batch(
    sig: &Option<alloy_json_abi::Event>,
    batch: ArrowBatch,
) -> Result<ArrowBatch> {
    let schema =
        schema_from_event_signature(sig).context("build arrow schema from event signature")?;

    if batch.chunk.is_empty() {
        return Ok(ArrowBatch {
            chunk: Arc::new(empty_chunk(&schema)),
            schema: Arc::new(schema),
        });
    }

    let sig = match sig {
        Some(sig) => sig,
        None => {
            return Ok(ArrowBatch {
                chunk: Arc::new(empty_chunk(&schema)),
                schema: Arc::new(schema),
            })
        }
    };

    let event = sig.resolve().context("resolve signature into event")?;

    let topic_cols = event
        .indexed()
        .iter()
        .zip(["topic1", "topic2", "topic3"].iter())
        .map(|(decoder, topic_name)| {
            let col = batch
                .column::<BinaryViewArray>(topic_name)
                .context("get column")?;
            let col = decode_col(col, decoder).context("decode column")?;
            Ok::<_, anyhow::Error>(col)
        })
        .collect::<Result<Vec<_>>>()?;

    let body_cols = {
        let data = batch
            .column::<BinaryViewArray>("data")
            .context("get column")?;

        let tuple_decoder = DynSolType::Tuple(event.body().to_vec());

        let mut decoded_tuples = Vec::with_capacity(data.len());
        for val in data.values_iter() {
            let tuple = tuple_decoder
                .abi_decode(val)
                .context("decode body tuple")
                .and_then(|v| {
                    let tuple = v
                        .as_tuple()
                        .context("expected tuple after decoding")?
                        .to_vec();

                    if tuple.len() != event.body().len() {
                        return Err(anyhow!(
                            "expected tuple of length {} after decoding",
                            event.body().len()
                        ));
                    }

                    Ok(Some(tuple))
                });

            let tuple = match tuple {
                Err(e) => {
                    log::error!(
                        "failed to decode body of a log, will write null instead. Error was: {:?}",
                        e
                    );
                    None
                }
                Ok(v) => v,
            };

            decoded_tuples.push(tuple);
        }

        let mut decoded_cols = Vec::with_capacity(event.body().len());

        for (i, ty) in event.body().iter().enumerate() {
            decoded_cols.push(
                decode_body_col(
                    decoded_tuples
                        .iter()
                        .map(|t| t.as_ref().map(|t| t.get(i).unwrap())),
                    ty,
                )
                .context("decode body column")?,
            );
        }

        decoded_cols
    };

    let mut cols = topic_cols;
    cols.extend_from_slice(&body_cols);

    let chunk = Arc::new(ArrowChunk::try_new(cols).context("create arrow chunk")?);

    Ok(ArrowBatch {
        chunk,
        schema: Arc::new(schema),
    })
}

fn decode_body_col<'a, I: ExactSizeIterator<Item = Option<&'a DynSolValue>>>(
    vals: I,
    ty: &DynSolType,
) -> Result<Box<dyn Array>> {
    match ty {
        DynSolType::Bool => {
            let mut builder = MutableBooleanArray::with_capacity(vals.len());

            for val in vals {
                let val = match val {
                    Some(val) => val,
                    None => {
                        builder.push_null();
                        continue;
                    }
                };

                match val {
                    DynSolValue::Bool(b) => builder.push(Some(*b)),
                    v => {
                        return Err(anyhow!(
                            "unexpected output type from decode: {:?}",
                            v.as_type()
                        ))
                    }
                }
            }

            Ok(builder.as_box())
        }
        _ => {
            let mut builder = MutableBinaryViewArray::<[u8]>::new();

            for val in vals {
                let val = match val {
                    Some(val) => val,
                    None => {
                        builder.push_null();
                        continue;
                    }
                };

                match val {
                    DynSolValue::Int(v, _) => builder.push(Some(v.to_be_bytes::<32>())),
                    DynSolValue::Uint(v, _) => builder.push(Some(v.to_be_bytes::<32>())),
                    DynSolValue::FixedBytes(v, _) => builder.push(Some(v)),
                    DynSolValue::Address(v) => builder.push(Some(v)),
                    DynSolValue::Bytes(v) => builder.push(Some(v)),
                    DynSolValue::String(v) => builder.push(Some(v)),
                    v => {
                        return Err(anyhow!(
                            "unexpected output type from decode: {:?}",
                            v.as_type()
                        ))
                    }
                }
            }

            Ok(builder.as_box())
        }
    }
}

fn decode_erc20_amount(data: &BinaryViewArray, decoder: &DynSolType) -> Result<Box<dyn Array>> {
    let mut builder = MutableBinaryViewArray::<[u8]>::new();

    for val in data.values_iter() {
        // Check if we are decoding a single u256 and the body is empty
        //
        // This case can happen when decoding zero value erc20 transfers
        let v = if val.is_empty() {
            [0; 32].as_slice()
        } else {
            val
        };

        match decoder.abi_decode(v).context("decode val")? {
            DynSolValue::Uint(v, _) => builder.push(Some(v.to_be_bytes::<32>())),
            v => {
                return Err(anyhow!(
                    "unexpected output type from decode: {:?}",
                    v.as_type()
                ))
            }
        }
    }

    Ok(builder.as_box())
}

fn decode_col(col: &BinaryViewArray, decoder: &DynSolType) -> Result<Box<dyn Array>> {
    match decoder {
        DynSolType::Bool => {
            let mut builder = MutableBooleanArray::with_capacity(col.len());

            for val in col.iter() {
                let val = match val {
                    Some(val) => val,
                    None => {
                        builder.push_null();
                        continue;
                    }
                };
                match decoder.abi_decode(val).context("decode sol value")? {
                    DynSolValue::Bool(b) => builder.push(Some(b)),
                    v => {
                        return Err(anyhow!(
                            "unexpected output type from decode: {:?}",
                            v.as_type()
                        ))
                    }
                }
            }

            Ok(builder.as_box())
        }
        _ => {
            let mut builder = MutableBinaryViewArray::<[u8]>::new();

            for val in col.iter() {
                let val = match val {
                    Some(val) => val,
                    None => {
                        builder.push_null();
                        continue;
                    }
                };

                match decoder.abi_decode(val).context("decode sol value")? {
                    DynSolValue::Int(v, _) => builder.push(Some(v.to_be_bytes::<32>())),
                    DynSolValue::Uint(v, _) => builder.push(Some(v.to_be_bytes::<32>())),
                    DynSolValue::FixedBytes(v, _) => builder.push(Some(v)),
                    DynSolValue::Address(v) => builder.push(Some(v)),
                    DynSolValue::Bytes(v) => builder.push(Some(v)),
                    DynSolValue::String(v) => builder.push(Some(v)),
                    v => {
                        return Err(anyhow!(
                            "unexpected output type from decode: {:?}",
                            v.as_type()
                        ))
                    }
                }
            }

            Ok(builder.as_box())
        }
    }
}

fn schema_from_event_signature(sig: &Option<alloy_json_abi::Event>) -> Result<Schema> {
    let sig = match sig {
        Some(sig) => sig,
        None => {
            return Ok(Schema::from(vec![Field::new(
                "dummy",
                DataType::Boolean,
                true,
            )]));
        }
    };

    let event = sig.resolve().context("resolve signature into event")?;

    let mut fields: Vec<Field> = Vec::with_capacity(sig.inputs.len());

    for (input, resolved_type) in sig
        .inputs
        .iter()
        .filter(|i| i.indexed)
        .zip(event.indexed().iter())
    {
        fields.push(
            signature_input_to_field(&fields, input, resolved_type).context("process input")?,
        );
    }

    for (input, resolved_type) in sig
        .inputs
        .iter()
        .filter(|i| !i.indexed)
        .zip(event.body().iter())
    {
        fields.push(
            signature_input_to_field(&fields, input, resolved_type).context("process input")?,
        );
    }

    Ok(Schema::from(fields))
}

fn signature_input_to_field(
    fields: &[Field],
    input: &EventParam,
    resolved_type: &DynSolType,
) -> Result<Field> {
    if input.name.is_empty() {
        return Err(anyhow!("empty param names are not supported"));
    }

    if fields
        .iter()
        .any(|f| f.name.as_str() == input.name.as_str())
    {
        return Err(anyhow!("duplicate param name: {}", input.name));
    }

    let ty = DynSolType::parse(&input.ty).context("parse solidity type")?;

    if &ty != resolved_type {
        return Err(anyhow!(
            "Internal error: Parsed type doesn't match resolved type. This should never happen."
        ));
    }

    let dt = simple_type_to_data_type(&ty).context("convert simple type to arrow datatype")?;

    Ok(Field::new(input.name.clone(), dt, true))
}

fn simple_type_to_data_type(ty: &DynSolType) -> Result<DataType> {
    match ty {
        DynSolType::Bool => Ok(DataType::Boolean),
        DynSolType::Int(_) => Ok(DataType::BinaryView),
        DynSolType::Uint(_) => Ok(DataType::BinaryView),
        DynSolType::FixedBytes(_) => Ok(DataType::BinaryView),
        DynSolType::Address => Ok(DataType::BinaryView),
        DynSolType::Bytes => Ok(DataType::BinaryView),
        DynSolType::String => Ok(DataType::BinaryView),
        ty => Err(anyhow!(
            "Complex types are not supported. Unexpected type: {}",
            ty
        )),
    }
}

pub fn map_batch_to_binary_view(batch: ArrowBatch) -> ArrowBatch {
    let cols = batch
        .chunk
        .arrays()
        .iter()
        .map(|col| match col.data_type() {
            DataType::Binary => BinaryViewArray::arr_from_iter(
                col.as_any()
                    .downcast_ref::<BinaryArray<i32>>()
                    .unwrap()
                    .iter(),
            )
            .boxed(),
            DataType::Utf8 => Utf8ViewArray::arr_from_iter(
                col.as_any()
                    .downcast_ref::<Utf8Array<i32>>()
                    .unwrap()
                    .iter(),
            )
            .boxed(),
            _ => col.clone(),
        })
        .collect::<Vec<_>>();

    let fields = cols
        .iter()
        .zip(batch.schema.fields.iter())
        .map(|(col, field)| {
            Field::new(
                field.name.clone(),
                col.data_type().clone(),
                field.is_nullable,
            )
        })
        .collect::<Vec<_>>();

    let schema = Schema {
        fields,
        metadata: Default::default(),
    };

    ArrowBatch {
        chunk: Arc::new(ArrowChunk::new(cols)),
        schema: Arc::new(schema),
    }
}

#[cfg(test)]
mod tests {
    use alloy_json_abi::Event;

    use super::*;

    #[test]
    fn test_trailing_indexed_to_schema() {
        let schema = schema_from_event_signature(&Some(Event::parse(
            "Swap(address indexed sender, uint amount0In, uint amount1In, uint amount0Out, uint amount1Out, address indexed to)"
        ).unwrap())).unwrap();

        assert_eq!(
            schema,
            Schema::from(vec![
                Field::new("sender", DataType::BinaryView, true),
                Field::new("to", DataType::BinaryView, true),
                Field::new("amount0In", DataType::BinaryView, true),
                Field::new("amount1In", DataType::BinaryView, true),
                Field::new("amount0Out", DataType::BinaryView, true),
                Field::new("amount1Out", DataType::BinaryView, true),
            ])
        );
    }
}
