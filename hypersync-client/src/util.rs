use std::sync::Arc;

use alloy_dyn_abi::{DynSolType, DynSolValue, Specifier};
use alloy_json_abi::EventParam;
use anyhow::{anyhow, Context, Result};
use hypersync_schema::empty_chunk;
use polars_arrow::{
    array::{
        Array, ArrayFromIter, BinaryArray, BinaryViewArray, MutableArray, MutableBinaryArray,
        MutableBooleanArray, MutableUtf8Array, Utf8Array, Utf8ViewArray,
    },
    datatypes::{ArrowDataType as DataType, ArrowSchema as Schema, Field},
};
use rayon::iter::{
    IndexedParallelIterator, IntoParallelRefIterator, ParallelBridge, ParallelIterator,
};

use crate::{ArrowBatch, ArrowChunk};

pub fn hex_encode_prefixed(bytes: &[u8]) -> String {
    let mut out = vec![0; bytes.len() * 2 + 2];

    out[0] = b'0';
    out[1] = b'x';

    faster_hex::hex_encode(bytes, &mut out[2..]).unwrap();

    unsafe { String::from_utf8_unchecked(out) }
}

pub fn hex_encode_batch<F: Fn(&[u8]) -> String + Send + Sync + Copy>(
    batch: &ArrowBatch,
    encode: F,
) -> ArrowBatch {
    let (fields, cols) = batch
        .chunk
        .columns()
        .par_iter()
        .zip(batch.schema.fields.par_iter())
        .map(|(col, field)| {
            let col = match col.data_type() {
                DataType::Binary => {
                    Box::new(hex_encode(col.as_any().downcast_ref().unwrap(), encode))
                }
                _ => col.clone(),
            };

            (
                Field::new(
                    field.name.clone(),
                    col.data_type().clone(),
                    field.is_nullable,
                ),
                col,
            )
        })
        .collect::<(Vec<_>, Vec<_>)>();

    ArrowBatch {
        chunk: ArrowChunk::new(cols).into(),
        schema: Schema::from(fields).into(),
    }
}

fn hex_encode<F: Fn(&[u8]) -> String + Copy>(
    input: &BinaryArray<i32>,
    encode: F,
) -> Utf8Array<i32> {
    let mut arr = MutableUtf8Array::<i32>::new();

    for buf in input.iter() {
        arr.push(buf.map(encode));
    }

    arr.into()
}

pub fn decode_logs_batch(sig: &str, batch: &ArrowBatch) -> Result<ArrowBatch> {
    let sig = alloy_json_abi::Event::parse(sig).context("parse event signature")?;

    let schema =
        schema_from_event_signature(&sig).context("build arrow schema from event signature")?;

    if batch.chunk.is_empty() {
        return Ok(ArrowBatch {
            chunk: Arc::new(empty_chunk(&schema)),
            schema: Arc::new(schema),
        });
    }

    let event = sig.resolve().context("resolve signature into event")?;

    let topic_cols = event
        .indexed()
        .par_iter()
        .zip(["topic1", "topic2", "topic3"].par_iter())
        .map(|(decoder, topic_name)| {
            let col = batch
                .column::<BinaryArray<i32>>(topic_name)
                .context("get column")?;
            let col = decode_col(col, decoder).context("decode column")?;
            Ok::<_, anyhow::Error>(col)
        })
        .collect::<Result<Vec<_>>>()?;

    let body_cols = {
        let data = batch
            .column::<BinaryArray<i32>>("data")
            .context("get column")?;

        let tuple_decoder = DynSolType::Tuple(event.body().to_vec());

        let decoded_tuples = data
            .values_iter()
            .par_bridge()
            .map(|val| {
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
                        log::trace!(
                        "failed to decode body of a log, will write null instead. Error was: {:?}",
                        e
                    );
                        None
                    }
                    Ok(v) => v,
                };

                Ok(tuple)
            })
            .collect::<Result<Vec<_>>>()?;

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

        event
            .body()
            .par_iter()
            .enumerate()
            .map(|(i, ty)| {
                decode_body_col(
                    decoded_tuples
                        .iter()
                        .map(|t| t.as_ref().map(|t| t.get(i).unwrap())),
                    ty,
                )
                .context("decode body column")
            })
            .collect::<Result<Vec<_>>>()?
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
            let mut builder = MutableBinaryArray::<i32>::new();

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

fn decode_col(col: &BinaryArray<i32>, decoder: &DynSolType) -> Result<Box<dyn Array>> {
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
            let mut builder = MutableBinaryArray::<i32>::new();

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

fn schema_from_event_signature(sig: &alloy_json_abi::Event) -> Result<Schema> {
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
        DynSolType::Int(_) => Ok(DataType::Binary),
        DynSolType::Uint(_) => Ok(DataType::Binary),
        DynSolType::FixedBytes(_) => Ok(DataType::Binary),
        DynSolType::Address => Ok(DataType::Binary),
        DynSolType::Bytes => Ok(DataType::Binary),
        DynSolType::String => Ok(DataType::Binary),
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
        let schema = schema_from_event_signature(&Event::parse(
            "Swap(address indexed sender, uint amount0In, uint amount1In, uint amount0Out, uint amount1Out, address indexed to)"
        ).unwrap()).unwrap();

        assert_eq!(
            schema,
            Schema::from(vec![
                Field::new("sender", DataType::Binary, true),
                Field::new("to", DataType::Binary, true),
                Field::new("amount0In", DataType::Binary, true),
                Field::new("amount1In", DataType::Binary, true),
                Field::new("amount0Out", DataType::Binary, true),
                Field::new("amount1Out", DataType::Binary, true),
            ])
        );
    }
}
