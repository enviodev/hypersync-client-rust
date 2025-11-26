use std::sync::Arc;

use alloy_dyn_abi::{DynSolType, DynSolValue, Specifier};
use alloy_json_abi::EventParam;
use anyhow::{anyhow, Context, Result};
use arrow::{array::{ArrayRef, AsArray, BinaryArray, BinaryBuilder, BooleanBuilder, RecordBatch, StringArray, StringBuilder}, datatypes::{DataType, Field, Schema}};
use rayon::iter::{IndexedParallelIterator, IntoParallelRefIterator, ParallelIterator};

pub fn hex_encode_prefixed(bytes: &[u8]) -> String {
    let mut out = vec![0; bytes.len() * 2 + 2];

    out[0] = b'0';
    out[1] = b'x';

    faster_hex::hex_encode(bytes, &mut out[2..]).unwrap();

    String::from_utf8(out).unwrap()
}

fn hex_encode_array(
    input: &BinaryArray,
    prefixed: bool,
) -> StringArray {
    let mut arr = StringBuilder::new();

    if prefixed {
        for buf in input.iter() {
            arr.push(buf.map(hex_encode_prefixed));
        }
    } else {
        for buf in input.iter() {
            arr.push(buf.map(faster_hex::hex_string));
        }
    }

    arr.into()
}

pub fn hex_encode_batch(
    batch: &RecordBatch,
    prefixed: bool,
) -> RecordBatch {
    let (fields, cols) = batch
        .columns()
        .par_iter()
        .zip(batch.schema().fields().par_iter())
        .map(|(col, field)| {
            let col: ArrayRef = match col.data_type() {
                DataType::Binary => {
                    Arc::new(hex_encode_array(col.as_any().downcast_ref().unwrap(), prefixed))
                }
                _ => col.clone(),
            };

            (
                Field::new(
                    field.name().clone(),
                    col.data_type().clone(),
                    field.is_nullable(),
                ),
                col,
            )
        })
        .collect::<(Vec<_>, Vec<_>)>();

    let schema = Arc::new(Schema::new(fields));

    Ok(RecordBatch::try_new(schema, cols).unwrap())
}

pub fn decode_logs_batch(sig: &str, batch: &RecordBatch) -> Result<RecordBatch> {
    let sig = alloy_json_abi::Event::parse(sig).context("parse event signature")?;

    let schema =
        Arc::new(schema_from_event_signature(&sig).context("build arrow schema from event signature")?);

    if batch.num_rows() == 0 {
        return RecordBatch::new_empty(schema);
    }

    let event = sig.resolve().context("resolve signature into event")?;

    let topic_cols = event
        .indexed()
        .par_iter()
        .zip(["topic1", "topic2", "topic3"].par_iter())
        .map(|(decoder, topic_name)| {
            let col = batch
                .column_by_name(topic_name)
                .context("get column")?
                .as_binary_opt()
                .context("column as binary")?;
            let col = decode_col(col, decoder).context("decode column")?;
            Ok::<_, anyhow::Error>(col)
        })
        .collect::<Result<Vec<_>>>()?;

    let body_cols = {
        let data = batch
            .column_by_name("data")
            .context("get data column")?
            .as_binary_opt()
            .context("data column as binary")?;

        let tuple_decoder = DynSolType::Tuple(event.body().to_vec());

        let decoded_tuples = data
            .values_iter()
            .map(|val| {
                let tuple = tuple_decoder
                    .abi_decode_sequence(val)
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
                            "failed to decode body of a log, will write null instead. Error was: \
                             {e:?}"
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

    Ok(RecordBatch::try_new(schema, cols).unwrap())
}

fn decode_body_col<'a, I: ExactSizeIterator<Item = Option<&'a DynSolValue>>>(
    vals: I,
    ty: &DynSolType,
) -> Result<ArrayRef> {
    match ty {
        DynSolType::Bool => {
            let mut builder = BooleanBuilder::with_capacity(vals.len());

            for val in vals {
                let val = match val {
                    Some(val) => val,
                    None => {
                        builder.append_null();
                        continue;
                    }
                };

                match val {
                    DynSolValue::Bool(b) => builder.append_value(*b),
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
        DynSolType::String => {
            let mut builder = StringBuilder::new();

            for val in vals {
                let val = match val {
                    Some(val) => val,
                    None => {
                        builder.append_null();
                        continue;
                    }
                };

                match val {
                    DynSolValue::String(v) => builder.append_value(v),
                    v => {
                        return Err(anyhow!(
                            "unexpected output type from decode: {:?}",
                            v.as_type()
                        ))
                    }
                }
            }

            Ok(Arc::new(builder.finish()))
        }
        _ => {
            let mut builder = BinaryBuilder::new();

            for val in vals {
                let val = match val {
                    Some(val) => val,
                    None => {
                        builder.append_null();
                        continue;
                    }
                };

                push_sol_value_to_binary(val, &mut builder)?;
            }

            Ok(Arc::new(builder.finish()))
        }
    }
}

fn decode_col(col: &BinaryArray, decoder: &DynSolType) -> Result<ArrayRef> {
    match decoder {
        DynSolType::Bool => {
            let mut builder = BooleanBuilder::with_capacity(col.len());

            for val in col.iter() {
                let val = match val {
                    Some(val) => val,
                    None => {
                        builder.append_null();
                        continue;
                    }
                };
                match decoder.abi_decode(val).context("decode sol value")? {
                    DynSolValue::Bool(b) => builder.append_value(b),
                    v => {
                        return Err(anyhow!(
                            "unexpected output type from decode: {:?}",
                            v.as_type()
                        ))
                    }
                }
            }

            Ok(Arc::new(builder.finish()))
        }
        DynSolType::String => {
            let mut builder = StringBuilder::new();

            for val in col.iter() {
                let val = match val {
                    Some(val) => val,
                    None => {
                        builder.append_null();
                        continue;
                    }
                };

                match decoder.abi_decode(val).context("decode sol value")? {
                    DynSolValue::String(v) => builder.append_value(v),
                    v => {
                        return Err(anyhow!(
                            "unexpected output type from decode: {:?}",
                            v.as_type()
                        ))
                    }
                }
            }

            Ok(Arc::new(builder.finish()))
        }
        _ => {
            let mut builder = BinaryBuilder::new();

            for val in col.iter() {
                let val = match val {
                    Some(val) => val,
                    None => {
                        builder.append_null();
                        continue;
                    }
                };

                let val = decoder.abi_decode(val).context("decode sol value")?;
                push_sol_value_to_binary(&val, &mut builder)?;
            }

            Ok(Arc::new(builder.finish()))
        }
    }
}

fn push_sol_value_to_binary(
    val: &DynSolValue,
    builder: &mut BinaryBuilder,
) -> Result<()> {
    match val {
        DynSolValue::Int(v, _) => builder.append_value(v.to_be_bytes::<32>()),
        DynSolValue::Uint(v, _) => builder.append_value(v.to_be_bytes::<32>()),
        DynSolValue::FixedBytes(v, _) => builder.append_value(v),
        DynSolValue::Address(v) => builder.append_value(v),
        DynSolValue::Bytes(v) => builder.append_value(v),
        v => {
            return Err(anyhow!(
                "unexpected output type from decode: {:?}",
                v.as_type()
            ))
        }
    }
    Ok(())
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
        .any(|f| f.name().as_str() == input.name.as_str())
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
        DynSolType::String => Ok(DataType::Utf8),
        ty => Err(anyhow!(
            "Complex types are not supported. Unexpected type: {}",
            ty
        )),
    }
}

#[cfg(test)]
mod tests {
    use alloy_json_abi::Event;
    use alloy_primitives::I256;

    use super::*;

    #[test]
    fn test_trailing_indexed_to_schema() {
        let schema = schema_from_event_signature(
            &Event::parse(
                "Swap(address indexed sender, uint amount0In, uint amount1In, uint amount0Out, \
                 uint amount1Out, address indexed to)",
            )
            .unwrap(),
        )
        .unwrap();

        assert_eq!(
            schema,
            Schema::new(vec![
                Field::new("sender", DataType::Binary, true),
                Field::new("to", DataType::Binary, true),
                Field::new("amount0In", DataType::Binary, true),
                Field::new("amount1In", DataType::Binary, true),
                Field::new("amount0Out", DataType::Binary, true),
                Field::new("amount1Out", DataType::Binary, true),
            ])
        );
    }

    #[test]
    fn test_sol_value_to_binary() {
        let mut builder = BinaryBuilder::new();
        let input_val = I256::try_from(69).unwrap();
        let val = DynSolValue::Int(input_val, 24);

        push_sol_value_to_binary(&val, &mut builder).unwrap();

        let raw_output = builder.pop().unwrap();

        let output_val = I256::try_from_be_slice(&raw_output).unwrap();

        assert_eq!(input_val, output_val);
    }
}
