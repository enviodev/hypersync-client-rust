use std::sync::Arc;

use alloy_dyn_abi::{DynSolType, DynSolValue, Specifier};
use alloy_json_abi::EventParam;
use anyhow::{anyhow, Context, Result};
use arrow::{
    array::{
        Array, ArrayRef, AsArray, BinaryArray, BinaryBuilder, BooleanBuilder, RecordBatch,
        RecordBatchOptions, StringArray, StringBuilder,
    },
    datatypes::{DataType, Field, Schema},
};
use rayon::iter::{IndexedParallelIterator, IntoParallelRefIterator, ParallelIterator};

pub fn hex_encode_prefixed(bytes: &[u8]) -> String {
    let mut out = vec![0; bytes.len() * 2 + 2];

    out[0] = b'0';
    out[1] = b'x';

    faster_hex::hex_encode(bytes, &mut out[2..]).unwrap();

    String::from_utf8(out).unwrap()
}

fn hex_encode_array(input: &BinaryArray, prefixed: bool) -> StringArray {
    let mut arr = StringBuilder::new();

    if prefixed {
        for buf in input.iter() {
            arr.append_option(buf.map(hex_encode_prefixed));
        }
    } else {
        for buf in input.iter() {
            arr.append_option(buf.map(faster_hex::hex_string));
        }
    }

    arr.finish()
}

pub fn hex_encode_batch(batch: &RecordBatch, prefixed: bool) -> RecordBatch {
    let (fields, cols) = batch
        .columns()
        .par_iter()
        .zip(batch.schema().fields().par_iter())
        .map(|(col, field)| {
            let col: ArrayRef = match col.data_type() {
                DataType::Binary => Arc::new(hex_encode_array(
                    col.as_any().downcast_ref().unwrap(),
                    prefixed,
                )),
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

    RecordBatch::try_new(schema, cols).unwrap()
}

pub fn decode_logs_batch(sig: &str, batch: &RecordBatch) -> Result<RecordBatch> {
    let sig = alloy_json_abi::Event::parse(sig).context("parse event signature")?;

    let schema = Arc::new(
        schema_from_event_signature(&sig).context("build arrow schema from event signature")?,
    );

    if batch.num_rows() == 0 {
        return Ok(RecordBatch::new_empty(schema));
    }

    let event = sig.resolve().context("resolve signature into event")?;

    // Anonymous events have no selector in topic0, so their indexed params start there.
    let topic_names: &[&str] = if sig.anonymous {
        &["topic0", "topic1", "topic2", "topic3"]
    } else {
        &["topic1", "topic2", "topic3"]
    };

    let topic_cols = event
        .indexed()
        .par_iter()
        .zip(topic_names.par_iter())
        .map(|(decoder, topic_name)| {
            let col = batch
                .column_by_name(topic_name)
                .context("get column")?
                .as_binary_opt::<i32>()
                .context("column as binary")?;
            if is_hashed_in_topic(decoder) {
                return Ok::<_, anyhow::Error>(Arc::new(col.clone()) as ArrayRef);
            }
            let col = decode_col(col, decoder).context("decode column")?;
            Ok(col)
        })
        .collect::<Result<Vec<_>>>()?;

    let body_cols = {
        let data = batch
            .column_by_name("data")
            .context("get data column")?
            .as_binary_opt::<i32>()
            .context("data column as binary")?;

        let tuple_decoder = DynSolType::Tuple(event.body().to_vec());

        let decoded_tuples = data
            .iter()
            .map(|opt| {
                let val = opt.unwrap_or(b"");
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

    // The row count is needed for events without params, which have no columns.
    let options = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
    RecordBatch::try_new_with_options(schema, cols, &options).context("build decoded batch")
}

/// Indexed params that aren't value types are stored in the topic as the
/// keccak256 hash of their encoding, so they can't be decoded.
fn is_hashed_in_topic(ty: &DynSolType) -> bool {
    !matches!(
        ty,
        DynSolType::Bool
            | DynSolType::Int(_)
            | DynSolType::Uint(_)
            | DynSolType::FixedBytes(_)
            | DynSolType::Address
    )
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

            Ok(Arc::new(builder.finish()))
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

fn push_sol_value_to_binary(val: &DynSolValue, builder: &mut BinaryBuilder) -> Result<()> {
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

    Ok(Schema::new(fields))
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

    let dt = if input.indexed && is_hashed_in_topic(&ty) {
        DataType::Binary
    } else {
        simple_type_to_data_type(&ty).context("convert simple type to arrow datatype")?
    };

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

    fn word(v: u64) -> Vec<u8> {
        alloy_primitives::U256::from(v).to_be_bytes::<32>().to_vec()
    }

    /// A raw logs batch with one row, as returned by HyperSync.
    fn logs_batch(topics: [Option<Vec<u8>>; 4], data: Vec<u8>) -> RecordBatch {
        let mut fields = Vec::new();
        let mut cols: Vec<ArrayRef> = Vec::new();
        for (i, topic) in topics.into_iter().enumerate() {
            fields.push(Field::new(format!("topic{i}"), DataType::Binary, true));
            cols.push(Arc::new(BinaryArray::from(vec![topic.as_deref()])));
        }
        fields.push(Field::new("data", DataType::Binary, true));
        cols.push(Arc::new(BinaryArray::from(vec![Some(data.as_slice())])));
        RecordBatch::try_new(Arc::new(Schema::new(fields)), cols).unwrap()
    }

    fn binary_value(batch: &RecordBatch, name: &str) -> Vec<u8> {
        batch
            .column_by_name(name)
            .unwrap()
            .as_binary::<i32>()
            .value(0)
            .to_vec()
    }

    #[test]
    fn test_decode_logs_batch_no_params() {
        let sig = Event::parse("Ping()").unwrap();
        let batch = logs_batch([Some(sig.selector().to_vec()), None, None, None], vec![]);

        let decoded = decode_logs_batch("Ping()", &batch).unwrap();

        assert_eq!(decoded.num_rows(), 1);
        assert_eq!(decoded.num_columns(), 0);
    }

    #[test]
    fn test_decode_logs_batch_anonymous() {
        let sig = "E(uint256 indexed a, uint256 indexed b, uint256 indexed c, uint256 indexed d) \
                   anonymous";
        let batch = logs_batch(
            [Some(word(1)), Some(word(2)), Some(word(3)), Some(word(4))],
            vec![],
        );

        let decoded = decode_logs_batch(sig, &batch).unwrap();

        assert_eq!(binary_value(&decoded, "a"), word(1));
        assert_eq!(binary_value(&decoded, "b"), word(2));
        assert_eq!(binary_value(&decoded, "c"), word(3));
        assert_eq!(binary_value(&decoded, "d"), word(4));
    }

    #[test]
    fn test_decode_logs_batch_indexed_dynamic_is_hash() {
        let sig = "E(string indexed name, bytes indexed blob, uint256 value)";
        let event = Event::parse(sig).unwrap();
        let name_hash = alloy_primitives::keccak256("").to_vec();
        let blob_hash = alloy_primitives::keccak256([1u8, 2, 3]).to_vec();
        let batch = logs_batch(
            [
                Some(event.selector().to_vec()),
                Some(name_hash.clone()),
                Some(blob_hash.clone()),
                None,
            ],
            word(7),
        );

        let decoded = decode_logs_batch(sig, &batch).unwrap();

        assert_eq!(
            decoded
                .schema()
                .field_with_name("name")
                .unwrap()
                .data_type(),
            &DataType::Binary
        );
        assert_eq!(binary_value(&decoded, "name"), name_hash);
        assert_eq!(binary_value(&decoded, "blob"), blob_hash);
        assert_eq!(binary_value(&decoded, "value"), word(7));
    }

    #[test]
    fn test_sol_value_to_binary() {
        let mut builder = BinaryBuilder::new();
        let input_val = I256::try_from(69).unwrap();
        let val = DynSolValue::Int(input_val, 24);

        push_sol_value_to_binary(&val, &mut builder).unwrap();

        let arr = builder.finish();

        assert!(arr.is_valid(0));
        let raw_output = arr.value(0);

        let output_val = I256::try_from_be_slice(raw_output).unwrap();

        assert_eq!(input_val, output_val);
    }
}
