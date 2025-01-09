use alloy_dyn_abi::{DynSolType, DynSolValue, JsonAbiExt};
use alloy_json_abi::Function;
use anyhow::{Context, Result};
use hypersync_format::Data;
use std::collections::HashMap;

#[derive(Debug, Hash, Eq, PartialEq)]
struct FunctionKey {
    signature: Vec<u8>,
}

type DecoderMap = HashMap<FunctionKey, Function>;

/// Decode input data parsing input data.
pub struct CallDecoder {
    // A map of signature => Function decoder
    map: DecoderMap,
}

impl CallDecoder {
    /// Initialize decoder from event signatures.
    ///
    ///     use hypersync_client::CallDecoder;
    ///     let decoder = CallDecoder::from_signatures(&[
    ///        "transfer(address to,uint256 amount)",
    ///     ]).unwrap();
    pub fn from_signatures<S: AsRef<str>>(signatures: &[S]) -> Result<Self> {
        let map: DecoderMap = signatures
            .iter()
            .map(|sig| {
                let function = Function::parse(sig.as_ref()).context("parse event signature")?;
                let signature = function.selector().to_vec();
                let event_key = FunctionKey { signature };
                Ok((event_key, function))
            })
            .collect::<Result<DecoderMap>>()
            .context("construct function decoder map")?;

        Ok(Self { map })
    }

    /// Parse input data and return result
    ///
    /// Returns Ok(None) if signature not found.
    pub fn decode_input(&self, data: &Data) -> Result<Option<Vec<DynSolValue>>> {
        let function_key = FunctionKey {
            signature: data[0..4].to_vec(),
        };
        let function = match self.map.get(&function_key) {
            Some(function) => function,
            None => return Ok(None),
        };
        let decoded = function
            .abi_decode_input(data.as_ref(), false)
            .context("decoding input data")?;
        Ok(Some(decoded))
    }

    /// Parse output data and return result
    ///
    /// Decodes the output field from a trace
    /// and returns the decoded values in a `Vec<DynSolValue>`. If the function
    /// signature is not found or the decoding fails, it returns `Ok(None)` as
    /// the result to match the behavior of `decode_input`
    pub fn decode_output(&self, data: &Data, function_signature: &str) -> Result<Option<Vec<DynSolValue>>>{
        // Parse the provided function signature into a Function object
        let function = Function::parse(function_signature).context("parsing function signature")?;

        //Extract the output types of the function
        let output_types = function.outputs;

        // Convert the output types into the corresponding DynSolType representations,
        let output_types: Vec<DynSolType> = output_types
            .into_iter()
            .map(|param| param.ty.parse::<DynSolType>())
            .collect::<Result<_, _>>()  // Parse each type as DynSolType
            .context("parsing output types")?;

        // Create a tuple type from the output parameters
        let tuple_type = DynSolType::Tuple(output_types);

        // Attempt to decode the data using the constructed tuple type
        match tuple_type.abi_decode(data.as_ref()) {
            // If decoding succeeds, return the decoded values as a tuple or single value
            Ok(decoded) => {
                if let DynSolValue::Tuple(values) = decoded {
                    Ok(Some(values)) // Return the decoded values as a Vec if it's a tuple
                } else {
                    Ok(Some(vec![decoded])) // Return a single value wrapped in a Vec
                }
            }
            // If decoding fails, return None (to match the behavior of decode_input)
            Err(_) => {
                Ok(None) // Return None to signal that decoding failed
            }
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use hypersync_format::{Data, Hex};

    #[test]
    fn test_decode_input_with_single_signature() {
        let function =
            alloy_json_abi::Function::parse("transfer(address dst, uint256 wad)").unwrap();
        let input = "0xa9059cbb000000000000000000000000dc4bde73fa35b7478a574f78d5dfd57a0b2e22810000000000000000000000000000000000000000000000004710ca26d3eeae0a";
        let input = Data::decode_hex(input).unwrap();
        let expected = function.abi_decode_input(input.as_ref(), false).unwrap();

        let decoder =
            CallDecoder::from_signatures(&["transfer(address dst, uint256 wad)"]).unwrap();
        let got = decoder.decode_input(&input).unwrap().unwrap();

        for (expected, got) in expected.iter().zip(got.iter()) {
            assert_eq!(expected, got, "Checking that decodes are the same");
        }
    }

    #[test]
    fn test_decode_input_with_multiple_signature() {
        let function =
            alloy_json_abi::Function::parse("transfer(address dst, uint256 wad)").unwrap();
        let input = "0xa9059cbb000000000000000000000000dc4bde73fa35b7478a574f78d5dfd57a0b2e22810000000000000000000000000000000000000000000000004710ca26d3eeae0a";
        let input = Data::decode_hex(input).unwrap();
        let expected = function.abi_decode_input(input.as_ref(), false).unwrap();

        let decoder = CallDecoder::from_signatures(&[
            "transfer(address dst, uint256 wad)",
            "approve(address usr, uint256 wad)",
        ])
            .unwrap();
        let got = decoder.decode_input(&input).unwrap().unwrap();

        for (expected, got) in expected.iter().zip(got.iter()) {
            assert_eq!(expected, got, "Checking that decodes are the same");
        }
    }

    #[test]
    #[should_panic]
    fn test_decode_input_with_incorrect_signature() {
        let _function = alloy_json_abi::Function::parse("incorrect signature").unwrap();
    }
}
