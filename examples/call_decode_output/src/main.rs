use alloy_json_abi::Function;
use anyhow::Context;
use hypersync_client::{
    net_types::{FieldSelection, Query, TraceField, TraceFilter},
    simple_types::Trace,
    ArrowResponseData, CallDecoder, Client, ClientConfig, FromArrow, StreamConfig,
};
use std::sync::Arc;

const BALANCE_OF_SIGNATURE: &str =
    "function balanceOf(address account) external view returns (uint256)";
const DAI_ADDRESS: &str = "0x6B175474E89094C44Da98b954EedeAC495271d0F";
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init()?;

    let client = Arc::new(
        Client::new(ClientConfig {
            max_num_retries: Some(10),
            ..ClientConfig::default()
        })
        .unwrap(),
    );

    let balance_of_sighash = Function::parse(BALANCE_OF_SIGNATURE)
        .context("parse function signature")?
        .selector()
        .to_string();

    let query = Query::new()
        .from_block(16291127) // Aave V3 deployment block
        .select_fields(FieldSelection::new().trace([TraceField::Input, TraceField::Output]))
        .where_traces([TraceFilter::any()
            .and_to_address([DAI_ADDRESS])?
            .and_sighash([balance_of_sighash])?]);

    let decoder = CallDecoder::from_signatures(&[BALANCE_OF_SIGNATURE]).unwrap();

    let config = StreamConfig {
        ..Default::default()
    };

    let mut rx = client.clone().stream_arrow(query, config).await?;

    fn convert_traces(arrow_response_data: ArrowResponseData) -> Vec<Trace> {
        arrow_response_data
            .traces
            .iter()
            .flat_map(Trace::from_arrow)
            .collect()
    }

    while let Some(result) = rx.recv().await {
        match result {
            Ok(response) => {
                println!("Received response");
                let traces = convert_traces(response.data);
                for trace in traces {
                    if let (Some(input), Some(output)) = (trace.input, trace.output) {
                        if let Some(args) = decoder
                            .decode_input(&input)
                            .context("Failed to decode input")?
                        {
                            let address = args[0].as_address().unwrap();
                            if let Some(results) = decoder
                                .decode_output(&output, BALANCE_OF_SIGNATURE)
                                .context("Failed to decode output")?
                            {
                                if !results.is_empty() {
                                    let (balance, _) = results[0].as_uint().unwrap();
                                    println!("ADDRESS {address} : {balance} DAI");
                                }
                            }
                        }
                    }
                }
            }
            Err(e) => {
                eprintln!("Error: {e:?}");
            }
        }
    }

    Ok(())
}