use alloy_json_abi::Function;
use anyhow::Context;
use hypersync_client::{
    arrow_reader::TraceReader,
    net_types::{Query, TraceField, TraceFilter},
    CallDecoder, Client, StreamConfig,
};

const BALANCE_OF_SIGNATURE: &str =
    "function balanceOf(address account) external view returns (uint256)";
const DAI_ADDRESS: &str = "0x6B175474E89094C44Da98b954EedeAC495271d0F";
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init()?;

    let client = Client::builder()
        .chain_id(1)
        .api_token(std::env::var("ENVIO_API_TOKEN")?)
        .max_num_retries(10)
        .build()
        .unwrap();

    let balance_of_sighash = Function::parse(BALANCE_OF_SIGNATURE)
        .context("parse function signature")?
        .selector()
        .to_string();

    let query = Query::new()
        .from_block(16291127) // Aave V3 deployment block
        .select_trace_fields([TraceField::Input, TraceField::Output])
        .where_traces(
            TraceFilter::all()
                .and_to([DAI_ADDRESS])?
                .and_sighash([balance_of_sighash])?,
        );

    let decoder = CallDecoder::from_signatures(&[BALANCE_OF_SIGNATURE]).unwrap();

    let config = StreamConfig {
        ..Default::default()
    };

    let mut rx = client.clone().stream_arrow(query, config).await?;

    while let Some(result) = rx.recv().await {
        match result {
            Ok(response) => {
                println!("Received response");
                for batch in response.data.traces {
                    for trace in TraceReader::iter(&batch) {
                        if let (Some(input), Some(output)) = (trace.input()?, trace.output()?) {
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
            }
            Err(e) => {
                eprintln!("Error: {e:?}");
            }
        }
    }

    Ok(())
}
