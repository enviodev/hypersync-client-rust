use hypersync_net_types::Query;
use schemars::schema_for;

fn main() {
    let schema = schema_for!(Query);
    println!("{}", serde_json::to_string_pretty(&schema).unwrap());
}
