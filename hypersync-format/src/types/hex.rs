use crate::Result;

pub trait Hex: Sized {
    fn encode_hex(&self) -> String;
    fn decode_hex(hex: &str) -> Result<Self>;

    fn encode_hex_with_quotes(&self) -> String {
        format!("\"{}\"", self.encode_hex())
    }
}
