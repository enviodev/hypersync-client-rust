pub fn decode_hex(hex: &str) -> Result<Vec<u8>, faster_hex::Error> {
    let len = hex.as_bytes().len();
    let mut dst = vec![0; len / 2];

    faster_hex::hex_decode(hex.as_bytes(), &mut dst)?;

    Ok(dst)
}
