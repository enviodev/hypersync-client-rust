pub fn decode_hex(hex: &str) -> Result<Vec<u8>, faster_hex::Error> {
    let len = hex.len();
    let mut dst = vec![0; len / 2];

    faster_hex::hex_decode(hex.as_bytes(), &mut dst)?;

    Ok(dst)
}

/// Normalize to canonical form by removing leading zero bytes
pub fn canonicalize_bytes(bytes: Vec<u8>) -> Vec<u8> {
    if bytes.len() > 1 && bytes[0] == 0 {
        // Find first non-zero byte
        let first_non_zero = bytes
            .iter()
            .position(|&b| b != 0)
            .unwrap_or(bytes.len() - 1);
        bytes[first_non_zero..].to_vec()
    } else {
        bytes
    }
}
