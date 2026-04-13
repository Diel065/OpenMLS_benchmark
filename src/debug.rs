pub fn hex_prefix(bytes: &[u8], n: usize) -> String {
    bytes.iter()
        .take(n)
        .map(|b| format!("{:02x}", b))
        .collect::<Vec<_>>()
        .join(" ")
}

pub fn print_bytes(label: &str, bytes: &[u8]) {
    println!(
        "[DBG] {} | len={} | first bytes={}",
        label,
        bytes.len(),
        hex_prefix(bytes, 16)
    );
}