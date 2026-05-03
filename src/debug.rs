pub fn hex_prefix(bytes: &[u8], n: usize) -> String {
    bytes
        .iter()
        .take(n)
        .map(|b| format!("{:02x}", b))
        .collect::<Vec<_>>()
        .join(" ")
}

pub fn debug_logs_enabled() -> bool {
    std::env::var("MLS_DEBUG_LOGS").ok().as_deref() == Some("1")
}

pub fn print_bytes(label: &str, bytes: &[u8]) {
    if !debug_logs_enabled() {
        return;
    }

    println!(
        "[DBG] {} | len={} | first bytes={}",
        label,
        bytes.len(),
        hex_prefix(bytes, 16)
    );
}
