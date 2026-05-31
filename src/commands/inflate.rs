use std::io::{self, Read};

const INFLATE_CHUNK_SIZE: usize = 64 * 1024;

/// Read a DEFLATE/Zlib decoder into a Vec using fallible growth.
///
/// MoonProto has legitimate large plaintext API blobs (full candles, strategy
/// snapshots), so this helper deliberately does not impose a Rust-only size cap.
/// The important hygiene property is simpler: inflater output growth is explicit
/// and fallible, so malformed streams fail the parser instead of relying on an
/// implicit allocation path.
pub(crate) fn read_inflate_to_vec<R: Read>(
    reader: &mut R,
    capacity_hint: usize,
) -> io::Result<Vec<u8>> {
    let mut out = Vec::new();
    if capacity_hint > 0 {
        out.try_reserve(capacity_hint).map_err(alloc_error)?;
    }

    let mut buf = [0u8; INFLATE_CHUNK_SIZE];
    loop {
        let n = reader.read(&mut buf)?;
        if n == 0 {
            return Ok(out);
        }
        out.try_reserve(n).map_err(alloc_error)?;
        out.extend_from_slice(&buf[..n]);
    }
}

fn alloc_error(err: std::collections::TryReserveError) -> io::Error {
    io::Error::new(
        io::ErrorKind::Other,
        format!("inflate output allocation failed: {err}"),
    )
}
