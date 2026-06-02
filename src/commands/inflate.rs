use std::io::{self, Read};

const INFLATE_CHUNK_SIZE: usize = 64 * 1024;
pub(crate) const MAX_INFLATE_OUTPUT_SIZE: usize = 512 * 1024 * 1024;

/// Read a DEFLATE/Zlib decoder into a Vec using fallible growth.
///
/// MoonProto has legitimate large plaintext API blobs (full candles, strategy
/// snapshots), so the cap is a large process fuse, not a tiny packet-envelope
/// cap. It keeps a malformed DEFLATE stream below OOM while valid bulk blobs
/// stay comfortably inside the same 512 MiB Delphi limit.
pub(crate) fn read_inflate_to_vec<R: Read>(
    reader: &mut R,
    capacity_hint: usize,
    max_output: usize,
) -> io::Result<Vec<u8>> {
    let mut out = Vec::new();
    if capacity_hint > 0 {
        out.try_reserve(capacity_hint.min(max_output))
            .map_err(alloc_error)?;
    }

    let mut buf = [0u8; INFLATE_CHUNK_SIZE];
    loop {
        let n = reader.read(&mut buf)?;
        if n == 0 {
            return Ok(out);
        }
        if out.len().checked_add(n).is_none_or(|len| len > max_output) {
            return Err(over_cap_error(max_output));
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

fn over_cap_error(max_output: usize) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!("inflate output exceeds {max_output} byte cap"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use flate2::{read::DeflateDecoder, write::DeflateEncoder, Compression};
    use std::io::Write;

    fn deflate(data: &[u8]) -> Vec<u8> {
        let mut encoder = DeflateEncoder::new(Vec::new(), Compression::best());
        encoder.write_all(data).unwrap();
        encoder.finish().unwrap()
    }

    #[test]
    fn read_inflate_to_vec_allows_output_inside_cap() {
        let plain = b"hello bounded inflate".repeat(128);
        let compressed = deflate(&plain);
        let mut decoder = DeflateDecoder::new(compressed.as_slice());

        let out = read_inflate_to_vec(&mut decoder, compressed.len(), plain.len()).unwrap();
        assert_eq!(out, plain);
    }

    #[test]
    fn read_inflate_to_vec_rejects_output_above_cap() {
        let plain = vec![0u8; 128 * 1024];
        let compressed = deflate(&plain);
        let mut decoder = DeflateDecoder::new(compressed.as_slice());

        let err = read_inflate_to_vec(&mut decoder, usize::MAX, plain.len() - 1).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }
}
