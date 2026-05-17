/// SynLZ decompression — byte-exact port of mORMot SynLZdecompress1pas.
/// Source: mormot.core.base.pas:10636-10735
///
/// Wire format:
///   [0..1] output_size: u16. If bit 15 set: real_size = (word & 0x7FFF) | (next_word << 15)
///   [2..] or [4..] compressed data (control words + literals + back-refs)

type Offsets = [usize; 4096];

/// Decompress SynLZ data. Returns decompressed bytes or None on error.
pub fn synlz_decompress(src: &[u8]) -> Option<Vec<u8>> {
    if src.len() < 2 {
        return None;
    }

    let mut pos = 0usize;

    // Read output size
    let first_word = u16::from_le_bytes([src[0], src[1]]);
    pos = 2;
    let out_size = if first_word & 0x8000 != 0 {
        if src.len() < 4 { return None; }
        let second_word = u16::from_le_bytes([src[2], src[3]]);
        pos = 4;
        ((first_word & 0x7FFF) as usize) | ((second_word as usize) << 15)
    } else {
        first_word as usize
    };

    if out_size == 0 {
        return Some(Vec::new());
    }

    let mut dst = vec![0u8; out_size];
    let mut dst_pos = 0usize;
    let mut offset: Offsets = [0; 4096];
    let mut last_hashed: usize = 0; // points into dst, starts at dst-1 effectively
    let mut first_hash = true;

    let src_end = src.len();

    while pos < src_end && dst_pos < out_size {
        // Read control word (32 bits)
        if pos + 4 > src_end { break; }
        let cw = u32::from_le_bytes([src[pos], src[pos+1], src[pos+2], src[pos+3]]);
        pos += 4;

        for bit in 0..32 {
            if pos >= src_end || dst_pos >= out_size {
                return Some(dst);
            }

            if cw & (1u32 << bit) == 0 {
                // Literal byte
                dst[dst_pos] = src[pos];
                pos += 1;
                dst_pos += 1;

                // Update hash table
                if !first_hash {
                    while last_hashed + 3 < dst_pos {
                        last_hashed += 1;
                        if last_hashed + 3 < dst.len() {
                            let v = u32::from_le_bytes([
                                dst[last_hashed], dst[last_hashed+1],
                                dst[last_hashed+2], dst[last_hashed+3],
                            ]);
                            let h = ((v >> 12) ^ v) as usize & 4095;
                            offset[h] = last_hashed;
                        }
                    }
                } else if dst_pos >= 4 {
                    first_hash = false;
                    last_hashed = 0;
                }
            } else {
                // Back-reference
                if pos + 2 > src_end { return Some(dst); }
                let h_word = u16::from_le_bytes([src[pos], src[pos+1]]);
                pos += 2;

                let mut t = (h_word & 15) as usize + 2;
                if t == 2 {
                    if pos >= src_end { return Some(dst); }
                    t = src[pos] as usize + 18;
                    pos += 1;
                }

                let h_idx = (h_word >> 4) as usize;
                let copy_from = offset[h_idx];

                // Copy bytes (may overlap)
                for i in 0..t {
                    if dst_pos + i >= out_size { break; }
                    dst[dst_pos + i] = dst[copy_from + i];
                }

                // Update hash table for decompressed bytes
                if !first_hash {
                    while last_hashed + 3 < dst_pos + t && last_hashed + 3 < dst.len() {
                        last_hashed += 1;
                        let v = u32::from_le_bytes([
                            dst[last_hashed], dst[last_hashed+1],
                            dst[last_hashed+2], dst[last_hashed+3],
                        ]);
                        let h = ((v >> 12) ^ v) as usize & 4095;
                        offset[h] = last_hashed;
                    }
                }

                dst_pos += t;
                if dst_pos > 3 && first_hash {
                    first_hash = false;
                    last_hashed = 0;
                }
                last_hashed = if dst_pos > 0 { dst_pos - 1 } else { 0 };
            }
        }
    }

    dst.truncate(dst_pos.min(out_size));
    Some(dst)
}

/// Decompress MoonProto packet (MPDecompress).
/// MPCompressionAlgo=1 uses SynLZ. Algo 2 = raw deflate. Algo 3 = RLE+SynLZ.
/// Currently only SynLZ (algo 1) is implemented — this is what the server uses.
pub fn mp_decompress(data: &[u8]) -> Option<Vec<u8>> {
    // SynLZ (algo 1) — default and only used in practice
    synlz_decompress(data)
}
