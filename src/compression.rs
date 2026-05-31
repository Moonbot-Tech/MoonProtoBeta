//! SynLZ decompression — byte-exact port of mORMot SynLZdecompress1pas.
//! Source: mORMot `mormot.core.base.pas` SynLZ implementation.
//!
//! Header format VERIFIED against mORMot source (SynLZdecompressdestlen, line 10493):
//!   result := PWord(in_p)^;
//!   if result and $8000 <> 0 then
//!     result := (result and $7fff) or (integer(PWord(in_p + 2)^) shl 15);
//!
//! Wire format:
//!   [0..1] output_size: u16. If bit 15 set: real_size = (word & 0x7FFF) | (next_word << 15)
//!   [2..] or [4..] compressed data (control words + literals + back-refs)
//!
//! Note: TAlgoSynLZ.AlgoCompress/AlgoDecompress call SynLZcompress1/SynLZdecompress1 directly.
//! NO additional 4-byte header — the u16 size prefix IS the only header.

type Offsets = [usize; 4096];

// A compressed MoonProto command still travels as one direct or Sliced command.
// Sliced block numbers are u8 and PMTU is u16, so even the theoretical maximum
// envelope is under 16 MiB. Real bulk data (5m candles) is chunked well below
// that; larger SynLZ destlen values are malformed input, not useful payload.
pub(crate) const MAX_DECOMPRESSED_SIZE: usize = ((u16::MAX as usize) - 15 - 4) * 256 - 12 - 1;

// Thread-local scratch buffer for SynLZ decompress (32 KB = `[usize; 4096]` × 8 bytes).
//
// Previously: `Box::new([0; 4096])` per call (~30 ns alloc + ~10 ns free).
// At peak TradesStream/OrderBook ~50K decompress/sec that is ~2 ms/sec of pure CPU
// on alloc/dealloc + allocator pressure. Thread-local: alloc once per thread
// on the first call, zero alloc afterwards.
//
// Important: the offset scratch must be reset before each decompress. Live
// OrderBook packets can reference a hash slot before it is written within the
// current call; a persistent value from the previous packet turns such a
// back-reference into a false Corrupt. In Rust the thread-local buffer must behave
// like fresh scratch for every `SynLZdecompress1pas`, so we reset it to 0.
//
// No recursion: `synlz_decompress` never calls itself. RefCell guarantees
// safety if someone violates this invariant (try_borrow_mut returns Err → fallback to its own alloc).
thread_local! {
    static DECOMPRESS_OFFSETS: std::cell::RefCell<Box<Offsets>> =
        std::cell::RefCell::new(Box::new([0usize; 4096]));
}

// Thread-local scratch for SynLZ compress: offset (32 KB) + cache (16 KB) = 48 KB.
// Likewise: alloc once, reused. cache requires a reset to `0` for the
// algorithm to work correctly (used as `v ^ cache[h]`); offset is initialized to
// `usize::MAX` (sentinel for "no entry yet").
thread_local! {
    static COMPRESS_OFFSETS: std::cell::RefCell<Box<[usize; 4096]>> =
        std::cell::RefCell::new(Box::new([usize::MAX; 4096]));
    static COMPRESS_CACHE: std::cell::RefCell<Box<[u32; 4096]>> =
        std::cell::RefCell::new(Box::new([0u32; 4096]));
}

/// Decompress SynLZ data. Returns decompressed bytes or None on error.
///
/// **Byte-exact port** `mormot.core.base.pas:10636-10717 SynLZdecompress1passub`.
///
/// Algorithm:
/// - `last_hashed` is initialized to the position **before** the buffer (`dst - 1` in Delphi pointer-math,
///   `isize -1` in Rust → use `Option<usize>` via a signed sentinel).
/// - For a **literal**: single hash-update `if last_hashed < dst - 3 then inc(last_hashed); update`.
///   This is exactly the Delphi pointer rule: after writing a literal, `dst` already points to the next
///   byte, so the position `<= dst - 3` is hashed. mORMot may read one byte
///   "ahead" from the already-allocated output buffer; shifting to `dst - 4` changes the hash table
///   and breaks valid live `OrderBook` streams.
/// - For a **back-ref**: before copying, the back-ref hashes positions `< dst` (NOT `dst + t`!), then
///   `inc(dst, t); last_hashed := dst - 1` — the copied t bytes are NOT hashed in this iteration.
pub(crate) fn synlz_decompress(src: &[u8]) -> Option<Vec<u8>> {
    if src.len() < 2 {
        return None;
    }

    // Read output size header (matches SynLZdecompress1pas:10719-10733)
    let first_word = u16::from_le_bytes([src[0], src[1]]);
    let pos: usize;
    if first_word == 0 {
        return Some(Vec::new());
    }
    let out_size = if first_word & 0x8000 != 0 {
        if src.len() < 4 {
            return None;
        }
        let second_word = u16::from_le_bytes([src[2], src[3]]);
        pos = 4;
        ((first_word & 0x7FFF) as usize) | ((second_word as usize) << 15)
    } else {
        pos = 2;
        first_word as usize
    };

    if out_size > MAX_DECOMPRESSED_SIZE {
        return None;
    }

    let mut dst = Vec::new();
    dst.try_reserve_exact(out_size).ok()?;
    dst.resize(out_size, 0);

    // Use the thread-local scratch buffer for offsets (32 KB). Reset is mandatory:
    // Delphi creates clean scratch for each decode, and a stale offset can
    // change the malformed-stream effect and byte-exact reproducibility.
    let result = DECOMPRESS_OFFSETS.with(|cell| {
        match cell.try_borrow_mut() {
            Ok(mut guard) => {
                guard.fill(0);
                synlz_decompress_inner(src, &mut dst, &mut guard, pos, out_size)
            }
            Err(_) => {
                // Recursion — impossible by invariant, but if someone violates the contract,
                // fall back to its own alloc.
                let mut fallback: Box<Offsets> = Box::new([0usize; 4096]);
                synlz_decompress_inner(src, &mut dst, &mut fallback, pos, out_size)
            }
        }
    });

    match result {
        DecompressResult::Ok(final_pos) => {
            dst.truncate(final_pos.min(out_size));
            Some(dst)
        }
        DecompressResult::Corrupt => None,
    }
}

enum DecompressResult {
    Ok(usize), // final dst_pos
    Corrupt,
}

/// Internal implementation — isolates the thread_local borrow from `?` early returns.
fn synlz_decompress_inner(
    src: &[u8],
    dst: &mut [u8],
    offset: &mut Offsets,
    mut pos: usize,
    out_size: usize,
) -> DecompressResult {
    let mut dst_pos = 0usize;
    // last_hashed = dst - 1 in Delphi pointer-math (1 position BEFORE the buffer).
    // In Rust we use i64, where -1 represents this initial state.
    let mut last_hashed: i64 = -1;

    let src_end = src.len();

    // Outer loop: read control words.
    'outer: while pos + 4 <= src_end {
        let cw = u32::from_le_bytes([src[pos], src[pos + 1], src[pos + 2], src[pos + 3]]);
        pos += 4;
        let mut cwbit: u32 = 1;

        // Inner loop: process 32 bits of control word.
        while pos < src_end {
            if cw & cwbit == 0 {
                // === LITERAL ===
                if dst_pos >= out_size {
                    return DecompressResult::Ok(dst_pos);
                }
                dst[dst_pos] = src[pos];
                pos += 1;
                dst_pos += 1;
                if pos >= src_end {
                    break 'outer;
                }

                // Update hash table (SINGLE update, not loop).
                // Delphi: `if last_hashed < dst - 3 then begin inc(last_hashed); update; end`.
                // Equivalent after `dst_pos += 1`: `last_hashed < dst_pos - 3`.
                if last_hashed < (dst_pos as i64) - 3 {
                    last_hashed += 1;
                    let lh = last_hashed as usize;
                    if lh + 4 <= dst.len() {
                        let v =
                            u32::from_le_bytes([dst[lh], dst[lh + 1], dst[lh + 2], dst[lh + 3]]);
                        let h = ((v >> 12) ^ v) as usize & 4095;
                        offset[h] = lh;
                    }
                }

                cwbit <<= 1;
                if cwbit == 0 {
                    continue 'outer;
                }
            } else {
                // === BACK-REFERENCE ===
                if pos + 2 > src_end {
                    return DecompressResult::Ok(dst_pos);
                }
                let h_word = u16::from_le_bytes([src[pos], src[pos + 1]]);
                pos += 2;

                let mut t = (h_word & 15) as usize + 2;
                if t == 2 {
                    if pos >= src_end {
                        return DecompressResult::Ok(dst_pos);
                    }
                    t = src[pos] as usize + 18;
                    pos += 1;
                }

                let h_idx = (h_word >> 4) as usize;
                let copy_from = offset[h_idx];

                // Copy t bytes (accounting for overlap — Delphi MoveByOne for overlap).
                if dst_pos + t > out_size {
                    // Guard against writing past the buffer boundary — Delphi relies on correctness.
                    return DecompressResult::Corrupt;
                }
                // D-V2-05 fix: a malicious/corrupt SynLZ stream can set copy_from
                // pointing past the already-decompressed data. Delphi (without a bounds
                // check) does an out-of-bounds read; in Rust that is a panic. We refuse instead of
                // panicking — corrupt input must not crash a long-running client.
                if copy_from.saturating_add(t) > dst.len() || copy_from > dst_pos {
                    return DecompressResult::Corrupt;
                }
                if dst_pos.saturating_sub(copy_from) < t {
                    // Overlap: byte-by-byte (MoveByOne)
                    for i in 0..t {
                        dst[dst_pos + i] = dst[copy_from + i];
                    }
                } else {
                    // No overlap: copy_within works.
                    dst.copy_within(copy_from..copy_from + t, dst_pos);
                }

                if pos >= src_end {
                    break 'outer;
                }

                // Update hash table: hash positions **up to** the copying-target (up to `dst_pos`).
                // Delphi: `if last_hashed < dst then repeat inc(last_hashed); hash; until last_hashed >= dst`.
                let target = dst_pos as i64;
                while last_hashed < target {
                    last_hashed += 1;
                    let lh = last_hashed as usize;
                    if lh + 4 <= dst.len() {
                        let v =
                            u32::from_le_bytes([dst[lh], dst[lh + 1], dst[lh + 2], dst[lh + 3]]);
                        let h = ((v >> 12) ^ v) as usize & 4095;
                        offset[h] = lh;
                    }
                }

                dst_pos += t;
                last_hashed = (dst_pos as i64) - 1;

                cwbit <<= 1;
                if cwbit == 0 {
                    continue 'outer;
                }
            }
        }
        // Inner loop ended (pos >= src_end).
        break;
    }

    DecompressResult::Ok(dst_pos)
}

/// Decompress MoonProto packet (MPDecompress).
/// MPCompressionAlgo=1 uses SynLZ. Algo 2 = raw deflate. Algo 3 = RLE+SynLZ.
/// Currently only SynLZ (algo 1) is implemented — this is what the server uses.
pub(crate) fn mp_decompress(data: &[u8]) -> Option<Vec<u8>> {
    synlz_decompress(data)
}

/// SynLZ compression — byte-exact port of SynLZcompress1pas.
/// Source: mormot.core.base.pas:10501-10633
pub(crate) fn synlz_compress(src: &[u8]) -> Vec<u8> {
    let mut dst = Vec::with_capacity(src.len() + src.len() / 8 + 32);
    synlz_compress_impl(src, &mut dst);
    dst
}

fn synlz_compress_impl(src: &[u8], dst: &mut Vec<u8>) {
    let size = src.len();

    // Header
    if size >= 0x8000 {
        dst.extend_from_slice(&((0x8000u16 | (size as u16 & 0x7FFF)).to_le_bytes()));
        dst.extend_from_slice(&((size >> 15) as u16).to_le_bytes());
    } else {
        dst.extend_from_slice(&(size as u16).to_le_bytes());
        if size == 0 {
            return;
        }
    }

    // Thread-local scratch — 32 KB offset + 16 KB cache. **cache requires a reset**
    // (used as `v ^ cache[h]` to decide whether it is a repeat); offset
    // is also reset to `usize::MAX` — sentinel for "no entry under this hash".
    // Without the reset the result would be wire-incompatible with a fresh compress.
    COMPRESS_OFFSETS.with(|off_cell| {
        COMPRESS_CACHE.with(|cache_cell| {
            let mut offset = off_cell.try_borrow_mut().map(Some).unwrap_or(None);
            let mut cache = cache_cell.try_borrow_mut().map(Some).unwrap_or(None);

            // Fall back to its own alloc if try_borrow_mut failed (recursion — should not happen).
            let mut fallback_off: Box<[usize; 4096]> = Box::new([usize::MAX; 4096]);
            let mut fallback_cache: Box<[u32; 4096]> = Box::new([0u32; 4096]);

            let off_ref: &mut [usize; 4096] = match offset.as_mut() {
                Some(g) => {
                    // Reset thread-local to its initial state.
                    g.fill(usize::MAX);
                    g
                }
                None => &mut fallback_off,
            };
            let cache_ref: &mut [u32; 4096] = match cache.as_mut() {
                Some(g) => {
                    g.fill(0);
                    g
                }
                None => &mut fallback_cache,
            };

            synlz_compress_inner(src, dst, off_ref, cache_ref);
        });
    });
}

/// Internal compress implementation — isolates the thread_local borrow.
fn synlz_compress_inner(
    src: &[u8],
    dst: &mut Vec<u8>,
    offset: &mut [usize; 4096],
    cache: &mut [u32; 4096],
) {
    let size = src.len();
    let srcend = size;
    let srcendmatch = size.saturating_sub(11);
    let mut src_pos: usize = 0;
    let mut cwbit: u8 = 0;

    // Reserve space for control word
    let mut cw_pos = dst.len();
    dst.extend_from_slice(&0u32.to_le_bytes());

    // Main loop
    while src_pos <= srcendmatch {
        let v = u32::from_le_bytes([
            src[src_pos],
            src[src_pos + 1],
            src[src_pos + 2],
            src[src_pos + 3],
        ]);
        let h = ((v >> 12) ^ v) as usize & 4095;
        let o = offset[h];
        offset[h] = src_pos;
        let cached = v ^ cache[h];
        cache[h] = v;

        if (cached & 0x00FFFFFF == 0) && o != usize::MAX && src_pos > o + 2 {
            // Back-reference: set bit in control word
            let cw = u32::from_le_bytes(dst[cw_pos..cw_pos + 4].try_into().unwrap());
            dst[cw_pos..cw_pos + 4].copy_from_slice(&(cw | (1u32 << cwbit)).to_le_bytes());

            src_pos += 2;
            let o_pos = o + 2;
            let mut t: usize = 1;
            // mORMot `SynLZcompress1pas` (mormot.core.base.pas:10557-10562), base = src+2
            // (same `inc(src,2)` before the cap as here): `tmax := srcend-src-1;
            // if tmax >= (255+16) then tmax := (255+16); while (o[t]=src[t]) and (t<tmax) do inc(t)`.
            // So `t` runs up to `min(remaining, 255+16) = 271`; encoded length byte `t-16` ∈ 0..255.
            let tmax = (srcend - src_pos - 1).min(255 + 16);
            while t < tmax && o_pos + t < srcend && src[o_pos + t] == src[src_pos + t] {
                t += 1;
            }
            src_pos += t;

            let h_shifted = (h as u16) << 4;
            if t <= 15 {
                dst.extend_from_slice(&(t as u16 | h_shifted).to_le_bytes());
            } else {
                dst.extend_from_slice(&h_shifted.to_le_bytes());
                dst.push((t - 16) as u8);
            }
        } else {
            // Literal byte
            dst.push(src[src_pos]);
            src_pos += 1;
        }

        if cwbit < 31 {
            cwbit += 1;
            if src_pos > srcendmatch {
                break;
            }
        } else {
            // New control word
            cw_pos = dst.len();
            dst.extend_from_slice(&0u32.to_le_bytes());
            cwbit = 0;
            if src_pos > srcendmatch {
                break;
            }
        }
    }

    // Remaining bytes (literals)
    while src_pos < srcend {
        dst.push(src[src_pos]);
        src_pos += 1;
        if cwbit < 31 {
            cwbit += 1;
        } else {
            dst.extend_from_slice(&0u32.to_le_bytes());
            cwbit = 0;
        }
    }
}

/// Compress for MoonProto (MPCompress).
/// Returns compressed data and size, or None if compression doesn't help (< 5% savings).
/// Matches MoonProtoDataStruct.pas:283-316 (MPCompressionAlgo=1 = SynLZ).
pub(crate) fn mp_compress(data: &[u8]) -> Option<Vec<u8>> {
    if data.len() <= 64 {
        return None;
    }
    let compressed = synlz_compress(data);
    // Only use if saves > 5%
    if compressed.len() < data.len() * 95 / 100 {
        Some(compressed)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hex_to_bytes(s: &str) -> Vec<u8> {
        assert!(s.len().is_multiple_of(2));
        let mut out = Vec::with_capacity(s.len() / 2);
        let bytes = s.as_bytes();
        for i in (0..bytes.len()).step_by(2) {
            let hi = (bytes[i] as char).to_digit(16).unwrap();
            let lo = (bytes[i + 1] as char).to_digit(16).unwrap();
            out.push(((hi << 4) | lo) as u8);
        }
        out
    }

    #[test]
    fn synlz_decompress_resets_thread_local_offsets() {
        let live_orderbook_payload = hex_to_bytes(
            "3f001000000001000300013084759647a037953d801b88475873803b00f87147faedeb3ae13e97800000004700000000464134791f4597476f12833b2145974700000000",
        );

        DECOMPRESS_OFFSETS.with(|cell| {
            let mut offsets = cell.borrow_mut();
            offsets[768] = usize::MAX;
            offsets[1939] = usize::MAX;
        });

        let decoded = synlz_decompress(&live_orderbook_payload)
            .expect("decompress must not depend on stale thread-local offsets");
        assert_eq!(
            decoded,
            hex_to_bytes(
                "0100030000030084759647a037953d801b88475873803b00f87147faedeb3ae13e97470000000046419747000000001f4597476f12833b2145974700000000"
            ),
            "literal hash update must match mORMot `last_hashed < dst - 3`, not only output length"
        );
    }

    #[test]
    fn synlz_decompress_accepts_protocol_payload_above_one_mib() {
        let plain = vec![0x5a; 1024 * 1024 + 1];
        let compressed = synlz_compress(&plain);

        let decoded = synlz_decompress(&compressed)
            .expect("SynLZ cap must not be the old arbitrary 1 MiB limit");

        assert_eq!(decoded, plain);
    }

    #[test]
    fn synlz_decompress_rejects_declared_size_above_protocol_cap() {
        let bomb_header = [0xFF, 0xFF, 0xFF, 0xFF];
        assert!(synlz_decompress(&bomb_header).is_none());
    }
}
