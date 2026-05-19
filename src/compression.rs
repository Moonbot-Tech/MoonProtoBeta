/// SynLZ decompression — byte-exact port of mORMot SynLZdecompress1pas.
/// Source: mormot.core.base.pas:10493-10735 (C:\files\dcomp12\mormot\core\mormot.core.base.pas)
///
/// Header format VERIFIED against mORMot source (SynLZdecompressdestlen, line 10493):
///   result := PWord(in_p)^;
///   if result and $8000 <> 0 then
///     result := (result and $7fff) or (integer(PWord(in_p + 2)^) shl 15);
///
/// Wire format:
///   [0..1] output_size: u16. If bit 15 set: real_size = (word & 0x7FFF) | (next_word << 15)
///   [2..] or [4..] compressed data (control words + literals + back-refs)
///
/// Note: TAlgoSynLZ.AlgoCompress/AlgoDecompress call SynLZcompress1/SynLZdecompress1 directly.
/// NO additional 4-byte header — the u16 size prefix IS the only header.

type Offsets = [usize; 4096];

/// Maximum allowed output size for SynLZ decompression (DoS protection).
///
/// MoonProto-сообщения внутри Sliced ограничены ~384 KB (256 блоков × PMTU ≤ ~1.5 KB).
/// Сжатые прикладные пакеты — единицы MB максимум. Лимит 16 MB закрывает decompression-bomb
/// vector: scoмpromised сервер мог бы отправить заголовок `out_size = 2 GB`, что приводит
/// к мгновенному OOM-крашу (особенно на mobile). Любой превышающий лимит payload — adversarial.
pub const MAX_SYNLZ_OUTPUT: usize = 16 * 1024 * 1024;

/// Decompress SynLZ data. Returns decompressed bytes or None on error.
///
/// **Byte-exact port** `mormot.core.base.pas:10636-10717 SynLZdecompress1passub`.
///
/// Алгоритм:
/// - `last_hashed` инициализируется на позицию **перед** буфером (`dst - 1` в Delphi pointer-math,
///   `isize -1` в Rust → используем `Option<usize>` через signed sentinel).
/// - Для **литерала**: одиночный hash-update `if last_hashed < dst - 3 then inc(last_hashed); update`.
///   Hash'ируется позиция **перед** только что записанным байтом (если есть 4 байта впереди).
/// - Для **back-ref**: до копирования back-ref хэшируются позиции `< dst` (НЕ `dst + t`!), затем
///   `inc(dst, t); last_hashed := dst - 1` — скопированные t байт НЕ хэшируются в этой итерации.
pub fn synlz_decompress(src: &[u8]) -> Option<Vec<u8>> {
    if src.len() < 2 {
        return None;
    }

    // Read output size header (matches SynLZdecompress1pas:10719-10733)
    let first_word = u16::from_le_bytes([src[0], src[1]]);
    let mut pos: usize;
    if first_word == 0 {
        return Some(Vec::new());
    }
    let out_size = if first_word & 0x8000 != 0 {
        if src.len() < 4 { return None; }
        let second_word = u16::from_le_bytes([src[2], src[3]]);
        pos = 4;
        ((first_word & 0x7FFF) as usize) | ((second_word as usize) << 15)
    } else {
        pos = 2;
        first_word as usize
    };

    // DoS protection: cap decompressed size. Закрывает decompression-bomb vector
    // когда скомпрометированный сервер отправляет header с гигантским out_size.
    if out_size > MAX_SYNLZ_OUTPUT {
        log::warn!(target: "moonproto::compression",
            "synlz_decompress: out_size {} exceeds MAX_SYNLZ_OUTPUT {}, rejecting (DoS protection)",
            out_size, MAX_SYNLZ_OUTPUT);
        return None;
    }

    let mut dst = vec![0u8; out_size];
    let mut dst_pos = 0usize;
    // audit_rust_quality #2: 32 KB на стеке (`[usize; 4096]` × 8 байт) — близко к лимиту
    // вторичных потоков на iOS (~512 KB) при глубоком callstack. Heap-аллокация ~30 нс
    // на каждый вызов, незначительно на фоне самого decompress.
    let mut offset: Box<Offsets> = Box::new([0; 4096]);

    // last_hashed = dst - 1 в Delphi pointer-math (на 1 позицию ДО буфера).
    // В Rust используем i64, где -1 представляет это начальное состояние.
    let mut last_hashed: i64 = -1;

    let src_end = src.len();

    // Outer loop: read control words.
    'outer: while pos + 4 <= src_end {
        let cw = u32::from_le_bytes([src[pos], src[pos+1], src[pos+2], src[pos+3]]);
        pos += 4;
        let mut cwbit: u32 = 1;

        // Inner loop: process 32 bits of control word.
        while pos < src_end {
            if cw & cwbit == 0 {
                // === LITERAL ===
                if dst_pos >= out_size { return Some(dst); }
                dst[dst_pos] = src[pos];
                pos += 1;
                dst_pos += 1;
                if pos >= src_end { break 'outer; }

                // Update hash table (SINGLE update, not loop).
                // Delphi: `if last_hashed < dst - 3 then begin inc(last_hashed); update; end`
                // Эквивалент: last_hashed + 1 <= (dst_pos as i64) - 4, т.е. в `dst[last_hashed+1..last_hashed+5]`
                // есть валидные 4 байта.
                if last_hashed < (dst_pos as i64) - 4 {
                    last_hashed += 1;
                    let lh = last_hashed as usize;
                    if lh + 4 <= dst.len() {
                        let v = u32::from_le_bytes([dst[lh], dst[lh+1], dst[lh+2], dst[lh+3]]);
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

                // Копируем t байт (учитываем overlap — Delphi MoveByOne для overlap'а).
                if dst_pos + t > out_size {
                    // Защита от записи за границу буфера — Delphi полагается на корректность.
                    return None;
                }
                // D-V2-05 fix: malicious/corrupt SynLZ stream может выставить copy_from
                // указывающий за пределы уже декомпрессированных данных. Delphi (без bounds
                // check) делает out-of-bounds read; в Rust это panic. Отказываемся вместо
                // panic — corrupt input не должен валить long-running клиент.
                if copy_from.saturating_add(t) > dst.len() || copy_from > dst_pos {
                    return None;
                }
                if dst_pos.saturating_sub(copy_from) < t {
                    // Overlap: byte-by-byte (MoveByOne)
                    for i in 0..t {
                        dst[dst_pos + i] = dst[copy_from + i];
                    }
                } else {
                    // No overlap: можно block-copy (но в Rust нельзя dst[..].copy_within с overlap
                    // если intervals don't overlap — используем split_at_mut логику через copy_within).
                    dst.copy_within(copy_from..copy_from + t, dst_pos);
                }

                if pos >= src_end { break 'outer; }

                // Update hash table: хэшируем позиции **до** copying-target (до `dst_pos`).
                // Delphi: `if last_hashed < dst then repeat inc(last_hashed); hash; until last_hashed >= dst`.
                // dst здесь = ДО `inc(dst, t)`, т.е. = dst_pos в текущий момент.
                let target = dst_pos as i64;
                while last_hashed < target {
                    last_hashed += 1;
                    let lh = last_hashed as usize;
                    if lh + 4 <= dst.len() {
                        let v = u32::from_le_bytes([dst[lh], dst[lh+1], dst[lh+2], dst[lh+3]]);
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
        // Inner loop закончился (pos >= src_end).
        break;
    }

    // Если расшифровка не дошла до out_size — возвращаем что есть (Delphi полагается на size).
    dst.truncate(dst_pos.min(out_size));
    Some(dst)
}

/// Decompress MoonProto packet (MPDecompress).
/// MPCompressionAlgo=1 uses SynLZ. Algo 2 = raw deflate. Algo 3 = RLE+SynLZ.
/// Currently only SynLZ (algo 1) is implemented — this is what the server uses.
pub fn mp_decompress(data: &[u8]) -> Option<Vec<u8>> {
    synlz_decompress(data)
}

/// SynLZ compression — byte-exact port of SynLZcompress1pas.
/// Source: mormot.core.base.pas:10501-10633
pub fn synlz_compress(src: &[u8]) -> Vec<u8> {
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
        if size == 0 { return; }
    }

    // audit_rust_quality #2: 32+16=48 KB на стеке — слишком много для mobile вторичных потоков.
    let mut offset: Box<[usize; 4096]> = Box::new([usize::MAX; 4096]);
    let mut cache: Box<[u32; 4096]> = Box::new([0u32; 4096]);

    let srcend = size;
    let srcendmatch = if size > 11 { size - 11 } else { 0 };
    let mut src_pos: usize = 0;
    let mut cwbit: u8 = 0;

    // Reserve space for control word
    let mut cw_pos = dst.len();
    dst.extend_from_slice(&0u32.to_le_bytes());

    // Main loop
    while src_pos <= srcendmatch {
        let v = u32::from_le_bytes([src[src_pos], src[src_pos+1], src[src_pos+2], src[src_pos+3]]);
        let h = ((v >> 12) ^ v) as usize & 4095;
        let o = offset[h];
        offset[h] = src_pos;
        let cached = v ^ cache[h];
        cache[h] = v;

        if (cached & 0x00FFFFFF == 0) && o != usize::MAX && src_pos > o + 2 {
            // Back-reference: set bit in control word
            let cw = u32::from_le_bytes(dst[cw_pos..cw_pos+4].try_into().unwrap());
            dst[cw_pos..cw_pos+4].copy_from_slice(&(cw | (1u32 << cwbit)).to_le_bytes());

            src_pos += 2;
            let o_pos = o + 2;
            let mut t: usize = 1;
            // mORMot SynLZcompress1pas: `while (...) and (t < 270) and ...` — потолок 269.
            let tmax = (srcend - src_pos - 1).min(269);
            while t < tmax && o_pos + t < srcend && src[o_pos + t] == src[src_pos + t] { t += 1; }
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
            if src_pos > srcendmatch { break; }
        } else {
            // New control word
            cw_pos = dst.len();
            dst.extend_from_slice(&0u32.to_le_bytes());
            cwbit = 0;
            if src_pos > srcendmatch { break; }
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
pub fn mp_compress(data: &[u8]) -> Option<Vec<u8>> {
    if data.len() <= 64 { return None; }
    let compressed = synlz_compress(data);
    // Only use if saves > 5%
    if compressed.len() < data.len() * 95 / 100 {
        Some(compressed)
    } else {
        None
    }
}
