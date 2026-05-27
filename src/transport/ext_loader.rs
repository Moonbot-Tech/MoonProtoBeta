/// Dynamic loader for moonext library (extended transport modes 1/2).
/// If not found — mode 0 only, no error.
///
/// Open source code does NOT know what moonext does internally.
/// It just calls wrap/unwrap and sends whatever moonext tells it to send.
///
/// ## FFI ABI
///
/// The closed-source moonext library uses this extended ABI:
///
/// - `moon_ext_wrap(buf: *mut u8, len: u32, cap: u32, mode: u8, is_server: u8,
///                   extra_buf: *mut u8, extra_len: *mut u32) -> u32`
///   - `cap` — buffer capacity for in-place growth.
///   - `is_server` — direction flag (1=server-side, 0=client-side).
///   - `extra_buf/extra_len` — output: optional service packet to send before main
///     (e.g. STUN binding request, fake DNS query, etc.).
///   - Returns new length of `buf` after wrapping.
///
/// - `moon_ext_unwrap(buf: *mut u8, len: u32, mode: u8) -> u32`
///   - Returns 0 if packet should be ignored (e.g. service packet response),
///     otherwise new length after unwrapping in-place.
///
/// - `moon_ext_is_service` — NOT exported by current moonext. The "is service"
///   determination is done by `moon_ext_unwrap` returning 0.
///
/// For V0 protocol (mask_ver=0), moonext is NOT loaded.
use libloading::Library;
use std::sync::OnceLock;

#[cfg(target_os = "windows")]
const EXT_LIB_NAME: &str = "moonext.dll";
#[cfg(target_os = "linux")]
const EXT_LIB_NAME: &str = "libmoonext.so";
#[cfg(target_os = "macos")]
const EXT_LIB_NAME: &str = "libmoonext.dylib";
#[cfg(target_os = "android")]
const EXT_LIB_NAME: &str = "libmoonext.so";
// iOS / *BSD / other — dynamic loading недоступно (iOS статически линкует .a),
// EXT_LIB_NAME не определён → load_ext всегда вернёт None через
// `#[cfg(not(any(...)))]` ниже. Это закрывает C-01: iOS compile breakage.
#[cfg(not(any(
    target_os = "windows",
    target_os = "linux",
    target_os = "macos",
    target_os = "android"
)))]
const EXT_LIB_NAME: &str = "moonext-unavailable-on-this-platform";

// C ABI: wrap may produce an additional packet to send
type WrapFn = unsafe extern "C" fn(*mut u8, u32, u32, u8, u8, *mut u8, *mut u32) -> u32;
// C ABI: unwrap returns 0 if packet should be ignored
type UnwrapFn = unsafe extern "C" fn(*mut u8, u32, u8) -> u32;

struct ExtLib {
    _lib: Library,
    wrap_fn: WrapFn,
    unwrap_fn: UnwrapFn,
}

static EXT: OnceLock<Option<ExtLib>> = OnceLock::new();

fn load_ext() -> Option<ExtLib> {
    // На неподдерживаемых платформах (iOS / *BSD) dynamic loading недоступно:
    // никогда не пытаемся, EXT_LIB_NAME — placeholder.
    #[cfg(not(any(
        target_os = "windows",
        target_os = "linux",
        target_os = "macos",
        target_os = "android"
    )))]
    {
        return None;
    }

    let paths = [EXT_LIB_NAME.to_string(), format!("ext/{}", EXT_LIB_NAME)];

    for path in &paths {
        if let Ok(lib) = unsafe { Library::new(path) } {
            let wrap_fn = unsafe { lib.get::<WrapFn>(b"moon_ext_wrap\0").ok()?.into_raw() };
            let unwrap_fn = unsafe { lib.get::<UnwrapFn>(b"moon_ext_unwrap\0").ok()?.into_raw() };
            return Some(ExtLib {
                _lib: lib,
                wrap_fn: *wrap_fn,
                unwrap_fn: *unwrap_fn,
            });
        }
    }
    None
}

fn get_ext() -> Option<&'static ExtLib> {
    EXT.get_or_init(load_ext).as_ref()
}

/// Check if extended transport library is loaded.
pub fn is_available() -> bool {
    get_ext().is_some()
}

/// Wrap outgoing packet via extended transport.
/// Returns: (success, optional_extra_packet_to_send)
/// The extra packet is something moonext needs sent — caller doesn't need to know what it is.
pub fn ext_wrap(buf: &mut Vec<u8>, mode: u8, is_server: bool) -> (bool, Option<Vec<u8>>) {
    let ext = match get_ext() {
        Some(e) => e,
        None => return (false, None),
    };

    buf.reserve(24);
    let cap = buf.capacity() as u32;
    let len = buf.len() as u32;

    // D-V3-01 fix: ранее `extra_buf` был стэковым [u8; 32]. Если closed-source moonext
    // запишет больше 32 байт через `extra_buf.as_mut_ptr()` — STACK BUFFER OVERFLOW
    // (запись произойдёт *до* того как мы успеем проверить extra_len) → UB →
    // potential RCE при скомпрометированном moonext.dll.
    //
    // Защита: heap-buffer с заведомо большим размером (256 байт — запас на будущее
    // развитие ABI без перекомпиляции). FFI ABI moonext'а не передаёт capacity, так
    // что мы полагаемся на инвариант: moonext НЕ должен писать > EXTRA_BUF_SIZE.
    // Если запишет — overflow попадёт в Vec capacity (рядом heap-аллокация), что
    // не катастрофа уровня stack-smash + поймается ASAN. После — явная проверка
    // отбрасывает пакет.
    //
    // **TODO** для следующей версии moonext: добавить `extra_cap: u32` параметр
    // в FFI signature чтобы убрать инвариант "trust moonext won't overflow".
    const EXTRA_BUF_SIZE: usize = 256;
    let mut extra_buf = vec![0u8; EXTRA_BUF_SIZE];
    let mut extra_len: u32 = 0;

    let new_len = unsafe {
        (ext.wrap_fn)(
            buf.as_mut_ptr(),
            len,
            cap,
            mode,
            is_server as u8,
            extra_buf.as_mut_ptr(),
            &mut extra_len,
        )
    };

    if new_len == 0 {
        return (false, None);
    }
    // D-V2-03 fix: closed-source moonext = untrusted FFI. Если он вернёт new_len > cap,
    // `set_len(new_len)` создаст Vec с длиной за пределами allocation = UB при последующем
    // доступе. Проверяем границу и отказываемся вместо UB.
    let new_len_usize = new_len as usize;
    if new_len_usize > cap as usize {
        log::error!(target: "moonproto::transport",
            "moonext wrap returned new_len={} > cap={} — отказ, packet не отправлен", new_len_usize, cap);
        return (false, None);
    }
    unsafe {
        buf.set_len(new_len_usize);
    }

    // D-V3-01 fix: ловим overflow extra_buf'а (если произошёл — Rust panicнул бы
    // на slice access, но overflow уже на heap = UB). Логируем + отказ.
    let extra_len_usize = extra_len as usize;
    if extra_len_usize > EXTRA_BUF_SIZE {
        log::error!(target: "moonproto::transport",
            "moonext returned extra_len={} > EXTRA_BUF_SIZE={} — packet rejected, POSSIBLE BUFFER OVERFLOW",
            extra_len_usize, EXTRA_BUF_SIZE);
        return (false, None);
    }

    let extra = if extra_len > 0 {
        // Только тот префикс который moonext реально записал — обрезаем capacity.
        extra_buf.truncate(extra_len_usize);
        Some(extra_buf)
    } else {
        None
    };

    (true, extra)
}

/// Unwrap incoming packet via extended transport.
/// Returns Some(data) if valid, None if packet should be ignored.
pub fn ext_unwrap(data: &[u8], mode: u8) -> Option<Vec<u8>> {
    let ext = get_ext()?;
    let mut buf = data.to_vec();
    let len = buf.len() as u32;
    let new_len = unsafe { (ext.unwrap_fn)(buf.as_mut_ptr(), len, mode) };
    if new_len == 0 {
        return None;
    } // moonext says: ignore this packet
      // Симметрия с ext_wrap: closed-source moonext = untrusted FFI. По контракту
      // unwrap может только урезать (не расширять). Если вернул new_len > len —
      // означает что moonext уже **записал за пределы** аллокации (`data.to_vec()` имеет
      // capacity == len) = UB уже произошло. Отбрасываем пакет + лог.
    let new_len_usize = new_len as usize;
    if new_len_usize > buf.len() {
        log::error!(target: "moonproto::transport",
            "moonext unwrap returned new_len={} > input len={} — packet rejected (POSSIBLE BUFFER OVERFLOW)",
            new_len_usize, buf.len());
        return None;
    }
    buf.truncate(new_len_usize);
    Some(buf)
}
