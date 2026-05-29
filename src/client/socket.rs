//! UDP socket options used by the MoonProto client.

use std::net::UdpSocket;

/// Установить SO_RCVBUF + SO_SNDBUF в 8 MB через socket2 (cross-platform).
/// Закрывает ARCH §30 ("UDP buffer sizes — должны быть существенно больше sysctl-defaults").
/// На пиковой нагрузке (~50K packets/sec) маленький ядерный буфер → silent drop.
/// D-07 + D-08: ошибки больше не игнорируются — логируем как warn (OS может отказать,
/// например Linux без `net.core.rmem_max ≥ 8MB` молча обрежет до настройки sysctl).
pub(crate) fn set_socket_buffers(sock: &UdpSocket) {
    let sock2 = socket2::SockRef::from(sock);
    if let Err(e) = sock2.set_recv_buffer_size(8 * 1024 * 1024) {
        log::warn!("SO_RCVBUF=8MB rejected by OS (probably net.core.rmem_max too small): {e}");
    }
    if let Err(e) = sock2.set_send_buffer_size(8 * 1024 * 1024) {
        log::warn!("SO_SNDBUF=8MB rejected by OS: {e}");
    }
}

/// Cross-platform IP_DONTFRAGMENT / IP_MTU_DISCOVER / IP_DONTFRAG.
/// Закрывает ARCH §20 (PMTU discovery должен работать на всех платформах, не только Windows).
/// Без этого SizeAck/ProbeMTUAck отправляются с разрешённой фрагментацией → измерение PMTU
/// становится ложным → клиент выбирает неоптимальный PMTU → каскадные retransmit'ы.
///
/// IPv4 vs IPv6: option name на IPv6 socket'е другой — `IP_DONTFRAGMENT` (v4) НЕ работает
/// на AF_INET6, нужен `IPV6_DONTFRAG` (или `IPV6_MTU_DISCOVER` на Linux). Без этого dual-stack
/// клиент (Android/iOS) silently failед бы PMTU detection. См. rust_quality audit #5.
///
/// Return value setsockopt проверяется и при ошибке логируется warn (раньше silently
/// ignored — fingerprinting'у проблемы было не оставлено следов).
pub(crate) fn set_dont_fragment_for_socket(sock: &UdpSocket, enable: bool) {
    // Определяем IPv6 vs IPv4 по local address. Если local_addr вернул ошибку — fallback на IPv4
    // semantics (большая часть систем — IPv4 по умолчанию).
    let is_v6 = sock.local_addr().map(|a| a.is_ipv6()).unwrap_or(false);

    #[cfg(target_os = "windows")]
    {
        use std::os::windows::io::AsRawSocket;
        let raw = sock.as_raw_socket();
        let val: i32 = if enable { 1 } else { 0 };
        // IPPROTO_IP=0, IP_DONTFRAGMENT=14; IPPROTO_IPV6=41, IPV6_DONTFRAG=14 (Win 10+ same value).
        let (level, optname) = if is_v6 { (41, 14) } else { (0, 14) };
        let rc = unsafe {
            extern "system" {
                fn setsockopt(
                    s: usize,
                    level: i32,
                    optname: i32,
                    optval: *const i8,
                    optlen: i32,
                ) -> i32;
            }
            setsockopt(
                raw as usize,
                level,
                optname,
                &val as *const i32 as *const i8,
                4,
            )
        };
        if rc != 0 {
            log::warn!(target: "moonproto::client",
                "set_dont_fragment_for_socket: setsockopt(level={level}, optname={optname}, v6={is_v6}) failed rc={rc} (Windows); PMTU discovery may be inaccurate");
        }
    }
    #[cfg(any(target_os = "linux", target_os = "android"))]
    {
        use std::os::fd::AsRawFd;
        let fd = sock.as_raw_fd();
        // IPv4: IPPROTO_IP=0, IP_MTU_DISCOVER=10.
        // IPv6: IPPROTO_IPV6=41, IPV6_MTU_DISCOVER=23.
        //
        // Linux `IP_PMTUDISC_DO` (2) sets DF, but also rejects datagrams above
        // the already cached route PMTU before they leave the host. MoonProto's
        // SizeAck/ProbeMTUAck packets are the PMTU probe itself: they must be
        // sent with DF while bypassing the cached path-MTU estimate, otherwise
        // the server never sees candidate sizes above Linux's stale/fallback
        // cache. `IP_PMTUDISC_PROBE` (3) is the Linux mode for that exact
        // packetization model; disabling returns to `IP_PMTUDISC_DONT` (0),
        // matching Delphi's "DF only around probe ack" behavior.
        let val: i32 = if enable { 3 } else { 0 };
        let (level, optname) = if is_v6 { (41, 23) } else { (0, 10) };
        let rc = unsafe {
            extern "C" {
                fn setsockopt(
                    s: i32,
                    level: i32,
                    optname: i32,
                    optval: *const i8,
                    optlen: u32,
                ) -> i32;
            }
            setsockopt(fd, level, optname, &val as *const i32 as *const i8, 4)
        };
        if rc != 0 {
            log::warn!(target: "moonproto::client",
                "set_dont_fragment_for_socket: setsockopt(level={level}, optname={optname}, v6={is_v6}) failed rc={rc} (Linux/Android); PMTU discovery may be inaccurate");
        }
    }
    #[cfg(any(target_os = "macos", target_os = "ios"))]
    {
        use std::os::fd::AsRawFd;
        let fd = sock.as_raw_fd();
        // IPv4: IPPROTO_IP=0, IP_DONTFRAG=28
        // IPv6: IPPROTO_IPV6=41, IPV6_DONTFRAG=62
        let val: i32 = if enable { 1 } else { 0 };
        let (level, optname) = if is_v6 { (41, 62) } else { (0, 28) };
        let rc = unsafe {
            extern "C" {
                fn setsockopt(
                    s: i32,
                    level: i32,
                    optname: i32,
                    optval: *const i8,
                    optlen: u32,
                ) -> i32;
            }
            setsockopt(fd, level, optname, &val as *const i32 as *const i8, 4)
        };
        if rc != 0 {
            log::warn!(target: "moonproto::client",
                "set_dont_fragment_for_socket: setsockopt(level={level}, optname={optname}, v6={is_v6}) failed rc={rc} (macOS/iOS); PMTU discovery may be inaccurate");
        }
    }
    #[cfg(not(any(
        target_os = "windows",
        target_os = "linux",
        target_os = "android",
        target_os = "macos",
        target_os = "ios"
    )))]
    {
        // Other platforms (BSD, etc.) — no-op для безопасности, PMTU discovery не работает.
        let _ = (sock, enable, is_v6);
    }
}
