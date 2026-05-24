use polling::{Event, Events, Poller};
use std::io;
use std::net::UdpSocket;
use std::time::{Duration, Instant};

fn wait_for_readable(
    poller: &Poller,
    events: &mut Events,
    key: usize,
    timeout: Duration,
) -> io::Result<()> {
    let deadline = Instant::now() + timeout;
    loop {
        events.clear();
        let now = Instant::now();
        if now >= deadline {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "poller did not report UDP readable event",
            ));
        }
        poller.wait(events, Some(deadline.saturating_duration_since(now)))?;
        if events.iter().any(|ev| ev.key == key && ev.readable) {
            return Ok(());
        }
    }
}

fn drain_udp(sock: &UdpSocket) -> io::Result<Vec<Vec<u8>>> {
    let mut out = Vec::new();
    let mut buf = [0u8; 2048];
    loop {
        match sock.recv_from(&mut buf) {
            Ok((n, _)) => out.push(buf[..n].to_vec()),
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => return Ok(out),
            Err(e) => return Err(e),
        }
    }
}

#[test]
fn cross_platform_udp_poller_drains_to_wouldblock_and_survives_rebind() -> io::Result<()> {
    let peer = UdpSocket::bind("127.0.0.1:0")?;
    let sock = UdpSocket::bind("127.0.0.1:0")?;
    sock.set_nonblocking(true)?;
    let addr = sock.local_addr()?;

    let poller = Poller::new()?;
    let mut events = Events::new();

    // Safety: the socket is deleted from this poller before it is dropped.
    unsafe {
        poller.add(&sock, Event::readable(1))?;
    }

    events.clear();
    poller.wait(&mut events, Some(Duration::from_millis(5)))?;
    assert!(
        events.iter().all(|ev| ev.key != 1 || !ev.readable),
        "fresh UDP socket must not be reported as readable before packets arrive",
    );

    for i in 0..3u8 {
        peer.send_to(&[i], addr)?;
    }
    wait_for_readable(&poller, &mut events, 1, Duration::from_secs(1))?;

    let drained = drain_udp(&sock)?;
    assert_eq!(
        drained,
        vec![vec![0], vec![1], vec![2]],
        "readable event must allow recv drain until WouldBlock without toggling socket options",
    );

    poller.modify(&sock, Event::readable(1))?;
    events.clear();
    poller.wait(&mut events, Some(Duration::from_millis(5)))?;
    assert!(
        events.iter().all(|ev| ev.key != 1 || !ev.readable),
        "after drain-to-WouldBlock and rearm, socket must go quiet again",
    );

    poller.delete(&sock)?;
    drop(sock);

    let rebound = UdpSocket::bind("127.0.0.1:0")?;
    rebound.set_nonblocking(true)?;
    let rebound_addr = rebound.local_addr()?;
    // Safety: the rebound socket is deleted from this poller before it is dropped.
    unsafe {
        poller.add(&rebound, Event::readable(2))?;
    }

    peer.send_to(&[9], rebound_addr)?;
    wait_for_readable(&poller, &mut events, 2, Duration::from_secs(1))?;
    assert_eq!(drain_udp(&rebound)?, vec![vec![9]]);

    poller.delete(&rebound)?;
    Ok(())
}
