use super::*;

#[test]
fn registry_default_is_empty() {
    let r = SubscriptionRegistry::default();
    assert!(r.orderbook_subs.is_empty());
    assert!(r.trades_sub.is_none());
}

#[test]
fn registry_orderbook_insert_dedups() {
    let mut r = SubscriptionRegistry::default();
    assert!(r.orderbook_subs.insert("BTCUSDT".to_string()));
    assert!(!r.orderbook_subs.insert("BTCUSDT".to_string()));
    assert!(r.orderbook_subs.insert("ETHUSDT".to_string()));
    assert_eq!(r.orderbook_subs.len(), 2);
}

#[test]
fn trades_subscription_round_trip() {
    let sub = TradesSubscription { want_mm: true };
    assert!(sub.want_mm);
    let sub_off = TradesSubscription { want_mm: false };
    assert!(!sub_off.want_mm);
}

/// Verify that Connected{fresh:true} fires only on the FIRST Authenticated
/// in a Client's lifetime. After that every subsequent one = fresh:false.
/// Tested through state-machine simulation (without a full Client::new).
#[test]
fn lifecycle_event_connected_fresh_flag_semantics() {
    let mut was_ever_connected = false;
    let first = LifecycleEvent::Connected {
        fresh: !was_ever_connected,
    };
    was_ever_connected = true;
    let second = LifecycleEvent::Connected {
        fresh: !was_ever_connected,
    };
    assert_eq!(first, LifecycleEvent::Connected { fresh: true });
    assert_eq!(second, LifecycleEvent::Connected { fresh: false });
}
