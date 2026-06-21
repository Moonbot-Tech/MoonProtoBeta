//! Apply full/diff orderbook packets into the read model.

use super::{BookKey, OrderBookKind, OrderBookLevel, OrderBookMap, OrderBookSnapshot, TopOfBook};
use crate::commands::order_book::{OrderBookUpdate, OrderLevel};
use crate::state::eps::EpsProfile;
use std::mem;
use std::sync::Arc;

pub(super) fn apply_cached_packet(
    books: &mut OrderBookMap,
    scratch: &mut Vec<OrderBookLevel>,
    key: BookKey,
    pkt: &OrderBookUpdate,
    eps: EpsProfile,
) -> TopOfBook {
    if pkt.is_full {
        apply_full_book(books, key, pkt.seq, &pkt.buys, &pkt.sells)
    } else {
        apply_diff_book(books, scratch, key, pkt.seq, &pkt.buys, &pkt.sells, eps)
    }
}

pub(super) fn apply_full_book(
    books: &mut OrderBookMap,
    key: BookKey,
    seq: u16,
    buys: &[OrderLevel],
    sells: &[OrderLevel],
) -> TopOfBook {
    let book = order_book_entry_arc(books, key);
    let mut book = book.write_arc();
    book.mark_applied(seq);
    book.buys.clear();
    book.buys
        .extend(buys.iter().copied().map(OrderBookLevel::from));
    book.sells.clear();
    book.sells
        .extend(sells.iter().copied().map(OrderBookLevel::from));
    book.top()
}

pub(super) fn apply_diff_book(
    books: &mut OrderBookMap,
    scratch: &mut Vec<OrderBookLevel>,
    key: BookKey,
    seq: u16,
    buy_diff: &[OrderLevel],
    sell_diff: &[OrderLevel],
    eps: EpsProfile,
) -> TopOfBook {
    let book = order_book_entry_arc(books, key);
    let mut book = book.write_arc();
    apply_order_book_diff_keep_zero_with_eps(
        &mut book.buys,
        scratch,
        buy_diff,
        sell_diff,
        true,
        eps,
    );
    apply_order_book_diff_keep_zero_with_eps(
        &mut book.sells,
        scratch,
        sell_diff,
        buy_diff,
        false,
        eps,
    );
    book.mark_applied(seq);
    book.top()
}

fn order_book_entry_arc(
    books: &mut OrderBookMap,
    key: BookKey,
) -> Arc<parking_lot::RwLock<OrderBookSnapshot>> {
    Arc::clone(books.entry(key).or_insert_with(|| {
        Arc::new(parking_lot::RwLock::new(OrderBookSnapshot {
            market_index: key.0,
            kind: OrderBookKind::from_u8(key.1).unwrap_or(OrderBookKind::Futures),
            #[cfg(any(test, feature = "diagnostics"))]
            seq: 0,
            revision: 0,
            buys: Vec::new(),
            sells: Vec::new(),
        }))
    }))
}

#[cfg(test)]
pub(crate) fn apply_order_book_diff_keep_zero(
    book: &mut Vec<OrderBookLevel>,
    scratch: &mut Vec<OrderBookLevel>,
    diff: &[OrderLevel],
    shrink: &[OrderLevel],
    is_buy_book: bool,
) {
    apply_order_book_diff_keep_zero_with_eps(
        book,
        scratch,
        diff,
        shrink,
        is_buy_book,
        EpsProfile::BINANCE,
    );
}

pub(crate) fn apply_order_book_diff_keep_zero_with_eps(
    book: &mut Vec<OrderBookLevel>,
    scratch: &mut Vec<OrderBookLevel>,
    diff: &[OrderLevel],
    shrink: &[OrderLevel],
    is_buy_book: bool,
    eps: EpsProfile,
) {
    if diff.is_empty() {
        return;
    }

    scratch.clear();
    mem::swap(book, scratch);
    book.reserve(scratch.len() + diff.len());
    let mut k = 0usize;

    for diff_level in diff {
        let diff_rate = diff_level.rate as f64;

        if is_buy_book {
            while k < scratch.len() && scratch[k].rate > diff_rate + eps.eps_m {
                book.push(scratch[k]);
                k += 1;
            }
        } else {
            while k < scratch.len() && scratch[k].rate < diff_rate - eps.eps_m {
                book.push(scratch[k]);
                k += 1;
            }
        }

        if (diff_level.quantity as f64) > eps.eps {
            book.push((*diff_level).into());
        }

        if k < scratch.len() && (scratch[k].rate - diff_rate).abs() < eps.eps_m {
            k += 1;
        }
    }

    while k < scratch.len() {
        book.push(scratch[k]);
        k += 1;
    }

    let mut cut_price = -1.0;
    for level in shrink {
        let rate = level.rate as f64;
        if rate > eps.eps_m {
            cut_price = rate;
            break;
        }
    }

    if cut_price > 0.0 {
        let mut cut = 0usize;
        if is_buy_book {
            while cut < book.len() && book[cut].rate >= cut_price {
                cut += 1;
            }
        } else {
            while cut < book.len() && book[cut].rate <= cut_price {
                cut += 1;
            }
        }
        if cut > 0 {
            book.drain(0..cut);
        }
    }
}
