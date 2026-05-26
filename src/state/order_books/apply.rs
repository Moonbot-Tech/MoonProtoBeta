//! Apply full/diff orderbook packets into the read model.

use super::{BookKey, OrderBookLevel, OrderBookSnapshot, EPS, EPS_M};
use crate::commands::order_book::{OrderBookUpdate, OrderLevel};
use std::collections::HashMap;

pub(super) fn apply_cached_packet(
    books: &mut HashMap<BookKey, OrderBookSnapshot>,
    scratch: &mut Vec<OrderBookLevel>,
    key: BookKey,
    pkt: &OrderBookUpdate,
) {
    if pkt.is_full {
        apply_full_book(books, key, pkt.seq, &pkt.buys, &pkt.sells);
    } else {
        apply_diff_book(books, scratch, key, pkt.seq, &pkt.buys, &pkt.sells);
    }
}

pub(super) fn apply_full_book(
    books: &mut HashMap<BookKey, OrderBookSnapshot>,
    key: BookKey,
    seq: u16,
    buys: &[OrderLevel],
    sells: &[OrderLevel],
) {
    let book = books.entry(key).or_insert_with(|| OrderBookSnapshot {
        market_index: key.0,
        book_kind: key.1,
        seq: 0,
        buys: Vec::new(),
        sells: Vec::new(),
    });
    book.seq = seq;
    book.buys.clear();
    book.buys
        .extend(buys.iter().copied().map(OrderBookLevel::from));
    book.sells.clear();
    book.sells
        .extend(sells.iter().copied().map(OrderBookLevel::from));
}

pub(super) fn apply_diff_book(
    books: &mut HashMap<BookKey, OrderBookSnapshot>,
    scratch: &mut Vec<OrderBookLevel>,
    key: BookKey,
    seq: u16,
    buy_diff: &[OrderLevel],
    sell_diff: &[OrderLevel],
) {
    let book = books.entry(key).or_insert_with(|| OrderBookSnapshot {
        market_index: key.0,
        book_kind: key.1,
        seq: 0,
        buys: Vec::new(),
        sells: Vec::new(),
    });
    apply_order_book_diff_keep_zero(&mut book.buys, scratch, buy_diff, sell_diff, true);
    apply_order_book_diff_keep_zero(&mut book.sells, scratch, sell_diff, buy_diff, false);
    book.seq = seq;
}

pub(crate) fn apply_order_book_diff_keep_zero(
    book: &mut Vec<OrderBookLevel>,
    scratch: &mut Vec<OrderBookLevel>,
    diff: &[OrderLevel],
    shrink: &[OrderLevel],
    is_buy_book: bool,
) {
    if diff.is_empty() {
        return;
    }

    scratch.clear();
    scratch.extend_from_slice(book);
    book.clear();
    book.reserve(scratch.len() + diff.len());
    let mut k = 0usize;

    for diff_level in diff {
        let diff_rate = diff_level.rate as f64;

        if is_buy_book {
            while k < scratch.len() && scratch[k].rate > diff_rate + EPS_M {
                book.push(scratch[k]);
                k += 1;
            }
        } else {
            while k < scratch.len() && scratch[k].rate < diff_rate - EPS_M {
                book.push(scratch[k]);
                k += 1;
            }
        }

        if (diff_level.quantity as f64) > EPS {
            book.push((*diff_level).into());
        }

        if k < scratch.len() && (scratch[k].rate - diff_rate).abs() < EPS_M {
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
        if rate > EPS_M {
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
