//! Canonical order state and `MPC_Order` commands 41..47 for protocol v4.
//!
//! The core publishes one immutable order description plus a 342-byte state
//! split into 13 independently revisioned sections. This module owns that
//! exact wire shape; the public order read model is materialized later.

use super::{BaseCommandHeader, StopSettings};
use crate::commands::registry::{decode_utf8_delphi, CURRENT_PROTO_CMD_VER};
use crc32c::crc32c_append;
use std::sync::atomic::{AtomicU64, Ordering};

pub(crate) const ORDER_STATE_SIZE: usize = 342;
pub(crate) const ORDER_SECTION_COUNT: usize = 13;
pub(crate) const ORDER_SECTION_ALL_MASK: u16 = 0x1fff;
pub(crate) const ORDER_RECONCILE_MASK: u16 =
    (1 << OSEC_FLAGS) | (1 << OSEC_STOPS) | (1 << OSEC_VSTOP) | (1 << OSEC_PLANNED);
pub(crate) const ORDER_STATE_HASH_SEED: u32 = 0x4f72_6432;
pub(crate) const ORDER_DESC_NAME_MAX: usize = 64;

pub(crate) const OSEC_PHASE: usize = 0;
pub(crate) const OSEC_FLAGS: usize = 1;
pub(crate) const OSEC_BUY_TARGET: usize = 2;
pub(crate) const OSEC_SELL_TARGET: usize = 3;
pub(crate) const OSEC_BUY_EXEC: usize = 4;
pub(crate) const OSEC_BUY_PLACEMENT: usize = 5;
pub(crate) const OSEC_BUY_SLOW: usize = 6;
pub(crate) const OSEC_SELL_EXEC: usize = 7;
pub(crate) const OSEC_SELL_PLACEMENT: usize = 8;
pub(crate) const OSEC_SELL_SLOW: usize = 9;
pub(crate) const OSEC_STOPS: usize = 10;
pub(crate) const OSEC_VSTOP: usize = 11;
pub(crate) const OSEC_PLANNED: usize = 12;

pub(crate) const ORDER_SECTION_OFFSET: [usize; ORDER_SECTION_COUNT] =
    [0, 9, 11, 28, 37, 70, 133, 153, 186, 249, 269, 315, 333];
pub(crate) const ORDER_SECTION_SIZE: [usize; ORDER_SECTION_COUNT] =
    [9, 2, 17, 9, 33, 63, 20, 33, 63, 20, 46, 18, 9];

pub(crate) const OFL_IMMUNE: u8 = 1;
pub(crate) const OFL_PANIC_ON: u8 = 2;
pub(crate) const OFL_PANIC_AUTO: u8 = 4;
pub(crate) const ODF_EMULATOR: u8 = 1;
pub(crate) const ODF_IS_SHORT: u8 = 2;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct OrderDescription {
    name_len: u8,
    name: [u8; ORDER_DESC_NAME_MAX],
    flags: u8,
}

impl Default for OrderDescription {
    fn default() -> Self {
        Self {
            name_len: 0,
            name: [0; ORDER_DESC_NAME_MAX],
            flags: 0,
        }
    }
}

impl OrderDescription {
    pub(crate) fn market_name(&self) -> String {
        decode_utf8_delphi(&self.name[..self.name_len as usize])
    }

    pub(crate) fn emulator(&self) -> bool {
        self.flags & ODF_EMULATOR != 0
    }

    pub(crate) fn is_short(&self) -> bool {
        self.flags & ODF_IS_SHORT != 0
    }

    pub(crate) fn wire_bytes(&self, out: &mut Vec<u8>) {
        out.push(self.name_len);
        out.extend_from_slice(&self.name[..self.name_len as usize]);
        out.push(self.flags);
    }

    fn read(input: &mut &[u8]) -> Option<Self> {
        let (&name_len, rest) = input.split_first()?;
        if name_len as usize > ORDER_DESC_NAME_MAX || rest.len() < name_len as usize {
            return None;
        }
        let mut desc = Self::default();
        desc.name_len = name_len;
        desc.name[..name_len as usize].copy_from_slice(&rest[..name_len as usize]);
        let tail = &rest[name_len as usize..];
        if let Some((&flags, tail)) = tail.split_first() {
            desc.flags = flags;
            *input = tail;
        } else {
            // Delphi does not check the final Flags read result.
            *input = tail;
        }
        Some(desc)
    }

    #[cfg(test)]
    pub(crate) fn for_test(market: &str, emulator: bool, is_short: bool) -> Self {
        let bytes = market.as_bytes();
        assert!(!bytes.is_empty() && bytes.len() <= ORDER_DESC_NAME_MAX);
        let mut desc = Self::default();
        desc.name_len = bytes.len() as u8;
        desc.name[..bytes.len()].copy_from_slice(bytes);
        if emulator {
            desc.flags |= ODF_EMULATOR;
        }
        if is_short {
            desc.flags |= ODF_IS_SHORT;
        }
        desc
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CanonicalOrderState(pub(crate) [u8; ORDER_STATE_SIZE]);

impl Default for CanonicalOrderState {
    fn default() -> Self {
        Self([0; ORDER_STATE_SIZE])
    }
}

impl CanonicalOrderState {
    pub(crate) fn section(&self, section: usize) -> &[u8] {
        let start = ORDER_SECTION_OFFSET[section];
        &self.0[start..start + ORDER_SECTION_SIZE[section]]
    }

    pub(crate) fn section_mut(&mut self, section: usize) -> &mut [u8] {
        let start = ORDER_SECTION_OFFSET[section];
        &mut self.0[start..start + ORDER_SECTION_SIZE[section]]
    }

    pub(crate) fn copy_section_from(&mut self, source: &Self, section: usize) {
        self.section_mut(section)
            .copy_from_slice(source.section(section));
    }

    pub(crate) fn status_byte(&self) -> u8 {
        self.0[0]
    }

    pub(crate) fn is_terminal(&self) -> bool {
        matches!(self.status_byte(), 1 | 3 | 5 | 7 | 8)
    }

    pub(crate) fn read_u8(&self, offset: usize) -> u8 {
        self.0[offset]
    }

    pub(crate) fn read_i64(&self, offset: usize) -> i64 {
        i64::from_le_bytes(self.0[offset..offset + 8].try_into().unwrap())
    }

    pub(crate) fn read_u64(&self, offset: usize) -> u64 {
        u64::from_le_bytes(self.0[offset..offset + 8].try_into().unwrap())
    }

    pub(crate) fn read_f64(&self, offset: usize) -> f64 {
        f64::from_le_bytes(self.0[offset..offset + 8].try_into().unwrap())
    }

    pub(crate) fn read_f32(&self, offset: usize) -> f32 {
        f32::from_le_bytes(self.0[offset..offset + 4].try_into().unwrap())
    }

    pub(crate) fn stops(&self) -> StopSettings {
        let mut bytes = self.section(OSEC_STOPS);
        StopSettings::read_from_delphi_stream(&mut bytes)
    }

    fn write_sections(&self, mask: u16, out: &mut Vec<u8>) {
        for section in 0..ORDER_SECTION_COUNT {
            if mask & (1 << section) != 0 {
                out.extend_from_slice(self.section(section));
            }
        }
    }

    fn read_sections(&mut self, mask: u16, input: &mut &[u8]) -> bool {
        for section in 0..ORDER_SECTION_COUNT {
            if mask & (1 << section) == 0 {
                continue;
            }
            let size = ORDER_SECTION_SIZE[section];
            let copied = size.min(input.len());
            self.section_mut(section)[..copied].copy_from_slice(&input[..copied]);
            *input = &input[copied..];
            if copied != size {
                return false;
            }
        }
        true
    }
}

pub(crate) fn state_hash(
    revision: u64,
    desc: &OrderDescription,
    state: &CanonicalOrderState,
) -> u32 {
    let mut crc = crc32c_append(ORDER_STATE_HASH_SEED, &revision.to_le_bytes());
    crc = crc32c_append(crc, &[desc.name_len]);
    crc = crc32c_append(crc, &desc.name[..desc.name_len as usize]);
    crc = crc32c_append(crc, &[desc.flags]);
    crc32c_append(crc, &state.0)
}

pub(crate) fn write_uleb(mut value: u64, out: &mut Vec<u8>) {
    loop {
        let mut byte = (value & 0x7f) as u8;
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
        }
        out.push(byte);
        if value == 0 {
            return;
        }
    }
}

pub(crate) fn read_uleb(input: &mut &[u8]) -> Option<u64> {
    let original = *input;
    let mut value = 0u64;
    let mut shift = 0;
    for (index, &byte) in original.iter().take(10).enumerate() {
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            *input = &original[index + 1..];
            return Some(value);
        }
        shift += 7;
    }
    None
}

#[derive(Debug, Clone)]
pub(crate) struct OrderImage {
    pub header: BaseCommandHeader,
    pub state_rev: u64,
    pub desc: OrderDescription,
    pub section_mask: u16,
    pub state: CanonicalOrderState,
}

impl OrderImage {
    pub(crate) fn read(input: &mut &[u8]) -> Option<Self> {
        let header = BaseCommandHeader::read(input)?;
        let mut image = Self {
            header,
            state_rev: 0,
            desc: OrderDescription::default(),
            section_mask: 0,
            state: CanonicalOrderState::default(),
        };
        let Some(state_rev) = read_uleb(input) else {
            return Some(image);
        };
        image.state_rev = state_rev;
        let Some(desc) = OrderDescription::read(input) else {
            return Some(image);
        };
        image.desc = desc;
        image.section_mask = read_u16_zero_tail(input);
        let _ = image.state.read_sections(image.section_mask, input);
        Some(image)
    }

    #[cfg(test)]
    pub(crate) fn write(&self, out: &mut Vec<u8>) {
        self.header.write(out);
        write_uleb(self.state_rev, out);
        self.desc.wire_bytes(out);
        out.extend_from_slice(&self.section_mask.to_le_bytes());
        self.state.write_sections(self.section_mask, out);
    }
}

#[derive(Debug, Clone)]
pub(crate) struct OrderPatch {
    pub header: BaseCommandHeader,
    pub state_rev: u64,
    pub state_hash: u32,
    pub section_mask: u16,
    pub state: CanonicalOrderState,
}

impl OrderPatch {
    pub(crate) fn read(input: &mut &[u8]) -> Option<Self> {
        let header = BaseCommandHeader::read(input)?;
        let mut patch = Self {
            header,
            state_rev: 0,
            state_hash: 0,
            section_mask: 0,
            state: CanonicalOrderState::default(),
        };
        let Some(state_rev) = read_uleb(input) else {
            return Some(patch);
        };
        patch.state_rev = state_rev;
        patch.state_hash = read_u32_zero_tail(input);
        patch.section_mask = read_u16_zero_tail(input);
        let _ = patch.state.read_sections(patch.section_mask, input);
        Some(patch)
    }

    #[cfg(test)]
    pub(crate) fn write(&self, out: &mut Vec<u8>) {
        self.header.write(out);
        write_uleb(self.state_rev, out);
        out.extend_from_slice(&self.state_hash.to_le_bytes());
        out.extend_from_slice(&self.section_mask.to_le_bytes());
        self.state.write_sections(self.section_mask, out);
    }
}

#[derive(Debug, Clone)]
pub(crate) struct OrderImageRecord {
    pub order_id: u64,
    pub state_rev: u64,
    pub desc: OrderDescription,
    pub section_mask: u16,
    pub state: CanonicalOrderState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct OrderCatalogRecord {
    pub order_id: u64,
    pub state_rev: u64,
}

#[derive(Debug, Clone)]
pub(crate) struct OrdersSnapshot {
    pub header: BaseCommandHeader,
    pub from_uid: u64,
    pub range_end_uid: u64,
    pub records: Vec<OrderImageRecord>,
}

impl OrdersSnapshot {
    pub(crate) fn read(input: &mut &[u8]) -> Option<Self> {
        let header = BaseCommandHeader::read(input)?;
        let from_uid = read_u64_zero_tail(input);
        let range_end_uid = read_u64_zero_tail(input);
        let mut records = Vec::new();
        while !input.is_empty() {
            let before = *input;
            let Some(order_id) = read_u64_exact(input) else {
                break;
            };
            let Some(state_rev) = read_uleb(input) else {
                *input = &[];
                break;
            };
            let Some(desc) = OrderDescription::read(input) else {
                *input = &[];
                break;
            };
            let Some(section_mask) = read_u16_exact(input) else {
                *input = &[];
                break;
            };
            let mut state = CanonicalOrderState::default();
            if !state.read_sections(section_mask, input) {
                *input = &[];
                break;
            }
            records.push(OrderImageRecord {
                order_id,
                state_rev,
                desc,
                section_mask,
                state,
            });
            debug_assert!(input.len() < before.len());
        }
        Some(Self {
            header,
            from_uid,
            range_end_uid,
            records,
        })
    }
}

#[derive(Debug, Clone)]
pub(crate) struct OrdersCatalog {
    pub header: BaseCommandHeader,
    pub from_uid: u64,
    pub range_end_uid: u64,
    pub records: Vec<OrderCatalogRecord>,
}

impl OrdersCatalog {
    pub(crate) fn read(input: &mut &[u8]) -> Option<Self> {
        let header = BaseCommandHeader::read(input)?;
        let from_uid = read_u64_zero_tail(input);
        let range_end_uid = read_u64_zero_tail(input);
        let mut records = Vec::new();
        while !input.is_empty() {
            let Some(order_id) = read_u64_exact(input) else {
                break;
            };
            let Some(state_rev) = read_uleb(input) else {
                *input = &[];
                break;
            };
            records.push(OrderCatalogRecord {
                order_id,
                state_rev,
            });
        }
        Some(Self {
            header,
            from_uid,
            range_end_uid,
            records,
        })
    }
}

#[derive(Debug, Clone)]
pub(crate) struct OrderStatusRequest {
    pub header: BaseCommandHeader,
    pub exact_rev: u64,
}

impl OrderStatusRequest {
    pub(crate) fn read(input: &mut &[u8]) -> Option<Self> {
        let header = BaseCommandHeader::read(input)?;
        let exact_rev = read_uleb(input).unwrap_or(0);
        Some(Self { header, exact_rev })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OrderCommandGroup {
    Buy,
    Sell,
    Stops,
    VStop,
    Panic,
    Immune,
}

impl OrderCommandGroup {
    pub(crate) const fn unique_kind(self) -> u8 {
        use crate::commands::registry::*;
        match self {
            Self::Buy => UK_ORDER_CMD_BUY,
            Self::Sell => UK_ORDER_CMD_SELL,
            Self::Stops => UK_ORDER_CMD_STOPS,
            Self::VStop => UK_ORDER_CMD_VSTOP,
            Self::Panic => UK_ORDER_CMD_PANIC,
            Self::Immune => UK_ORDER_CMD_IMMUNE,
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) enum OrderCommandPayload {
    TargetBuy {
        order_id: u64,
        price: f64,
        size: f64,
    },
    TargetSell {
        order_id: u64,
        price: f64,
    },
    Stops {
        order_id: u64,
        stops: StopSettings,
    },
    VStop {
        order_id: u64,
        enabled: bool,
        fixed: bool,
        level: f64,
        volume: f64,
    },
    Panic {
        order_id: u64,
        enabled: bool,
    },
    Immune {
        order_id: u64,
        enabled: bool,
    },
    CancelBuy {
        order_id: u64,
    },
    CancelSell {
        order_id: u64,
    },
    PendingCancel {
        order_id: u64,
    },
    Start {
        market_name: String,
        is_short: bool,
        use_market_stop: bool,
        strategy_id: u64,
        size: f64,
        price: f64,
        planned_sell_price: f64,
    },
    MoveAllKind {
        market_name: String,
        leg: u8,
        move_kind: u8,
        side: u8,
        price: f64,
    },
    MoveAllZone {
        market_name: String,
        side: u8,
        min_price: f64,
        max_price: f64,
    },
    MoveAllPercent {
        market_name: String,
        leg: u8,
        percent: f64,
    },
    Join {
        market_name: String,
        is_short: bool,
    },
    SplitOrder {
        order_id: u64,
        parts: i32,
        split_small: bool,
        split_small_sell: bool,
    },
    ClosePosition {
        market_name: String,
        mode: u8,
        flag: bool,
    },
    ManualSell {
        market_name: String,
        price: f64,
        size: f64,
    },
    PanicSellAll,
    Unknown(u8),
}

impl OrderCommandPayload {
    pub(crate) const fn opcode(&self) -> u8 {
        match self {
            Self::TargetBuy { .. } => 0,
            Self::TargetSell { .. } => 1,
            Self::Stops { .. } => 3,
            Self::VStop { .. } => 4,
            Self::Panic { .. } => 5,
            Self::Immune { .. } => 6,
            Self::CancelBuy { .. } => 7,
            Self::CancelSell { .. } => 8,
            Self::PendingCancel { .. } => 9,
            Self::Start { .. } => 10,
            Self::MoveAllKind { .. } | Self::MoveAllZone { .. } | Self::MoveAllPercent { .. } => 11,
            Self::Join { .. } => 12,
            Self::SplitOrder { .. } => 13,
            Self::ClosePosition { .. } => 14,
            Self::ManualSell { .. } => 15,
            Self::PanicSellAll => 17,
            Self::Unknown(opcode) => *opcode,
        }
    }

    pub(crate) const fn group(&self) -> Option<OrderCommandGroup> {
        match self {
            Self::TargetBuy { .. } | Self::CancelBuy { .. } | Self::PendingCancel { .. } => {
                Some(OrderCommandGroup::Buy)
            }
            Self::TargetSell { .. } | Self::CancelSell { .. } => Some(OrderCommandGroup::Sell),
            Self::Stops { .. } => Some(OrderCommandGroup::Stops),
            Self::VStop { .. } => Some(OrderCommandGroup::VStop),
            Self::Panic { .. } => Some(OrderCommandGroup::Panic),
            Self::Immune { .. } => Some(OrderCommandGroup::Immune),
            _ => None,
        }
    }

    pub(crate) const fn order_id(&self) -> Option<u64> {
        match self {
            Self::TargetBuy { order_id, .. }
            | Self::TargetSell { order_id, .. }
            | Self::Stops { order_id, .. }
            | Self::VStop { order_id, .. }
            | Self::Panic { order_id, .. }
            | Self::Immune { order_id, .. }
            | Self::CancelBuy { order_id }
            | Self::CancelSell { order_id }
            | Self::PendingCancel { order_id }
            | Self::SplitOrder { order_id, .. } => Some(*order_id),
            _ => None,
        }
    }

    pub(crate) const fn is_move_all(&self) -> bool {
        matches!(
            self,
            Self::MoveAllKind { .. } | Self::MoveAllZone { .. } | Self::MoveAllPercent { .. }
        )
    }

    fn write(&self, out: &mut Vec<u8>) {
        out.push(self.opcode());
        match self {
            Self::TargetBuy {
                order_id,
                price,
                size,
            } => {
                out.extend_from_slice(&order_id.to_le_bytes());
                out.extend_from_slice(&price.to_le_bytes());
                out.extend_from_slice(&size.to_le_bytes());
            }
            Self::TargetSell { order_id, price } => {
                out.extend_from_slice(&order_id.to_le_bytes());
                out.extend_from_slice(&price.to_le_bytes());
            }
            Self::Stops { order_id, stops } => {
                out.extend_from_slice(&order_id.to_le_bytes());
                stops.write_to(out);
            }
            Self::VStop {
                order_id,
                enabled,
                fixed,
                level,
                volume,
            } => {
                out.extend_from_slice(&order_id.to_le_bytes());
                out.push(u8::from(*enabled));
                out.push(u8::from(*fixed));
                out.extend_from_slice(&level.to_le_bytes());
                out.extend_from_slice(&volume.to_le_bytes());
            }
            Self::Panic { order_id, enabled } | Self::Immune { order_id, enabled } => {
                out.extend_from_slice(&order_id.to_le_bytes());
                out.push(u8::from(*enabled));
            }
            Self::CancelBuy { order_id }
            | Self::CancelSell { order_id }
            | Self::PendingCancel { order_id } => out.extend_from_slice(&order_id.to_le_bytes()),
            Self::Start {
                market_name,
                is_short,
                use_market_stop,
                strategy_id,
                size,
                price,
                planned_sell_price,
            } => {
                write_short_string(out, market_name);
                out.push(u8::from(*is_short) | (u8::from(*use_market_stop) << 1));
                out.extend_from_slice(&strategy_id.to_le_bytes());
                out.extend_from_slice(&size.to_le_bytes());
                out.extend_from_slice(&price.to_le_bytes());
                out.extend_from_slice(&planned_sell_price.to_le_bytes());
            }
            Self::MoveAllKind {
                market_name,
                leg,
                move_kind,
                side,
                price,
            } => {
                write_short_string(out, market_name);
                out.extend_from_slice(&[*leg, 0, *move_kind, *side]);
                out.extend_from_slice(&price.to_le_bytes());
            }
            Self::MoveAllZone {
                market_name,
                side,
                min_price,
                max_price,
            } => {
                write_short_string(out, market_name);
                out.extend_from_slice(&[0, 1, *side]);
                out.extend_from_slice(&min_price.to_le_bytes());
                out.extend_from_slice(&max_price.to_le_bytes());
            }
            Self::MoveAllPercent {
                market_name,
                leg,
                percent,
            } => {
                write_short_string(out, market_name);
                out.extend_from_slice(&[*leg, 2]);
                out.extend_from_slice(&percent.to_le_bytes());
            }
            Self::Join {
                market_name,
                is_short,
            } => {
                write_short_string(out, market_name);
                out.push(u8::from(*is_short));
            }
            Self::SplitOrder {
                order_id,
                parts,
                split_small,
                split_small_sell,
            } => {
                out.extend_from_slice(&order_id.to_le_bytes());
                out.extend_from_slice(&parts.to_le_bytes());
                out.push(u8::from(*split_small) | (u8::from(*split_small_sell) << 1));
            }
            Self::ClosePosition {
                market_name,
                mode,
                flag,
            } => {
                write_short_string(out, market_name);
                out.extend_from_slice(&[*mode, u8::from(*flag)]);
            }
            Self::ManualSell {
                market_name,
                price,
                size,
            } => {
                write_short_string(out, market_name);
                out.extend_from_slice(&price.to_le_bytes());
                out.extend_from_slice(&size.to_le_bytes());
            }
            Self::PanicSellAll | Self::Unknown(_) => {}
        }
    }

    fn read(input: &mut &[u8]) -> Self {
        let opcode = read_u8_zero_tail(input);
        match opcode {
            0 => Self::TargetBuy {
                order_id: read_u64_zero_tail(input),
                price: read_f64_zero_tail(input),
                size: read_f64_zero_tail(input),
            },
            1 => Self::TargetSell {
                order_id: read_u64_zero_tail(input),
                price: read_f64_zero_tail(input),
            },
            3 => Self::Stops {
                order_id: read_u64_zero_tail(input),
                stops: StopSettings::read_from_delphi_stream(input),
            },
            4 => Self::VStop {
                order_id: read_u64_zero_tail(input),
                enabled: read_u8_zero_tail(input) != 0,
                fixed: read_u8_zero_tail(input) != 0,
                level: read_f64_zero_tail(input),
                volume: read_f64_zero_tail(input),
            },
            5 => Self::Panic {
                order_id: read_u64_zero_tail(input),
                enabled: read_u8_zero_tail(input) & 1 != 0,
            },
            6 => Self::Immune {
                order_id: read_u64_zero_tail(input),
                enabled: read_u8_zero_tail(input) & 1 != 0,
            },
            7 => Self::CancelBuy {
                order_id: read_u64_zero_tail(input),
            },
            8 => Self::CancelSell {
                order_id: read_u64_zero_tail(input),
            },
            9 => Self::PendingCancel {
                order_id: read_u64_zero_tail(input),
            },
            10 => {
                let market_name = read_short_string(input);
                let flags = read_u8_zero_tail(input);
                Self::Start {
                    market_name,
                    is_short: flags & 1 != 0,
                    use_market_stop: flags & 2 != 0,
                    strategy_id: read_u64_zero_tail(input),
                    size: read_f64_zero_tail(input),
                    price: read_f64_zero_tail(input),
                    planned_sell_price: read_f64_zero_tail(input),
                }
            }
            11 => {
                let market_name = read_short_string(input);
                let leg = read_u8_zero_tail(input);
                match read_u8_zero_tail(input) {
                    0 => Self::MoveAllKind {
                        market_name,
                        leg,
                        move_kind: read_u8_zero_tail(input),
                        side: read_u8_zero_tail(input),
                        price: read_f64_zero_tail(input),
                    },
                    1 => Self::MoveAllZone {
                        market_name,
                        side: read_u8_zero_tail(input),
                        min_price: read_f64_zero_tail(input),
                        max_price: read_f64_zero_tail(input),
                    },
                    2 => Self::MoveAllPercent {
                        market_name,
                        leg,
                        percent: read_f64_zero_tail(input),
                    },
                    _ => Self::Unknown(opcode),
                }
            }
            12 => Self::Join {
                market_name: read_short_string(input),
                is_short: read_u8_zero_tail(input) & 1 != 0,
            },
            13 => {
                let order_id = read_u64_zero_tail(input);
                let parts = read_i32_zero_tail(input);
                let flags = read_u8_zero_tail(input);
                Self::SplitOrder {
                    order_id,
                    parts,
                    split_small: flags & 1 != 0,
                    split_small_sell: flags & 2 != 0,
                }
            }
            14 => Self::ClosePosition {
                market_name: read_short_string(input),
                mode: read_u8_zero_tail(input),
                flag: read_u8_zero_tail(input) & 1 != 0,
            },
            15 => Self::ManualSell {
                market_name: read_short_string(input),
                price: read_f64_zero_tail(input),
                size: read_f64_zero_tail(input),
            },
            17 => Self::PanicSellAll,
            _ => Self::Unknown(opcode),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct OrderCommand {
    pub header: BaseCommandHeader,
    pub payload: OrderCommandPayload,
}

impl OrderCommand {
    pub(crate) fn new(uid: u64, payload: OrderCommandPayload) -> Self {
        Self {
            header: BaseCommandHeader {
                cmd_id: 47,
                ver: CURRENT_PROTO_CMD_VER,
                uid,
            },
            payload,
        }
    }

    pub(crate) fn read(input: &mut &[u8]) -> Option<Self> {
        let header = BaseCommandHeader::read(input)?;
        let payload = OrderCommandPayload::read(input);
        Some(Self { header, payload })
    }

    pub(crate) fn to_bytes(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(96);
        self.header.write(&mut out);
        self.payload.write(&mut out);
        out
    }
}

static NEXT_ORDER_ACTION_ID: AtomicU64 = AtomicU64::new(0);

pub(crate) fn next_order_action_id() -> u64 {
    loop {
        let next = NEXT_ORDER_ACTION_ID
            .fetch_add(1, Ordering::Relaxed)
            .wrapping_add(1);
        if next != 0 {
            return next;
        }
    }
}

pub(crate) fn build_order_status_request(order_id: u64, exact_rev: u64) -> Vec<u8> {
    let mut out = Vec::with_capacity(21);
    BaseCommandHeader {
        cmd_id: 45,
        ver: CURRENT_PROTO_CMD_VER,
        uid: order_id,
    }
    .write(&mut out);
    write_uleb(exact_rev, &mut out);
    out
}

pub(crate) fn build_order_command(uid: u64, payload: OrderCommandPayload) -> Vec<u8> {
    OrderCommand::new(uid, payload).to_bytes()
}

fn write_short_string(out: &mut Vec<u8>, value: &str) {
    let bytes = value.as_bytes();
    let wire_len = bytes.len() as u8;
    out.push(wire_len);
    out.extend_from_slice(&bytes[..wire_len as usize]);
}

fn read_short_string(input: &mut &[u8]) -> String {
    let len = read_u8_zero_tail(input) as usize;
    let copied = len.min(input.len());
    let result = decode_utf8_delphi(&input[..copied]);
    *input = &input[copied..];
    result
}

fn read_u8_zero_tail(input: &mut &[u8]) -> u8 {
    input.split_first().map_or(0, |(&value, tail)| {
        *input = tail;
        value
    })
}

fn read_u16_zero_tail(input: &mut &[u8]) -> u16 {
    let mut bytes = [0u8; 2];
    let copied = bytes.len().min(input.len());
    bytes[..copied].copy_from_slice(&input[..copied]);
    *input = &input[copied..];
    u16::from_le_bytes(bytes)
}

fn read_u32_zero_tail(input: &mut &[u8]) -> u32 {
    let mut bytes = [0u8; 4];
    let copied = bytes.len().min(input.len());
    bytes[..copied].copy_from_slice(&input[..copied]);
    *input = &input[copied..];
    u32::from_le_bytes(bytes)
}

fn read_u16_exact(input: &mut &[u8]) -> Option<u16> {
    if input.len() < 2 {
        return None;
    }
    let value = u16::from_le_bytes(input[..2].try_into().unwrap());
    *input = &input[2..];
    Some(value)
}

fn read_u64_exact(input: &mut &[u8]) -> Option<u64> {
    if input.len() < 8 {
        return None;
    }
    let value = u64::from_le_bytes(input[..8].try_into().unwrap());
    *input = &input[8..];
    Some(value)
}

fn read_u64_zero_tail(input: &mut &[u8]) -> u64 {
    let mut bytes = [0u8; 8];
    let copied = bytes.len().min(input.len());
    bytes[..copied].copy_from_slice(&input[..copied]);
    *input = &input[copied..];
    u64::from_le_bytes(bytes)
}

fn read_i32_zero_tail(input: &mut &[u8]) -> i32 {
    let mut bytes = [0u8; 4];
    let copied = bytes.len().min(input.len());
    bytes[..copied].copy_from_slice(&input[..copied]);
    *input = &input[copied..];
    i32::from_le_bytes(bytes)
}

fn read_f64_zero_tail(input: &mut &[u8]) -> f64 {
    f64::from_bits(read_u64_zero_tail(input))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn section_layout_is_exactly_342_bytes() {
        for section in 0..ORDER_SECTION_COUNT - 1 {
            assert_eq!(
                ORDER_SECTION_OFFSET[section] + ORDER_SECTION_SIZE[section],
                ORDER_SECTION_OFFSET[section + 1]
            );
        }
        assert_eq!(
            ORDER_SECTION_OFFSET[12] + ORDER_SECTION_SIZE[12],
            ORDER_STATE_SIZE
        );
    }

    #[test]
    fn uleb_round_trip_including_u64_max() {
        for value in [0, 1, 0x7f, 0x80, 0x3fff, 0x4000, u32::MAX as u64, u64::MAX] {
            let mut bytes = Vec::new();
            write_uleb(value, &mut bytes);
            let mut input = bytes.as_slice();
            assert_eq!(read_uleb(&mut input), Some(value));
            assert!(input.is_empty());
        }
    }

    #[test]
    fn image_round_trip_preserves_sparse_sections() {
        let mut state = CanonicalOrderState::default();
        state.section_mut(OSEC_PHASE).fill(0x11);
        state.section_mut(OSEC_STOPS).fill(0x22);
        let image = OrderImage {
            header: BaseCommandHeader {
                cmd_id: 41,
                ver: 4,
                uid: 77,
            },
            state_rev: 129,
            desc: OrderDescription::for_test("BTCUSDT", true, false),
            section_mask: (1 << OSEC_PHASE) | (1 << OSEC_STOPS),
            state,
        };
        let mut bytes = Vec::new();
        image.write(&mut bytes);
        let mut input = bytes.as_slice();
        let decoded = OrderImage::read(&mut input).unwrap();
        assert!(input.is_empty());
        assert_eq!(decoded.header.uid, 77);
        assert_eq!(decoded.state_rev, 129);
        assert_eq!(decoded.desc, image.desc);
        assert_eq!(decoded.state, image.state);
    }

    #[test]
    fn unchecked_image_and_patch_fields_keep_delphi_zero_tail_reads() {
        let header = BaseCommandHeader {
            cmd_id: 41,
            ver: 4,
            uid: 77,
        };
        let mut image_bytes = Vec::new();
        header.write(&mut image_bytes);
        write_uleb(1, &mut image_bytes);
        OrderDescription::for_test("BTCUSDT", false, false).wire_bytes(&mut image_bytes);
        image_bytes.push(0x34);
        let mut input = image_bytes.as_slice();
        let image = OrderImage::read(&mut input).unwrap();
        assert_eq!(image.section_mask, 0x0034);
        assert!(input.is_empty());

        let mut patch_bytes = Vec::new();
        BaseCommandHeader {
            cmd_id: 42,
            ver: 4,
            uid: 78,
        }
        .write(&mut patch_bytes);
        write_uleb(2, &mut patch_bytes);
        patch_bytes.extend_from_slice(&[0x78, 0x56, 0x34]);
        let mut input = patch_bytes.as_slice();
        let patch = OrderPatch::read(&mut input).unwrap();
        assert_eq!(patch.state_hash, 0x0034_5678);
        assert_eq!(patch.section_mask, 0);
        assert!(input.is_empty());
    }

    #[test]
    fn snapshot_and_catalog_page_bounds_keep_delphi_zero_tail_reads() {
        let mut snapshot_bytes = Vec::new();
        BaseCommandHeader {
            cmd_id: 43,
            ver: 4,
            uid: 0,
        }
        .write(&mut snapshot_bytes);
        snapshot_bytes.extend_from_slice(&[0x11, 0x22, 0x33]);
        let mut input = snapshot_bytes.as_slice();
        let snapshot = OrdersSnapshot::read(&mut input).unwrap();
        assert_eq!(snapshot.from_uid, 0x0033_2211);
        assert_eq!(snapshot.range_end_uid, 0);
        assert!(input.is_empty());

        let mut catalog_bytes = Vec::new();
        BaseCommandHeader {
            cmd_id: 44,
            ver: 4,
            uid: 0,
        }
        .write(&mut catalog_bytes);
        catalog_bytes.extend_from_slice(&7u64.to_le_bytes());
        catalog_bytes.extend_from_slice(&[0x44, 0x55]);
        let mut input = catalog_bytes.as_slice();
        let catalog = OrdersCatalog::read(&mut input).unwrap();
        assert_eq!(catalog.from_uid, 7);
        assert_eq!(catalog.range_end_uid, 0x5544);
        assert!(input.is_empty());
    }

    #[test]
    fn state_hash_excludes_unused_description_tail() {
        let desc = OrderDescription::for_test("ETHUSDT", false, true);
        let state = CanonicalOrderState::default();
        let mut expected = Vec::new();
        expected.extend_from_slice(&9u64.to_le_bytes());
        desc.wire_bytes(&mut expected);
        expected.extend_from_slice(&state.0);
        assert_eq!(
            state_hash(9, &desc, &state),
            crc32c_append(ORDER_STATE_HASH_SEED, &expected)
        );
    }

    #[test]
    fn order_command_groups_match_transport_contract() {
        let grouped = [
            (
                OrderCommandPayload::TargetBuy {
                    order_id: 4,
                    price: 1.0,
                    size: 2.0,
                },
                OrderCommandGroup::Buy,
            ),
            (
                OrderCommandPayload::CancelSell { order_id: 4 },
                OrderCommandGroup::Sell,
            ),
            (
                OrderCommandPayload::Stops {
                    order_id: 4,
                    stops: StopSettings::default(),
                },
                OrderCommandGroup::Stops,
            ),
            (
                OrderCommandPayload::VStop {
                    order_id: 4,
                    enabled: true,
                    fixed: false,
                    level: 1.0,
                    volume: 2.0,
                },
                OrderCommandGroup::VStop,
            ),
            (
                OrderCommandPayload::Panic {
                    order_id: 4,
                    enabled: true,
                },
                OrderCommandGroup::Panic,
            ),
            (
                OrderCommandPayload::Immune {
                    order_id: 4,
                    enabled: true,
                },
                OrderCommandGroup::Immune,
            ),
        ];
        for (payload, expected) in grouped {
            assert_eq!(payload.group(), Some(expected));
        }

        for payload in [
            OrderCommandPayload::Start {
                market_name: "BTCUSDT".to_owned(),
                is_short: false,
                use_market_stop: false,
                strategy_id: 0,
                size: 0.0,
                price: 0.0,
                planned_sell_price: 0.0,
            },
            OrderCommandPayload::MoveAllPercent {
                market_name: "BTCUSDT".to_owned(),
                leg: 0,
                percent: 1.0,
            },
            OrderCommandPayload::PanicSellAll,
        ] {
            assert_eq!(payload.group(), None);
        }
    }

    #[test]
    fn order_command_wire_layout_matches_protocol_v4() {
        let uid = 0x0102_0304_0506_0708;
        let bytes = OrderCommand::new(
            uid,
            OrderCommandPayload::TargetBuy {
                order_id: 0x1112_1314_1516_1718,
                price: 123.5,
                size: 7.25,
            },
        )
        .to_bytes();

        let mut expected = Vec::new();
        expected.push(47);
        expected.extend_from_slice(&CURRENT_PROTO_CMD_VER.to_le_bytes());
        expected.extend_from_slice(&uid.to_le_bytes());
        expected.push(0);
        expected.extend_from_slice(&0x1112_1314_1516_1718u64.to_le_bytes());
        expected.extend_from_slice(&123.5f64.to_le_bytes());
        expected.extend_from_slice(&7.25f64.to_le_bytes());
        assert_eq!(bytes, expected);

        let mut input = bytes.as_slice();
        let command = OrderCommand::read(&mut input).expect("valid TOrderCommand");
        assert!(input.is_empty());
        assert_eq!(command.header.cmd_id, 47);
        assert_eq!(command.header.ver, CURRENT_PROTO_CMD_VER);
        assert_eq!(command.header.uid, uid);
        assert!(matches!(
            command.payload,
            OrderCommandPayload::TargetBuy {
                order_id: 0x1112_1314_1516_1718,
                price: 123.5,
                size: 7.25,
            }
        ));
    }

    #[test]
    fn move_all_percent_has_no_side_byte_on_protocol_v4_wire() {
        let bytes = OrderCommand::new(
            9,
            OrderCommandPayload::MoveAllPercent {
                market_name: "BTCUSDT".to_owned(),
                leg: 1,
                percent: 3.5,
            },
        )
        .to_bytes();

        let payload = &bytes[11..];
        let mut expected = vec![11, 7];
        expected.extend_from_slice(b"BTCUSDT");
        expected.extend_from_slice(&[1, 2]);
        expected.extend_from_slice(&3.5f64.to_le_bytes());
        assert_eq!(payload, expected);
    }

    #[test]
    fn all_order_action_bodies_match_protocol_v4_wire() {
        fn with_u64(mut bytes: Vec<u8>, value: u64) -> Vec<u8> {
            bytes.extend_from_slice(&value.to_le_bytes());
            bytes
        }
        fn with_i32(mut bytes: Vec<u8>, value: i32) -> Vec<u8> {
            bytes.extend_from_slice(&value.to_le_bytes());
            bytes
        }
        fn with_f64(mut bytes: Vec<u8>, value: f64) -> Vec<u8> {
            bytes.extend_from_slice(&value.to_le_bytes());
            bytes
        }
        fn short(mut bytes: Vec<u8>, value: &str) -> Vec<u8> {
            bytes.push(value.len() as u8);
            bytes.extend_from_slice(value.as_bytes());
            bytes
        }
        fn assert_body(payload: OrderCommandPayload, expected: Vec<u8>) {
            let bytes = OrderCommand::new(0x8877_6655_4433_2211, payload).to_bytes();
            assert_eq!(&bytes[11..], expected.as_slice());
        }

        let order_id = 0x0102_0304_0506_0708;
        assert_body(
            OrderCommandPayload::TargetBuy {
                order_id,
                price: 12.5,
                size: 3.25,
            },
            with_f64(with_f64(with_u64(vec![0], order_id), 12.5), 3.25),
        );
        assert_body(
            OrderCommandPayload::TargetSell {
                order_id,
                price: 13.5,
            },
            with_f64(with_u64(vec![1], order_id), 13.5),
        );

        let mut stops = with_u64(vec![3], order_id);
        stops.extend_from_slice(&[0; 46]);
        assert_body(
            OrderCommandPayload::Stops {
                order_id,
                stops: StopSettings::default(),
            },
            stops,
        );
        assert_body(
            OrderCommandPayload::VStop {
                order_id,
                enabled: true,
                fixed: false,
                level: 7.5,
                volume: 8.5,
            },
            with_f64(
                with_f64(
                    {
                        let mut bytes = with_u64(vec![4], order_id);
                        bytes.extend_from_slice(&[1, 0]);
                        bytes
                    },
                    7.5,
                ),
                8.5,
            ),
        );
        assert_body(
            OrderCommandPayload::Panic {
                order_id,
                enabled: true,
            },
            {
                let mut bytes = with_u64(vec![5], order_id);
                bytes.push(1);
                bytes
            },
        );
        assert_body(
            OrderCommandPayload::Immune {
                order_id,
                enabled: false,
            },
            {
                let mut bytes = with_u64(vec![6], order_id);
                bytes.push(0);
                bytes
            },
        );
        for (opcode, payload) in [
            (7, OrderCommandPayload::CancelBuy { order_id }),
            (8, OrderCommandPayload::CancelSell { order_id }),
            (9, OrderCommandPayload::PendingCancel { order_id }),
        ] {
            assert_body(payload, with_u64(vec![opcode], order_id));
        }

        let mut start = short(vec![10], "ETHUSDT");
        start.push(3);
        start = with_u64(start, 99);
        start = with_f64(start, 250.0);
        start = with_f64(start, 1900.0);
        start = with_f64(start, 2000.0);
        assert_body(
            OrderCommandPayload::Start {
                market_name: "ETHUSDT".to_owned(),
                is_short: true,
                use_market_stop: true,
                strategy_id: 99,
                size: 250.0,
                price: 1900.0,
                planned_sell_price: 2000.0,
            },
            start,
        );

        let mut move_kind = short(vec![11], "SOLUSDT");
        move_kind.extend_from_slice(&[1, 0, 6, 2]);
        move_kind = with_f64(move_kind, 150.0);
        assert_body(
            OrderCommandPayload::MoveAllKind {
                market_name: "SOLUSDT".to_owned(),
                leg: 1,
                move_kind: 6,
                side: 2,
                price: 150.0,
            },
            move_kind,
        );
        let mut move_zone = short(vec![11], "SOLUSDT");
        move_zone.extend_from_slice(&[0, 1, 1]);
        move_zone = with_f64(move_zone, 140.0);
        move_zone = with_f64(move_zone, 160.0);
        assert_body(
            OrderCommandPayload::MoveAllZone {
                market_name: "SOLUSDT".to_owned(),
                side: 1,
                min_price: 140.0,
                max_price: 160.0,
            },
            move_zone,
        );
        let mut move_percent = short(vec![11], "SOLUSDT");
        move_percent.extend_from_slice(&[1, 2]);
        move_percent = with_f64(move_percent, 2.5);
        assert_body(
            OrderCommandPayload::MoveAllPercent {
                market_name: "SOLUSDT".to_owned(),
                leg: 1,
                percent: 2.5,
            },
            move_percent,
        );

        let mut join = short(vec![12], "BTCUSDT");
        join.push(1);
        assert_body(
            OrderCommandPayload::Join {
                market_name: "BTCUSDT".to_owned(),
                is_short: true,
            },
            join,
        );
        let mut split = with_i32(with_u64(vec![13], order_id), 7);
        split.push(3);
        assert_body(
            OrderCommandPayload::SplitOrder {
                order_id,
                parts: 7,
                split_small: true,
                split_small_sell: true,
            },
            split,
        );
        let mut close = short(vec![14], "BTCUSDT");
        close.extend_from_slice(&[3, 1]);
        assert_body(
            OrderCommandPayload::ClosePosition {
                market_name: "BTCUSDT".to_owned(),
                mode: 3,
                flag: true,
            },
            close,
        );
        let mut sell = short(vec![15], "BTCUSDT");
        sell = with_f64(sell, 66_000.0);
        sell = with_f64(sell, 250.0);
        assert_body(
            OrderCommandPayload::ManualSell {
                market_name: "BTCUSDT".to_owned(),
                price: 66_000.0,
                size: 250.0,
            },
            sell,
        );
        assert_body(OrderCommandPayload::PanicSellAll, vec![17]);
    }
}
