// Licensed under the Apache License, Version 2.0. See LICENSE.

//! Engine-enforced per-participant risk limits.
//!
//! The limits are log-ordered through [`CommandKind::SetRisk`], so a replay
//! reaches the same exposure and limits as the live engine. The table tracks
//! **exposure**, not just configuration: every resting order and every fill
//! moves the participant's entry, which is what makes the admission check
//! meaningful when several orders race for one limit.
//!
//! # Limits
//!
//! - `max_net_qty` bounds the worst-case absolute position. The exposure is
//!   conservative: a resting buy counts toward the long side and a resting sell
//!   toward the short side, because either may fill. A zero limit disables the
//!   bound.
//! - `max_open_buy_notional_cents` bounds the quote cents in resting buy orders.
//!   Only order types that can rest (`Limit` and `PostOnly`) are checked, and a
//!   crossing limit is charged its full quantity before matching: the engine
//!   must not admit a buy on the assumption that it will fill immediately.
//!   A zero limit disables the bound.
//!
//! A participant with no configured limit is unconstrained and costs one empty
//! lookup on admission. Entries with no limit and no exposure are pruned, so
//! the table is bounded by the participants that are actually constrained or
//! carry open interest.
//!
//! [`CommandKind::SetRisk`]: whippet_core::protocol::CommandKind::SetRisk

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use whippet_core::ids::ParticipantId;
use whippet_core::protocol::Side;

use crate::book::RestingOrder;

/// A participant's configured limits. Both zero means unrestricted.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RiskLimits {
    /// Absolute net-position bound in order quantity units; zero disables.
    pub max_net_qty: u64,
    /// Bound on resting buy notional in quote cents; zero disables.
    pub max_open_buy_notional_cents: u64,
}

impl RiskLimits {
    pub fn is_unrestricted(self) -> bool {
        self.max_net_qty == 0 && self.max_open_buy_notional_cents == 0
    }
}

/// What a participant currently has at risk.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RiskExposure {
    /// Realized position: buys filled minus sells filled.
    pub net_qty: i128,
    /// Quantity in resting buy orders.
    pub open_buy_qty: u128,
    /// Quantity in resting sell orders.
    pub open_sell_qty: u128,
    /// Quote cents in resting buy orders.
    pub open_buy_notional_cents: u128,
}

impl RiskExposure {
    pub fn is_flat(self) -> bool {
        self.net_qty == 0
            && self.open_buy_qty == 0
            && self.open_sell_qty == 0
            && self.open_buy_notional_cents == 0
    }

    /// Worst-case long position: every resting buy fills and no resting sell
    /// does. A short position is offset by the buys rather than ignored, so a
    /// participant may cover a short and go long up to the same bound.
    fn worst_long(self) -> i128 {
        self.net_qty
            .saturating_add(i128::try_from(self.open_buy_qty).unwrap_or(i128::MAX))
    }

    /// Worst-case short position: every resting sell fills and no resting buy
    /// does.
    fn worst_short(self) -> i128 {
        self.net_qty
            .saturating_sub(i128::try_from(self.open_sell_qty).unwrap_or(i128::MAX))
    }
}

/// One participant's limits and exposure.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RiskEntry {
    pub limits: RiskLimits,
    pub exposure: RiskExposure,
}

/// Why an order was refused by the engine's risk gate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RiskViolation {
    /// Admitting the order could push the participant's worst-case position
    /// beyond `max_net_qty`.
    NetQty { limit: u64, worst_case: i128 },
    /// Admitting the order could push resting buy notional beyond
    /// `max_open_buy_notional_cents`.
    OpenBuyNotional { limit: u64, worst_case: u128 },
}

/// The per-symbol risk table.
///
/// A `BTreeMap` is deliberate: the snapshot hash walks participants in a
/// stable order, so two engines with the same entries hash identically
/// regardless of insertion order.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RiskTable {
    entries: BTreeMap<ParticipantId, RiskEntry>,
}

impl RiskTable {
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn entry(&self, participant: ParticipantId) -> RiskEntry {
        self.entries.get(&participant).copied().unwrap_or_default()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&ParticipantId, &RiskEntry)> {
        self.entries.iter()
    }

    /// Set or clear a participant's limits, preserving its exposure.
    pub fn set_limits(&mut self, participant: ParticipantId, limits: RiskLimits) {
        if limits.is_unrestricted() {
            if let Some(entry) = self.entries.get_mut(&participant) {
                entry.limits = limits;
            }
            self.prune(participant);
            return;
        }
        self.entries.entry(participant).or_default().limits = limits;
    }

    /// Check a new order before it reaches the book.
    ///
    /// `price_cents` is the order's limit price in quote cents. It is only read
    /// when the order can rest and is a buy; callers pass the matching price for
    /// other order types, which is harmless.
    pub fn check_new_order(
        &self,
        participant: ParticipantId,
        side: Side,
        qty: u64,
        price_cents: u128,
        may_rest: bool,
    ) -> Result<(), RiskViolation> {
        let entry = self.entry(participant);
        let limits = entry.limits;
        if limits.is_unrestricted() {
            return Ok(());
        }
        let qty_signed = i128::from(qty);
        if limits.max_net_qty > 0 {
            let worst_case = match side {
                Side::Buy => entry.exposure.worst_long().saturating_add(qty_signed),
                Side::Sell => entry.exposure.worst_short().saturating_sub(qty_signed),
            };
            if worst_case > i128::from(limits.max_net_qty)
                || worst_case < -i128::from(limits.max_net_qty)
            {
                return Err(RiskViolation::NetQty {
                    limit: limits.max_net_qty,
                    worst_case,
                });
            }
        }
        if may_rest && side == Side::Buy && limits.max_open_buy_notional_cents > 0 {
            let notional = price_cents.saturating_mul(u128::from(qty));
            let worst_case = entry
                .exposure
                .open_buy_notional_cents
                .saturating_add(notional);
            if worst_case > u128::from(limits.max_open_buy_notional_cents) {
                return Err(RiskViolation::OpenBuyNotional {
                    limit: limits.max_open_buy_notional_cents,
                    worst_case,
                });
            }
        }
        Ok(())
    }

    /// Check an amendment before it reaches the book.
    ///
    /// The projection removes the order's old open contribution and adds the
    /// replacement, then applies the same bounds as [`RiskTable::check_new_order`].
    /// The check is read-only: a refusal leaves the table untouched, and the
    /// caller applies [`RiskTable::on_cancel`] + [`RiskTable::on_rest`] only
    /// after this returns `Ok`.
    ///
    /// The order is known to be resting, so its old contribution must be
    /// present. A mismatch is a caller bug, asserted in debug builds and
    /// clamped in release rather than panicking on the shard thread.
    pub fn check_amend(
        &self,
        participant: ParticipantId,
        side: Side,
        old_price_cents: u128,
        old_qty: u64,
        new_price_cents: u128,
        new_qty: u64,
    ) -> Result<(), RiskViolation> {
        let entry = self.entry(participant);
        let limits = entry.limits;
        if limits.is_unrestricted() {
            return Ok(());
        }
        let old_qty128 = u128::from(old_qty);
        let new_qty128 = u128::from(new_qty);
        let mut projected = entry.exposure;
        match side {
            Side::Buy => {
                debug_assert!(projected.open_buy_qty >= old_qty128);
                debug_assert!(
                    projected.open_buy_notional_cents >= old_price_cents.saturating_mul(old_qty128)
                );
                projected.open_buy_qty = projected
                    .open_buy_qty
                    .saturating_sub(old_qty128)
                    .saturating_add(new_qty128);
                projected.open_buy_notional_cents = projected
                    .open_buy_notional_cents
                    .saturating_sub(old_price_cents.saturating_mul(old_qty128))
                    .saturating_add(new_price_cents.saturating_mul(new_qty128));
            }
            Side::Sell => {
                debug_assert!(projected.open_sell_qty >= old_qty128);
                projected.open_sell_qty = projected
                    .open_sell_qty
                    .saturating_sub(old_qty128)
                    .saturating_add(new_qty128);
            }
        }

        if limits.max_net_qty > 0 {
            let bound = i128::from(limits.max_net_qty);
            let long = projected.worst_long();
            let short = projected.worst_short();
            if long > bound || short < -bound {
                return Err(RiskViolation::NetQty {
                    limit: limits.max_net_qty,
                    worst_case: if long > bound { long } else { short },
                });
            }
        }
        if side == Side::Buy && limits.max_open_buy_notional_cents > 0 {
            let worst_case = projected.open_buy_notional_cents;
            if worst_case > u128::from(limits.max_open_buy_notional_cents) {
                return Err(RiskViolation::OpenBuyNotional {
                    limit: limits.max_open_buy_notional_cents,
                    worst_case,
                });
            }
        }
        Ok(())
    }

    /// A new resting order: its full remaining quantity is now open.
    pub fn on_rest(&mut self, participant: ParticipantId, side: Side, price_cents: u128, qty: u64) {
        let exposure = &mut self.entries.entry(participant).or_default().exposure;
        let qty = u128::from(qty);
        match side {
            Side::Buy => {
                exposure.open_buy_qty = exposure.open_buy_qty.saturating_add(qty);
                exposure.open_buy_notional_cents = exposure
                    .open_buy_notional_cents
                    .saturating_add(price_cents.saturating_mul(qty));
            }
            Side::Sell => {
                exposure.open_sell_qty = exposure.open_sell_qty.saturating_add(qty);
            }
        }
    }

    /// A taker fill: the participant's net position moves by the fill.
    ///
    /// A participant may be unconstrained; the entry exists only while it
    /// carries exposure. Two opposite fills that net to zero prune it again.
    pub fn on_take(&mut self, participant: ParticipantId, side: Side, qty: u64) {
        let exposure = &mut self.entries.entry(participant).or_default().exposure;
        let qty = i128::from(qty);
        exposure.net_qty += match side {
            Side::Buy => qty,
            Side::Sell => -qty,
        };
        self.prune(participant);
    }

    /// A maker fill: the resting quantity leaves the book and the maker's net
    /// position moves by the fill.
    pub fn on_maker_fill(
        &mut self,
        participant: ParticipantId,
        side: Side,
        price_cents: u128,
        qty: u64,
    ) {
        self.remove_open(participant, side, price_cents, qty);
        self.on_take(participant, side, qty);
        self.prune(participant);
    }

    /// A resting order left the book without trading (client cancel or
    /// self-trade prevention).
    pub fn on_cancel(
        &mut self,
        participant: ParticipantId,
        side: Side,
        price_cents: u128,
        qty: u64,
    ) {
        self.remove_open(participant, side, price_cents, qty);
        self.prune(participant);
    }

    fn remove_open(&mut self, participant: ParticipantId, side: Side, price_cents: u128, qty: u64) {
        let Some(entry) = self.entries.get_mut(&participant) else {
            debug_assert!(false, "risk exposure removed without an entry");
            return;
        };
        let qty128 = u128::from(qty);
        match side {
            Side::Buy => {
                entry.exposure.open_buy_qty =
                    checked_sub(entry.exposure.open_buy_qty, qty128, "open buy quantity");
                entry.exposure.open_buy_notional_cents = checked_sub(
                    entry.exposure.open_buy_notional_cents,
                    price_cents.saturating_mul(qty128),
                    "open buy notional",
                );
            }
            Side::Sell => {
                entry.exposure.open_sell_qty =
                    checked_sub(entry.exposure.open_sell_qty, qty128, "open sell quantity");
            }
        }
    }

    fn prune(&mut self, participant: ParticipantId) {
        let remove = self
            .entries
            .get(&participant)
            .is_some_and(|entry| entry.limits.is_unrestricted() && entry.exposure.is_flat());
        if remove {
            self.entries.remove(&participant);
        }
    }

    /// Hash the table in participant order. Field order and widths are part of
    /// the engine state hash; change them only with a hash-domain bump.
    pub fn hash_into(&self, hasher: &mut blake3::Hasher) {
        hasher.update(&(self.entries.len() as u64).to_le_bytes());
        for (participant, entry) in &self.entries {
            hasher.update(&participant.0.to_le_bytes());
            hasher.update(&entry.limits.max_net_qty.to_le_bytes());
            hasher.update(&entry.limits.max_open_buy_notional_cents.to_le_bytes());
            hasher.update(&entry.exposure.net_qty.to_le_bytes());
            hasher.update(&entry.exposure.open_buy_qty.to_le_bytes());
            hasher.update(&entry.exposure.open_sell_qty.to_le_bytes());
            hasher.update(&entry.exposure.open_buy_notional_cents.to_le_bytes());
        }
    }

    /// The table's own invariants: no constrained entry with impossible
    /// exposure, and a deterministic participant order (the map's type
    /// guarantees it, checked explicitly for a future storage change).
    pub fn check_invariants(&self) -> Result<(), String> {
        let mut previous: Option<ParticipantId> = None;
        for (participant, entry) in &self.entries {
            if let Some(previous) = previous
                && *participant <= previous
            {
                return Err(format!(
                    "risk entries are not strictly ordered: {participant:?} after {previous:?}"
                ));
            }
            previous = Some(*participant);
            if entry.limits.is_unrestricted() && entry.exposure.is_flat() {
                return Err(format!(
                    "participant {participant:?} has an empty risk entry; flat unrestricted entries are pruned"
                ));
            }
        }
        Ok(())
    }

    /// Check open exposure against the book that carries it.
    ///
    /// `tick_size_cents` converts the book's tick prices to the quote cents the
    /// limits use. Net position is not derivable from resting orders, so this
    /// checks only the open quantities and buy notional. Every book order must
    /// be covered by its participant's entry and no entry may claim open
    /// interest the book does not hold.
    pub fn check_against_orders(
        &self,
        orders: &[RestingOrder],
        tick_size_cents: i64,
    ) -> Result<(), String> {
        let tick_size = u128::try_from(tick_size_cents)
            .map_err(|_| format!("tick size {tick_size_cents} is not a positive number"))?;
        let mut expected: BTreeMap<ParticipantId, RiskExposure> = BTreeMap::new();
        for order in orders {
            let exposure = expected.entry(order.participant).or_default();
            let qty = u128::from(order.remaining.0);
            match order.side {
                Side::Buy => {
                    exposure.open_buy_qty += qty;
                    let price_cents = u128::try_from(order.price.0)
                        .unwrap_or_default()
                        .saturating_mul(tick_size);
                    exposure.open_buy_notional_cents += price_cents.saturating_mul(qty);
                }
                Side::Sell => exposure.open_sell_qty += qty,
            }
        }
        for (participant, exposure) in &expected {
            let actual = self.entry(*participant).exposure;
            if actual.open_buy_qty != exposure.open_buy_qty
                || actual.open_sell_qty != exposure.open_sell_qty
                || actual.open_buy_notional_cents != exposure.open_buy_notional_cents
            {
                return Err(format!(
                    "participant {participant:?} risk open exposure {:?} disagrees with the book {:?}",
                    actual, exposure
                ));
            }
        }
        for (participant, entry) in &self.entries {
            let held = expected.get(participant).copied().unwrap_or_default();
            if entry.exposure.open_buy_qty != held.open_buy_qty
                || entry.exposure.open_sell_qty != held.open_sell_qty
                || entry.exposure.open_buy_notional_cents != held.open_buy_notional_cents
            {
                return Err(format!(
                    "participant {participant:?} claims open exposure {:?} but the book holds {:?}",
                    entry.exposure, held
                ));
            }
        }
        Ok(())
    }
}

fn checked_sub(value: u128, amount: u128, what: &str) -> u128 {
    match value.checked_sub(amount) {
        Some(value) => value,
        None => {
            debug_assert!(false, "risk {what} underflow: {value} - {amount}");
            0
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use whippet_core::ids::{OrderId, Price};

    fn participant() -> ParticipantId {
        ParticipantId(7)
    }

    fn limits(net: u64, notional: u64) -> RiskLimits {
        RiskLimits {
            max_net_qty: net,
            max_open_buy_notional_cents: notional,
        }
    }

    #[test]
    fn unrestricted_participants_are_never_refused() {
        let table = RiskTable::default();
        table
            .check_new_order(participant(), Side::Buy, u64::MAX, u128::MAX, true)
            .expect("an unconfigured participant is unconstrained");
        assert!(table.is_empty());
    }

    #[test]
    fn net_limit_counts_resting_orders_and_the_candidate() {
        let mut table = RiskTable::default();
        table.set_limits(participant(), limits(100, 0));
        table.on_rest(participant(), Side::Buy, 1_000, 60);
        // 60 open + 41 new would leave a 101-lot worst case.
        assert_eq!(
            table.check_new_order(participant(), Side::Buy, 41, 1_000, true),
            Err(RiskViolation::NetQty {
                limit: 100,
                worst_case: 101
            })
        );
        table
            .check_new_order(participant(), Side::Buy, 40, 1_000, true)
            .expect("exactly at the limit is allowed");
    }

    #[test]
    fn net_limit_is_side_aware() {
        let mut table = RiskTable::default();
        table.set_limits(participant(), limits(10, 0));
        table.on_take(participant(), Side::Buy, 8);
        table
            .check_new_order(participant(), Side::Sell, 18, 0, false)
            .expect("a sell may cover the long and stop exactly at the short bound");
        assert!(matches!(
            table.check_new_order(participant(), Side::Buy, 3, 0, false),
            Err(RiskViolation::NetQty { .. })
        ));
    }

    #[test]
    fn open_buy_notional_only_checks_orders_that_can_rest() {
        let mut table = RiskTable::default();
        table.set_limits(participant(), limits(0, 10_000));
        table
            .check_new_order(participant(), Side::Buy, 1, 10_001, false)
            .expect("a market/IOC buy does not rest");
        assert_eq!(
            table.check_new_order(participant(), Side::Buy, 3, 5_000, true),
            Err(RiskViolation::OpenBuyNotional {
                limit: 10_000,
                worst_case: 15_000
            })
        );
        table
            .check_new_order(participant(), Side::Buy, 1, 10_000, true)
            .expect("exactly at the notional limit is allowed");
    }

    #[test]
    fn amend_projects_the_replacement_without_mutating() {
        let mut table = RiskTable::default();
        table.set_limits(participant(), limits(100, 1_500));
        table.on_rest(participant(), Side::Buy, 100, 10);
        let before = table.entry(participant());

        // Raising the price at the same size would breach the notional bound.
        assert_eq!(
            table.check_amend(participant(), Side::Buy, 100, 10, 200, 10),
            Err(RiskViolation::OpenBuyNotional {
                limit: 1_500,
                worst_case: 2_000
            })
        );
        assert_eq!(
            table.entry(participant()),
            before,
            "a refused amendment leaves the exposure untouched"
        );

        // Shrinking to the same notional fits, and so does a pure size cut.
        table
            .check_amend(participant(), Side::Buy, 100, 10, 200, 5)
            .expect("5 lots at 200 cents is exactly the 1,000-cent exposure");
        table
            .check_amend(participant(), Side::Buy, 100, 10, 100, 8)
            .expect("a size cut cannot breach the notional bound");
        assert_eq!(table.entry(participant()), before);

        // Applying the accepted replacement moves the open contribution.
        table.on_cancel(participant(), Side::Buy, 100, 10);
        table.on_rest(participant(), Side::Buy, 200, 5);
        let exposure = table.entry(participant()).exposure;
        assert_eq!(exposure.open_buy_qty, 5);
        assert_eq!(exposure.open_buy_notional_cents, 1_000);
    }

    #[test]
    fn amend_projects_the_net_bound_on_both_sides() {
        let mut table = RiskTable::default();
        table.set_limits(participant(), limits(20, 0));
        table.on_take(participant(), Side::Buy, 15);
        table.on_rest(participant(), Side::Sell, 100, 5);
        // Worst-case short stays at -5, so shrinking the sell is fine.
        table
            .check_amend(participant(), Side::Sell, 100, 5, 100, 3)
            .expect("a reduction cannot breach the net bound");
        // A hypothetical increase would, which the engine refuses before the
        // risk check; the projection itself catches it too.
        assert_eq!(
            table.check_amend(participant(), Side::Sell, 100, 5, 100, 40),
            Err(RiskViolation::NetQty {
                limit: 20,
                worst_case: -25
            })
        );
    }

    #[test]
    fn fills_and_cancels_move_exposure() {
        let mut table = RiskTable::default();
        table.set_limits(participant(), limits(100, 1_000_000));
        table.on_rest(participant(), Side::Buy, 100, 10);
        assert_eq!(table.entry(participant()).exposure.open_buy_qty, 10);
        table.on_maker_fill(participant(), Side::Buy, 100, 4);
        let exposure = table.entry(participant()).exposure;
        assert_eq!(exposure.open_buy_qty, 6);
        assert_eq!(exposure.open_buy_notional_cents, 600);
        assert_eq!(exposure.net_qty, 4);
        table.on_cancel(participant(), Side::Buy, 100, 6);
        let exposure = table.entry(participant()).exposure;
        assert!(exposure.is_flat() || exposure.net_qty == 4);
        assert_eq!(exposure.open_buy_qty, 0);
        assert_eq!(exposure.open_buy_notional_cents, 0);
    }

    #[test]
    fn flat_unrestricted_entries_are_pruned() {
        let mut table = RiskTable::default();
        table.set_limits(participant(), limits(10, 0));
        table.on_rest(participant(), Side::Sell, 100, 5);
        table.on_cancel(participant(), Side::Sell, 100, 5);
        assert_eq!(table.len(), 1, "the configured limit keeps the entry");
        table.set_limits(participant(), RiskLimits::default());
        assert!(
            table.is_empty(),
            "clearing the last limit prunes a flat entry"
        );
    }

    #[test]
    fn hashing_is_insertion_order_independent() {
        let mut first = RiskTable::default();
        first.set_limits(ParticipantId(1), limits(10, 0));
        first.set_limits(ParticipantId(2), limits(20, 30));
        let mut second = RiskTable::default();
        second.set_limits(ParticipantId(2), limits(20, 30));
        second.set_limits(ParticipantId(1), limits(10, 0));
        let mut left = blake3::Hasher::new();
        let mut right = blake3::Hasher::new();
        first.hash_into(&mut left);
        second.hash_into(&mut right);
        assert_eq!(left.finalize(), right.finalize());
    }

    #[test]
    fn book_mirror_catches_a_lost_exposure_update() {
        let mut table = RiskTable::default();
        table.set_limits(participant(), limits(100, 0));
        let order = RestingOrder {
            order_id: OrderId(1),
            participant: participant(),
            side: Side::Buy,
            price: Price(1),
            qty: whippet_core::ids::Qty(2),
            remaining: whippet_core::ids::Qty(1),
            seq: 1,
        };
        assert!(
            table
                .check_against_orders(std::slice::from_ref(&order), 100)
                .is_err(),
            "the table lacks the book's open exposure"
        );
        table.on_rest(participant(), Side::Buy, 100, 1);
        table
            .check_against_orders(std::slice::from_ref(&order), 100)
            .expect("matching exposure at a 100-cent tick");
    }

    #[test]
    fn invariants_reject_a_flat_unrestricted_entry() {
        let mut table = RiskTable::default();
        table.entries.insert(participant(), RiskEntry::default());
        assert!(table.check_invariants().is_err());
    }
}
