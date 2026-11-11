"""
Unit Tests for ATM Mathematical Discovery and Risk Management Triggers.

Covers:
1. ATM Strike Price discovery across diverse BTC price regimes.
2. Combined premium calculations and drift percentage accuracy.
3. Strict boundary evaluations for +1% Stop-Loss (+0.9999% vs +1.0000% vs +1.0001%).
4. Strict boundary evaluations for -2% Take-Profit (-1.9999% vs -2.0000% vs -2.0001%).
"""

from __future__ import annotations

from decimal import Decimal
import pytest

from strategy import BtcOptionsStraddleStrategy


class TestAtmStrikeDiscovery:
    """Tests for ATM strike selection mathematical logic."""

    def test_atm_strike_discovery_varying_regimes(self) -> None:
        strikes = [
            Decimal("65000"),
            Decimal("66000"),
            Decimal("67000"),
            Decimal("68000"),
            Decimal("69000"),
            Decimal("70000"),
        ]

        # Case 1: Price near 67,420 -> Closest is 67,000 (diff 420) vs 68,000 (diff 580)
        btc_price_1 = Decimal("67420")
        assert BtcOptionsStraddleStrategy.find_atm_strike(btc_price_1, strikes) == Decimal("67000")

        # Case 2: Price near 67,850 -> Closest is 68,000 (diff 150)
        btc_price_2 = Decimal("67850")
        assert BtcOptionsStraddleStrategy.find_atm_strike(btc_price_2, strikes) == Decimal("68000")

        # Case 3: Exact match on strike boundary 70,000
        btc_price_3 = Decimal("70000")
        assert BtcOptionsStraddleStrategy.find_atm_strike(btc_price_3, strikes) == Decimal("70000")

        # Case 4: Deep low price regime
        btc_price_low = Decimal("61200")
        assert BtcOptionsStraddleStrategy.find_atm_strike(btc_price_low, strikes) == Decimal("65000")

        # Case 5: Deep high price regime
        btc_price_high = Decimal("75200")
        assert BtcOptionsStraddleStrategy.find_atm_strike(btc_price_high, strikes) == Decimal("70000")

    def test_empty_strike_list_raises_error(self) -> None:
        with pytest.raises(ValueError, match="cannot be empty"):
            BtcOptionsStraddleStrategy.find_atm_strike(Decimal("68000"), [])

    def test_single_strike_list(self) -> None:
        assert BtcOptionsStraddleStrategy.find_atm_strike(
            Decimal("68000"), [Decimal("68000")]
        ) == Decimal("68000")


class TestCombinedPremiumValuation:
    """Tests for combined straddle premium and drift percentage."""

    def test_combined_premium_calculation(self) -> None:
        call_px = Decimal("1425.50")
        put_px = Decimal("1374.50")
        combined = BtcOptionsStraddleStrategy.calculate_combined_premium(call_px, put_px)
        assert combined == Decimal("2800.00")

    def test_drift_percentage_calculation(self) -> None:
        entry_premium = Decimal("2000.00")

        # Unchanged
        assert BtcOptionsStraddleStrategy.calculate_drift_pct(
            Decimal("2000.00"), entry_premium
        ) == Decimal("0.00")

        # Increase by 1% (Buyback cost rises to 2020)
        drift_up = BtcOptionsStraddleStrategy.calculate_drift_pct(
            Decimal("2020.00"), entry_premium
        )
        assert drift_up == Decimal("0.01")

        # Decrease by 2% (Buyback cost drops to 1960)
        drift_down = BtcOptionsStraddleStrategy.calculate_drift_pct(
            Decimal("1960.00"), entry_premium
        )
        assert drift_down == Decimal("-0.02")

    def test_zero_or_negative_entry_premium_raises(self) -> None:
        with pytest.raises(ValueError, match="must be positive"):
            BtcOptionsStraddleStrategy.calculate_drift_pct(Decimal("100"), Decimal("0"))


class TestRiskTriggerBoundaries:
    """Tests exact Level 2 Stop-Loss (+1%) and Take-Profit (-2%) boundary triggers."""

    entry_premium = Decimal("2000.00")
    sl_pct = Decimal("0.01")  # +1%
    tp_pct = Decimal("0.02")  # -2%

    def test_stop_loss_trigger_boundary(self) -> None:
        """
        Stop Loss Threshold: +1.0000%
        Entry = 2000.00
        At +0.9999% drift -> Cost = 2019.998 -> NO TRIGGER
        At +1.0000% drift -> Cost = 2020.000 -> TRIGGERS SL
        At +1.0001% drift -> Cost = 2020.002 -> TRIGGERS SL
        """
        # 1. Just below Stop Loss (+0.9999%)
        sub_sl_cost = Decimal("2000.00") * (Decimal("1") + Decimal("0.009999"))
        res_sub = BtcOptionsStraddleStrategy.check_risk_triggers(
            sub_sl_cost, self.entry_premium, self.sl_pct, self.tp_pct
        )
        assert res_sub is None, f"Expected no trigger at +0.9999%, got: {res_sub}"

        # 2. Exact Stop Loss boundary (+1.0000%)
        exact_sl_cost = Decimal("2020.00")
        res_exact = BtcOptionsStraddleStrategy.check_risk_triggers(
            exact_sl_cost, self.entry_premium, self.sl_pct, self.tp_pct
        )
        assert res_exact is not None and "STOP_LOSS" in res_exact

        # 3. Beyond Stop Loss (+1.0001%)
        super_sl_cost = Decimal("2000.00") * (Decimal("1") + Decimal("0.010001"))
        res_super = BtcOptionsStraddleStrategy.check_risk_triggers(
            super_sl_cost, self.entry_premium, self.sl_pct, self.tp_pct
        )
        assert res_super is not None and "STOP_LOSS" in res_super

    def test_take_profit_trigger_boundary(self) -> None:
        """
        Take Profit Threshold: -2.0000%
        Entry = 2000.00
        At -1.9999% drift -> Cost = 1960.002 -> NO TRIGGER
        At -2.0000% drift -> Cost = 1960.000 -> TRIGGERS TP
        At -2.0001% drift -> Cost = 1959.998 -> TRIGGERS TP
        """
        # 1. Just shy of Take Profit (-1.9999%)
        sub_tp_cost = Decimal("2000.00") * (Decimal("1") - Decimal("0.019999"))
        res_sub = BtcOptionsStraddleStrategy.check_risk_triggers(
            sub_tp_cost, self.entry_premium, self.sl_pct, self.tp_pct
        )
        assert res_sub is None, f"Expected no trigger at -1.9999%, got: {res_sub}"

        # 2. Exact Take Profit boundary (-2.0000%)
        exact_tp_cost = Decimal("1960.00")
        res_exact = BtcOptionsStraddleStrategy.check_risk_triggers(
            exact_tp_cost, self.entry_premium, self.sl_pct, self.tp_pct
        )
        assert res_exact is not None and "TAKE_PROFIT" in res_exact

        # 3. Beyond Take Profit (-2.0001%)
        super_tp_cost = Decimal("2000.00") * (Decimal("1") - Decimal("0.020001"))
        res_super = BtcOptionsStraddleStrategy.check_risk_triggers(
            super_tp_cost, self.entry_premium, self.sl_pct, self.tp_pct
        )
        assert res_super is not None and "TAKE_PROFIT" in res_super
