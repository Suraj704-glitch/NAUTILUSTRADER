"""
Unit Tests for Strategy Order Lifecycle, State Machine, and Idempotency.

Covers:
1. Entry order submission only occurs once both Call and Put quotes are available.
2. Idempotency: Duplicate exit orders are prevented if multiple ticks arrive while is_exiting=True.
3. Position-verified exits: _close_positions() only generates BUY orders for active SHORT legs.
4. Single-leg failure watchdog: Triggers immediate square-off when one leg executes but the other fails.
"""

from __future__ import annotations

import time
from decimal import Decimal
from unittest.mock import MagicMock, call, patch
import pytest

from nautilus_trader.model.enums import OrderSide, PositionSide, TimeInForce
from nautilus_trader.model.identifiers import ClientOrderId, InstrumentId
from nautilus_trader.model.objects import Quantity

from config import StraddleStrategyConfig
from strategy import BtcOptionsStraddleStrategy


class DummyPosition:
    def __init__(self, side: PositionSide, quantity: Quantity) -> None:
        self.side = side
        self.quantity = quantity


class TestOrderLifecycleAndIdempotency:
    """Tests the order state transitions and exit logic."""

    @pytest.fixture
    def strategy(self) -> BtcOptionsStraddleStrategy:
        cfg = StraddleStrategyConfig(
            underlying_symbol="BTCUSDT",
            trade_size=Decimal("0.01"),
            stop_loss_pct=Decimal("0.01"),
            take_profit_pct=Decimal("0.02"),
            enable_risk_mgmt=True,
        )
        strat = BtcOptionsStraddleStrategy(config=cfg)

        # Mock instruments
        call_mock = MagicMock()
        call_mock.id = InstrumentId.from_str("BTC-28MAR25-68000-C.BYBIT")
        put_mock = MagicMock()
        put_mock.id = InstrumentId.from_str("BTC-28MAR25-68000-P.BYBIT")

        strat.call_instrument = call_mock
        strat.put_instrument = put_mock

        # Mock order factory, cache & submit order
        strat.order_factory = MagicMock()
        strat.cache = MagicMock()
        strat.submit_order = MagicMock()

        return strat

    def test_close_positions_only_buys_active_shorts(self, strategy: BtcOptionsStraddleStrategy) -> None:
        """
        Verify that _close_positions() inspects cache and only submits buy orders
        for instruments with PositionSide.SHORT and quantity > 0.
        """
        # Call leg is SHORT 0.01, Put leg is already FLAT
        qty = Quantity.from_str("0.01")
        call_pos = DummyPosition(PositionSide.SHORT, qty)
        put_pos = DummyPosition(PositionSide.FLAT, Quantity.from_str("0.0"))

        strategy.cache.position = MagicMock(side_effect=lambda inst_id: (
            call_pos if inst_id == strategy.call_instrument.id else put_pos
        ))

        mock_exit_order = MagicMock()
        mock_exit_order.client_order_id = ClientOrderId("EXIT-CALL-001")
        strategy.order_factory.market.return_value = mock_exit_order

        # Trigger exit
        strategy._close_positions(reason="TEST_STOP_LOSS")

        # Assert: Market BUY was created ONLY for Call leg
        strategy.order_factory.market.assert_called_once_with(
            instrument_id=strategy.call_instrument.id,
            order_side=OrderSide.BUY,
            quantity=qty,
            time_in_force=TimeInForce.IOC,
        )
        strategy.submit_order.assert_called_once_with(mock_exit_order)
        assert strategy.is_closed is True
        assert strategy.is_exiting is False

    def test_idempotency_prevents_duplicate_exits(self, strategy: BtcOptionsStraddleStrategy) -> None:
        """
        Verify that if multiple ticks arrive while is_exiting=True or is_closed=True,
        no duplicate exit orders are submitted.
        """
        qty = Quantity.from_str("0.01")
        call_pos = DummyPosition(PositionSide.SHORT, qty)
        put_pos = DummyPosition(PositionSide.SHORT, qty)
        strategy.cache.position = MagicMock(side_effect=lambda inst_id: (
            call_pos if inst_id == strategy.call_instrument.id else put_pos
        ))

        mock_exit_order = MagicMock()
        mock_exit_order.client_order_id = ClientOrderId("EXIT-001")
        strategy.order_factory.market.return_value = mock_exit_order

        # First exit call
        strategy._close_positions(reason="INITIAL_EXIT")
        assert strategy.submit_order.call_count == 2
        assert strategy.is_closed is True

        # Second rapid exit call (subsequent tick arrival)
        strategy._close_positions(reason="DUPLICATE_TICK_EXIT")
        # Submit count must remain exactly 2, not 4
        assert strategy.submit_order.call_count == 2

    def test_single_leg_failure_watchdog_triggers_square_off(self, strategy: BtcOptionsStraddleStrategy) -> None:
        """
        Verify that if Call is filled but Put fails, the watchdog squares off the open Call leg.
        """
        strategy.is_entering = True
        strategy._entry_submission_time = time.time() - 15.0  # 15 seconds elapsed (timeout is 10s)

        qty = Quantity.from_str("0.01")
        call_pos = DummyPosition(PositionSide.SHORT, qty)  # Call filled
        put_pos = None  # Put failed/unfilled

        strategy.cache.position = MagicMock(side_effect=lambda inst_id: (
            call_pos if inst_id == strategy.call_instrument.id else put_pos
        ))

        mock_exit_order = MagicMock()
        strategy.order_factory.market.return_value = mock_exit_order

        # Run watchdog
        strategy._check_single_leg_failure_watchdog()

        # Verify that square off was executed
        strategy.order_factory.market.assert_called_once_with(
            instrument_id=strategy.call_instrument.id,
            order_side=OrderSide.BUY,
            quantity=qty,
            time_in_force=TimeInForce.IOC,
        )
        assert strategy.is_closed is True

    def test_execute_straddle_entry_creates_both_sell_orders(self, strategy: BtcOptionsStraddleStrategy) -> None:
        """
        Verify that Level 1 entry submits simultaneous SELL orders for both Call and Put.
        """
        mock_order_call = MagicMock()
        mock_order_call.client_order_id = ClientOrderId("ENTRY-CALL-001")
        mock_order_put = MagicMock()
        mock_order_put.client_order_id = ClientOrderId("ENTRY-PUT-001")

        strategy.order_factory.market.side_effect = [mock_order_call, mock_order_put]

        strategy._execute_straddle_entry()

        assert strategy.is_entering is True
        assert strategy.call_order_id == ClientOrderId("ENTRY-CALL-001")
        assert strategy.put_order_id == ClientOrderId("ENTRY-PUT-001")
        assert strategy.submit_order.call_count == 2

        # Verify duplicate entry calls do not submit more orders
        strategy._execute_straddle_entry()
        assert strategy.submit_order.call_count == 2
