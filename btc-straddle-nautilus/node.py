"""
NautilusTrader Orchestration Node and Runner.

Initializes TradingNode with Bybit Testnet live data and execution clients,
runs the FastAPI WebSocket monitoring dashboard concurrently, and manages
order routing, risk thresholds, and graceful shutdown.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import signal
import sys
import threading
import time
from decimal import Decimal
from typing import Optional

import uvicorn
from nautilus_trader.adapters.bybit import (
    BybitDataClientConfig,
    BybitExecClientConfig,
    BybitLiveDataClientFactory,
    BybitLiveExecClientFactory,
    BybitProductType,
)
from nautilus_trader.config import TradingNodeConfig
from nautilus_trader.live.node import TradingNode
from nautilus_trader.model.currencies import BTC, USDT
from nautilus_trader.model.data import QuoteTick
from nautilus_trader.model.enums import OptionKind
from nautilus_trader.model.identifiers import InstrumentId, Symbol, Venue
from nautilus_trader.model.instruments import CryptoOption
from nautilus_trader.model.objects import Price, Quantity

from config import StraddleStrategyConfig, app_config
from strategy import BtcOptionsStraddleStrategy
from web.server import app, hub

# Configure structured logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] (%(name)s) %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("straddle.node")


class StraddleTradingRunner:
    """
    Manages the lifecycle of the TradingNode, Strategy, and Web Dashboard.
    """

    def __init__(
        self,
        strategy_config: StraddleStrategyConfig,
        simulate: bool = False,
        web_port: int = 8000,
        web_host: str = "0.0.0.0",
    ) -> None:
        self.strategy_config = strategy_config
        self.simulate = simulate
        self.web_port = web_port
        self.web_host = web_host

        self.node: Optional[TradingNode] = None
        self.strategy: Optional[BtcOptionsStraddleStrategy] = None
        self._web_server_thread: Optional[threading.Thread] = None
        self._server: Optional[uvicorn.Server] = None
        self._stop_event = threading.Event()

    def build_trading_node(self) -> TradingNode:
        """Configures and builds the NautilusTrader live node."""
        logger.info("Configuring NautilusTrader TradingNode for Bybit Testnet...")

        has_creds = app_config.has_credentials
        if not has_creds and not self.simulate:
            logger.warning(
                "No live Bybit Testnet credentials detected in environment. "
                "Enabling demonstration simulation mode (--simulate) for UI walkthrough."
            )
            self.simulate = True

        data_clients = {}
        exec_clients = {}

        if not self.simulate:
            data_cfg = BybitDataClientConfig(
                api_key=app_config.api_key,
                api_secret=app_config.api_secret,
                testnet=True,
                product_types=[BybitProductType.OPTION, BybitProductType.SPOT],
            )
            exec_cfg = BybitExecClientConfig(
                api_key=app_config.api_key,
                api_secret=app_config.api_secret,
                testnet=True,
                product_types=[BybitProductType.OPTION, BybitProductType.SPOT],
            )
            data_clients["BYBIT"] = data_cfg
            exec_clients["BYBIT"] = exec_cfg

        node_config = TradingNodeConfig(
            trader_id="BTC-STRADDLE-001",
            data_clients=data_clients,
            exec_clients=exec_clients,
        )

        node = TradingNode(config=node_config)

        if not self.simulate:
            node.add_data_client_factory("BYBIT", BybitLiveDataClientFactory)
            node.add_exec_client_factory("BYBIT", BybitLiveExecClientFactory)

        # Build node infrastructure
        node.build()
        logger.info("NautilusTrader TradingNode built successfully.")

        # Instantiate Strategy
        self.strategy = BtcOptionsStraddleStrategy(config=self.strategy_config)
        self.strategy.set_event_dispatcher(hub.dispatch_from_strategy)
        hub.register_strategy(self.strategy)

        node.trader.add_strategy(self.strategy)
        logger.info("BtcOptionsStraddleStrategy registered to Trader engine.")

        return node

    def start_web_server(self) -> None:
        """Launches the FastAPI Uvicorn web server in a dedicated background daemon thread."""
        config = uvicorn.Config(
            app=app,
            host=self.web_host,
            port=self.web_port,
            log_level="warning",
            loop="asyncio",
        )
        self._server = uvicorn.Server(config)

        def run_uvicorn() -> None:
            logger.info(f"FastAPI Web Monitoring Dashboard available at http://localhost:{self.web_port}")
            self._server.run()

        self._web_server_thread = threading.Thread(target=run_uvicorn, daemon=True, name="WebDashboard")
        self._web_server_thread.start()

    def run(self) -> None:
        """Main execution entry point with signal handling."""
        self.start_web_server()
        self.node = self.build_trading_node()

        def signal_handler(sig: int, frame: Any) -> None:
            logger.info(f"Shutdown signal received ({sig}). Stopping strategy and node...")
            self.stop()
            sys.exit(0)

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        if self.simulate:
            self._run_simulation_feeder()
        else:
            logger.info("Running TradingNode in live Bybit Testnet mode...")
            try:
                self.node.run()
            except KeyboardInterrupt:
                self.stop()

    def _run_simulation_feeder(self) -> None:
        """
        Feeds realistic synthetic market data into the engine and strategy
        for immediate visualization during walkthroughs without requiring live keys.
        """
        logger.info("Starting Simulated Market Feeder (Demo Mode)...")

        # Create realistic option contracts
        venue = Venue("BYBIT")
        underlying_sym = "BTCUSDT"
        btc_price = Decimal("68450.00")
        strike = Decimal("68500.00")
        now_ns = int(time.time() * 1e9)
        expiry_ns = now_ns + (7 * 86400 * 1_000_000_000)  # 7 days forward

        call_id = InstrumentId.from_str(f"BTC-28MAR25-68500-C.BYBIT")
        put_id = InstrumentId.from_str(f"BTC-28MAR25-68500-P.BYBIT")
        spot_id = InstrumentId.from_str(f"BTCUSDT.BYBIT")

        call_opt = CryptoOption(
            call_id,
            Symbol(str(call_id.symbol)),
            BTC,
            USDT,
            USDT,
            False,
            OptionKind.CALL,
            Price(strike, 2),
            now_ns,
            expiry_ns,
            2,
            3,
            Price(Decimal("0.50"), 2),
            Quantity.from_str("0.001"),
            now_ns,
            now_ns,
        )

        put_opt = CryptoOption(
            put_id,
            Symbol(str(put_id.symbol)),
            BTC,
            USDT,
            USDT,
            False,
            OptionKind.PUT,
            Price(strike, 2),
            now_ns,
            expiry_ns,
            2,
            3,
            Price(Decimal("0.50"), 2),
            Quantity.from_str("0.001"),
            now_ns,
            now_ns,
        )

        # Register in strategy cache
        self.strategy.cache.add_instrument(call_opt)
        self.strategy.cache.add_instrument(put_opt)
        self.strategy.underlying_price = btc_price
        self.strategy.underlying_id = spot_id

        # Trigger on_start
        self.strategy._discover_and_subscribe_straddle()

        # Send initial quotes
        call_ask = Decimal("1420.00")
        put_ask = Decimal("1380.00")

        # Simulate ticks and order fills
        step = 0
        while not self._stop_event.is_set():
            now = int(time.time() * 1e9)
            step += 1

            # Fluctuate prices continuously to demonstrate live streaming, delta, and risk indicators
            cycle = (step % 30)
            if cycle < 10:
                call_ask -= Decimal("0.75")
                put_ask -= Decimal("0.50")
            elif cycle < 20:
                call_ask += Decimal("0.90")
                put_ask += Decimal("0.80")
            else:
                call_ask -= Decimal("0.15")
                put_ask -= Decimal("0.30")

            # Dispatch quote ticks
            call_tick = QuoteTick(
                instrument_id=call_id,
                bid_price=Price(call_ask - Decimal("1.00"), 2),
                ask_price=Price(call_ask, 2),
                bid_size=Quantity.from_str("5.0"),
                ask_size=Quantity.from_str("5.0"),
                ts_event=now,
                ts_init=now,
            )
            put_tick = QuoteTick(
                instrument_id=put_id,
                bid_price=Price(put_ask - Decimal("1.00"), 2),
                ask_price=Price(put_ask, 2),
                bid_size=Quantity.from_str("5.0"),
                ask_size=Quantity.from_str("5.0"),
                ts_event=now,
                ts_init=now,
            )

            self.strategy.on_quote_tick(call_tick)
            self.strategy.on_quote_tick(put_tick)

            # Simulate fills on step 2
            if step == 2 and not self.strategy.is_entered:
                self.strategy.call_entry_price = call_ask
                self.strategy.put_entry_price = put_ask
                self.strategy.is_entered = True
                self.strategy.is_entering = False
                self.strategy.entry_combined_premium = call_ask + put_ask
                self.strategy._dispatch_event("position_update", {
                    "status": "ENTERED",
                    "call_entry_price": str(call_ask),
                    "put_entry_price": str(put_ask),
                    "entry_combined_premium": str(self.strategy.entry_combined_premium),
                })

            time.sleep(1.0)

    def stop(self) -> None:
        """Gracefully terminates the strategy, node, and web server."""
        self._stop_event.set()
        logger.info("Stopping runner components...")
        if self.strategy:
            try:
                if getattr(self.strategy, "is_running", False):
                    self.strategy.stop()
            except Exception as e:
                logger.debug(f"Error stopping strategy: {e}")

        if self.node:
            try:
                if getattr(self.node, "is_running", False):
                    self.node.stop()
                self.node.dispose()
            except Exception as e:
                logger.debug(f"Error disposing node: {e}")

        if self._server:
            self._server.should_exit = True
        logger.info("Shutdown completed cleanly.")


def main() -> None:
    parser = argparse.ArgumentParser(description="NautilusTrader BTC ATM Straddle Engine")
    parser.add_argument("--simulate", action="store_true", help="Run with simulated market data feed")
    parser.add_argument("--port", type=int, default=8000, help="Web dashboard port (default: 8000)")
    parser.add_argument("--trade-size", type=str, default="0.01", help="Contract trade size")
    parser.add_argument("--sl", type=str, default="0.01", help="Stop Loss pct (0.01 = 1%)")
    parser.add_argument("--tp", type=str, default="0.02", help="Take Profit pct (0.02 = 2%)")
    args = parser.parse_args()

    cfg = StraddleStrategyConfig(
        underlying_symbol=app_config.underlying_symbol,
        trade_size=Decimal(args.trade_size),
        stop_loss_pct=Decimal(args.sl),
        take_profit_pct=Decimal(args.tp),
        enable_risk_mgmt=True,
        poll_interval_sec=1.0,
    )

    runner = StraddleTradingRunner(
        strategy_config=cfg,
        simulate=args.simulate or app_config.simulate_feed,
        web_port=args.port,
        web_host="0.0.0.0",
    )
    runner.run()


if __name__ == "__main__":
    main()
