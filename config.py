"""
System and Strategy Configuration Module.

Manages environment variable loading and exposes strongly-typed configurations
for the NautilusTrader engine, Bybit Testnet adapters, and Straddle Strategy.
"""

from __future__ import annotations

import os
from decimal import Decimal
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv
from nautilus_trader.config import StrategyConfig

# Locate and load .env file from the current directory or workspace root
env_path = Path(".env")
if not env_path.exists():
    env_path = Path(__file__).resolve().parent / ".env"
if not env_path.exists():
    env_path = Path(__file__).resolve().parent.parent / ".env"

load_dotenv(dotenv_path=env_path if env_path.exists() else None)


class StraddleStrategyConfig(StrategyConfig, frozen=True):
    """
    Configuration parameters for BtcOptionsStraddleStrategy.

    Attributes
    ----------
    underlying_symbol : str
        The underlying reference ticker (e.g., 'BTCUSDT' or 'BTC').
    trade_size : Decimal
        Standard contract lot order size for both Call and Put legs.
    stop_loss_pct : Decimal
        Stop-loss threshold percentage (0.01 = +1% buyback premium increase).
    take_profit_pct : Decimal
        Take-profit threshold percentage (0.02 = -2% buyback premium decrease).
    enable_risk_mgmt : bool
        Flag enabling automated Level 2 Stop-Loss and Take-Profit execution.
    poll_interval_sec : float
        Frequency interval for watchdog checks and state broadcasting.
    """

    underlying_symbol: str = os.getenv("UNDERLYING_SYMBOL", "BTCUSDT")
    trade_size: Decimal = Decimal(os.getenv("TRADE_SIZE", "0.01"))
    stop_loss_pct: Decimal = Decimal(os.getenv("STOP_LOSS_PCT", "0.01"))
    take_profit_pct: Decimal = Decimal(os.getenv("TAKE_PROFIT_PCT", "0.02"))
    enable_risk_mgmt: bool = True
    poll_interval_sec: float = 1.0


class BybitTestnetConfig:
    """
    Environment-backed configuration for Bybit Testnet clients and Web UI.
    """

    def __init__(self) -> None:
        self.api_key: str = os.getenv("BYBIT_TESTNET_API_KEY", "")
        self.api_secret: str = os.getenv("BYBIT_TESTNET_API_SECRET", "")
        self.underlying_symbol: str = os.getenv("UNDERLYING_SYMBOL", "BTCUSDT")
        self.trade_size: Decimal = Decimal(os.getenv("TRADE_SIZE", "0.01"))
        self.stop_loss_pct: Decimal = Decimal(os.getenv("STOP_LOSS_PCT", "0.01"))
        self.take_profit_pct: Decimal = Decimal(os.getenv("TAKE_PROFIT_PCT", "0.02"))
        self.web_port: int = int(os.getenv("WEB_PORT", "8000"))
        self.web_host: str = os.getenv("WEB_HOST", "0.0.0.0")
        self.testnet: bool = True
        self.simulate_feed: bool = os.getenv("SIMULATE_FEED", "false").lower() in ("true", "1", "yes")

    @property
    def has_credentials(self) -> bool:
        """Check if valid credentials were provided in the environment."""
        return bool(
            self.api_key
            and self.api_secret
            and self.api_key != "your_bybit_testnet_api_key"
            and self.api_secret != "your_bybit_testnet_api_secret"
        )

    def get_strategy_config(self) -> StraddleStrategyConfig:
        """Instantiate a typed StrategyConfig using current parameters."""
        return StraddleStrategyConfig(
            underlying_symbol=self.underlying_symbol,
            trade_size=self.trade_size,
            stop_loss_pct=self.stop_loss_pct,
            take_profit_pct=self.take_profit_pct,
            enable_risk_mgmt=True,
            poll_interval_sec=1.0,
        )


app_config = BybitTestnetConfig()
