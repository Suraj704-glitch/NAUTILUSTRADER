/**
 * NautilusTrader BTC ATM Straddle Real-Time Dashboard Client.
 * Handles WebSocket streaming, state hydration, risk bar calculations, and OMS logs.
 */

class StraddleDashboard {
  constructor() {
    this.ws = null;
    this.reconnectInterval = 2000;
    this.maxLogLines = 150;
    this.heartbeatTimer = null;

    // Cache DOM Elements
    this.dom = {
      badge: document.getElementById("connection-badge"),
      badgeText: document.getElementById("connection-text"),
      navBtcPrice: document.getElementById("nav-btc-price"),
      
      atmStrike: document.getElementById("metric-atm-strike"),
      expiry: document.getElementById("metric-expiry"),
      contractStatus: document.getElementById("tag-contract-status"),
      tradeSizeBadge: document.getElementById("trade-size-badge"),

      currentCombined: document.getElementById("metric-current-combined"),
      entryCombined: document.getElementById("metric-entry-combined"),
      
      cardDrift: document.getElementById("card-drift"),
      driftPct: document.getElementById("metric-drift-pct"),
      driftTag: document.getElementById("tag-drift-state"),
      totalPnl: document.getElementById("metric-total-pnl"),

      slBpsDist: document.getElementById("sl-bps-dist"),
      tpBpsDist: document.getElementById("tp-bps-dist"),
      slFill: document.getElementById("sl-progress-fill"),
      tpFill: document.getElementById("tp-progress-fill"),
      riskTag: document.getElementById("tag-risk-status"),

      callInstId: document.getElementById("call-inst-id"),
      callQty: document.getElementById("call-qty"),
      callEntryPx: document.getElementById("call-entry-px"),
      callAskPx: document.getElementById("call-ask-px"),
      callPnl: document.getElementById("call-pnl"),
      callStatus: document.getElementById("call-status"),

      putInstId: document.getElementById("put-inst-id"),
      putQty: document.getElementById("put-qty"),
      putEntryPx: document.getElementById("put-entry-px"),
      putAskPx: document.getElementById("put-ask-px"),
      putPnl: document.getElementById("put-pnl"),
      putStatus: document.getElementById("put-status"),

      consoleFeed: document.getElementById("console-feed"),
      btnClearLog: document.getElementById("btn-clear-log"),
      btnManualExit: document.getElementById("btn-manual-exit"),
      footerState: document.getElementById("footer-state-machine"),
    };

    this.initEventListeners();
    this.connectWebSocket();
  }

  initEventListeners() {
    this.dom.btnClearLog.addEventListener("click", () => {
      this.dom.consoleFeed.innerHTML = "";
    });

    this.dom.btnManualExit.addEventListener("click", async () => {
      if (confirm("Are you sure you want to trigger an EMERGENCY FLATTEN of all short straddle legs?")) {
        try {
          const res = await fetch("/api/manual_exit", { method: "POST" });
          const data = await res.json();
          this.appendLog("USER ACTION", `Emergency flatten requested: ${data.message || data.status}`, "risk");
        } catch (err) {
          this.appendLog("ERROR", `Failed to send manual exit command: ${err}`, "warn");
        }
      }
    });
  }

  connectWebSocket() {
    const protocol = window.location.protocol === "https:" ? "wss:" : "ws:";
    const host = window.location.host || "localhost:8000";
    const wsUrl = `${protocol}//${host}/ws/stream`;

    this.updateConnectionStatus("connecting", "CONNECTING TESTNET");

    try {
      this.ws = new WebSocket(wsUrl);

      this.ws.onopen = () => {
        this.updateConnectionStatus("connected", "CONNECTED // BYBIT TESTNET");
        this.appendLog("WEBSOCKET", "Connected to NautilusTrader real-time stream", "info");
        
        // Start ping heartbeat
        clearInterval(this.heartbeatTimer);
        this.heartbeatTimer = setInterval(() => {
          if (this.ws && this.ws.readyState === WebSocket.OPEN) {
            this.ws.send("ping");
          }
        }, 10000);
      };

      this.ws.onmessage = (event) => {
        if (event.data === "pong") return;
        try {
          const msg = JSON.parse(event.data);
          this.handleEvent(msg);
        } catch (err) {
          console.error("Failed to parse websocket message:", err, event.data);
        }
      };

      this.ws.onclose = () => {
        this.updateConnectionStatus("disconnected", "DISCONNECTED // RECONNECTING");
        clearInterval(this.heartbeatTimer);
        setTimeout(() => this.connectWebSocket(), this.reconnectInterval);
      };

      this.ws.onerror = (err) => {
        console.warn("WebSocket error:", err);
      };
    } catch (e) {
      this.updateConnectionStatus("disconnected", "CONNECTION ERROR");
      setTimeout(() => this.connectWebSocket(), this.reconnectInterval);
    }
  }

  updateConnectionStatus(state, text) {
    this.dom.badge.className = `status-badge ${state}`;
    this.dom.badgeText.innerText = text;
  }

  handleEvent(payload) {
    const { event, data, logs } = payload;

    if (logs && Array.isArray(logs)) {
      logs.forEach((item) => this.handleEvent(item));
    }

    switch (event) {
      case "snapshot":
        this.hydrateSnapshot(data);
        break;
      case "discovery_update":
        this.handleDiscovery(data);
        break;
      case "ticker_update":
        this.handleTicker(data);
        break;
      case "position_update":
        this.handlePositionUpdate(data);
        break;
      case "order_event":
        this.appendLog("OMS EVENT", `${data.type} :: ClientOrderID=${data.id}`, "info");
        break;
      case "risk_alert":
        this.handleRiskAlert(data);
        break;
      case "order_entry_submitted":
        this.appendLog("OMS EXEC", `Submitted Straddle SELL Orders: Call=${data.call_order_id}, Put=${data.put_order_id}`, "fill");
        break;
      default:
        break;
    }
  }

  hydrateSnapshot(data) {
    if (!data) return;

    if (data.underlying_price && data.underlying_price !== "0") {
      this.dom.navBtcPrice.innerText = `$${parseFloat(data.underlying_price).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
    }

    if (data.selected_strike && data.selected_strike !== "-") {
      this.dom.atmStrike.innerText = `$${parseFloat(data.selected_strike).toLocaleString('en-US')}`;
      this.dom.contractStatus.innerText = "ACTIVE ATM";
      this.dom.contractStatus.className = "tag-pill tag-info";
    }

    if (data.call_instrument_id && data.call_instrument_id !== "-") {
      this.dom.callInstId.innerText = data.call_instrument_id;
    }

    if (data.put_instrument_id && data.put_instrument_id !== "-") {
      this.dom.putInstId.innerText = data.put_instrument_id;
    }

    if (data.trade_size) {
      this.dom.tradeSizeBadge.innerText = `Size: ${data.trade_size} BTC`;
      this.dom.callQty.innerText = data.trade_size;
      this.dom.putQty.innerText = data.trade_size;
    }

    if (data.entry_combined_premium && data.entry_combined_premium !== "0") {
      this.dom.entryCombined.innerText = `$${parseFloat(data.entry_combined_premium).toFixed(2)}`;
    }

    if (data.call_entry_price && data.call_entry_price !== "0") {
      this.dom.callEntryPx.innerText = `$${parseFloat(data.call_entry_price).toFixed(2)}`;
      this.dom.callStatus.innerText = "SHORT FILLED";
      this.dom.callStatus.className = "status-tag text-emerald";
    }

    if (data.put_entry_price && data.put_entry_price !== "0") {
      this.dom.putEntryPx.innerText = `$${parseFloat(data.put_entry_price).toFixed(2)}`;
      this.dom.putStatus.innerText = "SHORT FILLED";
      this.dom.putStatus.className = "status-tag text-emerald";
    }

    if (data.is_closed) {
      this.dom.footerState.innerText = "CLOSED / SQUARED OFF";
      this.dom.footerState.className = "font-mono text-rose";
    } else if (data.is_entered) {
      this.dom.footerState.innerText = "ACTIVE SHORT STRADDLE";
      this.dom.footerState.className = "font-mono text-emerald";
    }
  }

  handleDiscovery(data) {
    if (data.underlying_price) {
      this.dom.navBtcPrice.innerText = `$${parseFloat(data.underlying_price).toLocaleString('en-US', { minimumFractionDigits: 2 })}`;
    }

    if (data.selected_strike) {
      this.dom.atmStrike.innerText = `$${parseFloat(data.selected_strike).toLocaleString('en-US')}`;
      this.dom.contractStatus.innerText = "ACTIVE ATM";
      this.dom.contractStatus.className = "tag-pill tag-info";
    }

    if (data.selected_expiry_ns) {
      const expDate = new Date(data.selected_expiry_ns / 1000000);
      this.dom.expiry.innerText = `Expiry: ${expDate.toUTCString().slice(5, 16)}`;
    }

    if (data.call_instrument_id) this.dom.callInstId.innerText = data.call_instrument_id;
    if (data.put_instrument_id) this.dom.putInstId.innerText = data.put_instrument_id;

    this.appendLog("DISCOVERY", `Matched Straddle ATM Strike: $${data.selected_strike} for Call: ${data.call_instrument_id}, Put: ${data.put_instrument_id}`, "info");
  }

  handleTicker(data) {
    if (data.underlying_price && data.underlying_price !== "0") {
      this.dom.navBtcPrice.innerText = `$${parseFloat(data.underlying_price).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
    }

    if (data.current_combined_ask && data.current_combined_ask !== "0") {
      this.dom.currentCombined.innerText = `$${parseFloat(data.current_combined_ask).toFixed(2)}`;
    }

    if (data.entry_combined_premium && data.entry_combined_premium !== "0") {
      this.dom.entryCombined.innerText = `$${parseFloat(data.entry_combined_premium).toFixed(2)}`;
    }

    // Call Leg quotes & PnL
    if (data.call_ask && data.call_ask !== "0") {
      this.dom.callAskPx.innerText = `$${parseFloat(data.call_ask).toFixed(2)}`;
    }
    if (data.call_pnl) {
      const cPnl = parseFloat(data.call_pnl);
      this.dom.callPnl.innerText = `${cPnl >= 0 ? '+' : ''}$${cPnl.toFixed(2)}`;
      this.dom.callPnl.className = `font-mono ${cPnl >= 0 ? 'text-emerald' : 'text-rose'}`;
    }

    // Put Leg quotes & PnL
    if (data.put_ask && data.put_ask !== "0") {
      this.dom.putAskPx.innerText = `$${parseFloat(data.put_ask).toFixed(2)}`;
    }
    if (data.put_pnl) {
      const pPnl = parseFloat(data.put_pnl);
      this.dom.putPnl.innerText = `${pPnl >= 0 ? '+' : ''}$${pPnl.toFixed(2)}`;
      this.dom.putPnl.className = `font-mono ${pPnl >= 0 ? 'text-emerald' : 'text-rose'}`;
    }

    // Total Unrealized PnL
    if (data.total_pnl) {
      const totPnl = parseFloat(data.total_pnl);
      this.dom.totalPnl.innerText = `${totPnl >= 0 ? '+' : ''}$${totPnl.toFixed(2)}`;
      this.dom.totalPnl.className = `bold ${totPnl >= 0 ? 'text-emerald' : 'text-rose'}`;
    }

    // Drift Percentage & Visual Card Transition
    if (data.premium_drift_pct !== undefined) {
      const drift = parseFloat(data.premium_drift_pct);
      const driftPctFormatted = (drift * 100).toFixed(2);
      this.dom.driftPct.innerText = `${drift >= 0 ? '+' : ''}${driftPctFormatted}%`;

      this.dom.cardDrift.classList.remove("drift-warning", "drift-profit");

      if (drift > 0.005) {
        // Approaching +1% SL -> Rose Red warning
        this.dom.cardDrift.classList.add("drift-warning");
        this.dom.driftPct.className = "hero-metric text-rose";
        this.dom.driftTag.innerText = "APPROACHING SL";
        this.dom.driftTag.className = "tag-pill tag-warning";
      } else if (drift < -0.01) {
        // Approaching -2% TP -> Emerald Green profit
        this.dom.cardDrift.classList.add("drift-profit");
        this.dom.driftPct.className = "hero-metric text-emerald";
        this.dom.driftTag.innerText = "APPROACHING TP";
        this.dom.driftTag.className = "tag-pill tag-info";
      } else {
        this.dom.driftPct.className = "hero-metric";
        this.dom.driftTag.innerText = "STABLE";
        this.dom.driftTag.className = "tag-pill tag-neutral";
      }

      // Update SL and TP Progress Bars
      // Stop Loss trigger at +1.00% (drift = 0.01)
      const slRatio = Math.max(0, Math.min(100, (drift / 0.01) * 100));
      this.dom.slFill.style.width = `${slRatio}%`;
      this.dom.slBpsDist.innerText = `${data.sl_dist_bps} bps away`;

      // Take Profit trigger at -2.00% (drift = -0.02)
      const tpRatio = Math.max(0, Math.min(100, (-drift / 0.02) * 100));
      this.dom.tpFill.style.width = `${tpRatio}%`;
      this.dom.tpBpsDist.innerText = `${data.tp_dist_bps} bps away`;
    }
  }

  handlePositionUpdate(data) {
    if (data.entry_combined_premium) {
      this.dom.entryCombined.innerText = `$${parseFloat(data.entry_combined_premium).toFixed(2)}`;
    }
    if (data.call_entry_price) {
      this.dom.callEntryPx.innerText = `$${parseFloat(data.call_entry_price).toFixed(2)}`;
      this.dom.callStatus.innerText = "SHORT FILLED";
      this.dom.callStatus.className = "status-tag text-emerald";
    }
    if (data.put_entry_price) {
      this.dom.putEntryPx.innerText = `$${parseFloat(data.put_entry_price).toFixed(2)}`;
      this.dom.putStatus.innerText = "SHORT FILLED";
      this.dom.putStatus.className = "status-tag text-emerald";
    }

    this.dom.footerState.innerText = "ACTIVE SHORT STRADDLE";
    this.dom.footerState.className = "font-mono text-emerald";
    this.appendLog("POSITION", `Straddle Entered! Call=${data.call_entry_price}, Put=${data.put_entry_price}, Combined=${data.entry_combined_premium}`, "fill");
  }

  handleRiskAlert(data) {
    this.dom.riskTag.innerText = "TRIGGERED";
    this.dom.riskTag.className = "tag-pill tag-warning";
    this.dom.footerState.innerText = `EXIT: ${data.reason}`;
    this.dom.footerState.className = "font-mono text-rose";

    this.appendLog("RISK TRIGGER", `Automated Exit Triggered: ${data.reason}. Buyback Cost=${data.current_combined_ask}, Entry=${data.entry_combined_premium}`, "risk");
  }

  appendLog(category, msg, level = "info") {
    const d = new Date();
    const ts = d.toTimeString().split(" ")[0];

    const line = document.createElement("div");
    line.className = `log-line ${level}`;
    line.innerHTML = `<span class="log-ts">[${ts}]</span> <span class="log-msg"><strong class="terminal-prompt">[${category}]</strong> ${msg}</span>`;

    this.dom.consoleFeed.appendChild(line);

    // Limit log line count
    while (this.dom.consoleFeed.children.length > this.maxLogLines) {
      this.dom.consoleFeed.removeChild(this.dom.consoleFeed.firstChild);
    }

    // Auto-scroll to bottom
    this.dom.consoleFeed.scrollTop = this.dom.consoleFeed.scrollHeight;
  }
}

// Initialize on DOM load
window.addEventListener("DOMContentLoaded", () => {
  new StraddleDashboard();
});
