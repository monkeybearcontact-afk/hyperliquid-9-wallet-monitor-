import os
import json
import time
import threading
from datetime import datetime, timezone

import requests
import websocket
from flask import Flask, jsonify


# ============================================================
# HYPERLIQUID 9-WALLET HOSTING TEST
# ============================================================

WALLETS = [
    "0x523979b277a398574a81da1d3511cb84823ce040",
    "0xb854f7f40f0f3e1801fb6d8828b6f6dd97ebf33b",
    "0x7e09dfbba23d078e16f038b1ed4c558185c4be1e",
    "0x3f96c4df1e76902985d3dbd979af54b487f29671",
    "0x8477e447846c758f5a675856001ea72298fd9cb5",
    "0x21eaaee21905a7b9fbbb093caba2ee27fc1e9352",
    "0xb9149106c190095f0e548ed3fc436b8684e77029",
    "0x0c4695f6b8c3209cb3dfecf2010098065623e94d",
    "0x00000000000dfd47d200dc26e4e5c79b7e6de984",
]

WS_URL = "wss://api.hyperliquid.xyz/ws"

app = Flask(__name__)

ws = None
ws_lock = threading.Lock()

connected = False
last_message_time = None
last_connection_time = None
connection_count = 0
messages_received = 0

# Used to ignore the initial userFills snapshot.
snapshot_seen = {wallet: False for wallet in WALLETS}


# ============================================================
# LOGGING
# ============================================================

def log(message):
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    print(f"[{now}] {message}", flush=True)


# ============================================================
# HEALTH ENDPOINT
# ============================================================

@app.route("/")
def home():
    return jsonify({
        "service": "hyperliquid-9-wallet-monitor",
        "status": "running",
        "connected": connected,
        "wallets": len(WALLETS),
        "messages_received": messages_received,
        "connection_count": connection_count,
        "last_message_time": last_message_time,
    })


@app.route("/health")
def health():
    return jsonify({
        "status": "ok",
        "hyperliquid_connected": connected,
        "wallets": len(WALLETS),
        "messages_received": messages_received,
        "connection_count": connection_count,
        "last_message_time": last_message_time,
    }), 200


# ============================================================
# HYPERLIQUID MESSAGE HANDLER
# ============================================================

def on_message(wsapp, message):
    global last_message_time
    global messages_received

    last_message_time = datetime.now(timezone.utc).isoformat()
    messages_received += 1

    try:
        obj = json.loads(message)
    except Exception:
        log(f"NON-JSON MESSAGE: {message[:500]}")
        return

    channel = obj.get("channel")

    # Server pong
    if channel == "pong":
        log("HYPERLIQUID PONG")
        return

    # Subscription response
    if channel == "subscriptionResponse":
        log(f"SUBSCRIPTION RESPONSE: {obj}")
        return

    # User fills
    if channel == "userFills":
        data = obj.get("data", {})

        # Hyperliquid may return the fills in a list.
        fills = data if isinstance(data, list) else data.get("fills", [])

        if not isinstance(fills, list):
            fills = []

        # Determine wallet when possible.
        wallet = data.get("user") if isinstance(data, dict) else None

        if wallet and wallet.lower() in snapshot_seen:
            wallet_key = wallet.lower()
        else:
            wallet_key = None

        # The first userFills message after subscribing is the
        # initial snapshot. We deliberately do NOT treat those
        # as new live trades.
        if wallet_key and not snapshot_seen[wallet_key]:
            snapshot_seen[wallet_key] = True
            log(
                f"INITIAL SNAPSHOT RECEIVED | "
                f"{wallet_key} | fills={len(fills)}"
            )
            return

        # If the wallet cannot be identified, print the message
        # for debugging rather than pretending it is a live fill.
        if not wallet_key:
            log(f"USER FILLS MESSAGE: {json.dumps(obj)[:3000]}")
            return

        # Live fills
        for fill in fills:
            log(
                "LIVE FILL | "
                f"wallet={wallet_key} | "
                f"coin={fill.get('coin')} | "
                f"side={fill.get('side')} | "
                f"px={fill.get('px')} | "
                f"sz={fill.get('sz')} | "
                f"dir={fill.get('dir')} | "
                f"startPosition={fill.get('startPosition')} | "
                f"closedPnl={fill.get('closedPnl')} | "
                f"tid={fill.get('tid')}"
            )

        return

    # Other messages are useful during the hosting test.
    log(f"OTHER MESSAGE | channel={channel} | {json.dumps(obj)[:1500]}")


# ============================================================
# HYPERLIQUID OPEN
# ============================================================

def on_open(wsapp):
    global connected
    global last_connection_time

    connected = True
    last_connection_time = datetime.now(timezone.utc).isoformat()

    log("=" * 70)
    log("HYPERLIQUID WEBSOCKET CONNECTED")
    log("=" * 70)

    # Reset snapshot state for this new connection.
    for wallet in WALLETS:
        snapshot_seen[wallet] = False

    # Subscribe all 9 wallets.
    for wallet in WALLETS:
        subscription = {
            "method": "subscribe",
            "subscription": {
                "type": "userFills",
                "user": wallet,
            },
        }

        wsapp.send(json.dumps(subscription))

        log(f"SUBSCRIBED userFills | {wallet}")

    log(f"TOTAL SUBSCRIPTIONS: {len(WALLETS)}")


# ============================================================
# HYPERLIQUID CLOSE
# ============================================================

def on_close(wsapp, close_status_code, close_msg):
    global connected

    connected = False

    log(
        f"HYPERLIQUID DISCONNECTED | "
        f"code={close_status_code} | msg={close_msg}"
    )


# ============================================================
# HYPERLIQUID ERROR
# ============================================================

def on_error(wsapp, error):
    global connected

    connected = False

    log(f"HYPERLIQUID ERROR | {error}")


# ============================================================
# APPLICATION HEARTBEAT
# ============================================================

def heartbeat_loop():
    while True:
        time.sleep(20)

        with ws_lock:
            current_ws = ws

        if current_ws is not None and connected:
            try:
                current_ws.send(json.dumps({"method": "ping"}))
                log("APPLICATION PING SENT")
            except Exception as e:
                log(f"HEARTBEAT SEND ERROR | {e}")


# ============================================================
# WEBSOCKET RECONNECT LOOP
# ============================================================

def websocket_loop():
    global ws
    global connection_count

    retry_delay = 5

    while True:
        try:
            log("CONNECTING TO HYPERLIQUID...")
            log(f"URL: {WS_URL}")

            new_ws = websocket.WebSocketApp(
                WS_URL,
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close,
            )

            with ws_lock:
                ws = new_ws

            connection_count += 1

            # websocket-client automatic ping is disabled.
            # We use Hyperliquid's application-level ping instead.
            new_ws.run_forever(
                ping_interval=None,
                ping_timeout=None,
                reconnect=0,
            )

        except Exception as e:
            log(f"WEBSOCKET LOOP EXCEPTION | {e}")

        finally:
            connected = False

            with ws_lock:
                ws = None

        log(f"RECONNECTING IN {retry_delay} SECONDS...")
        time.sleep(retry_delay)

        # Gradually increase retry delay, capped at 60 seconds.
        retry_delay = min(retry_delay * 2, 60)


# ============================================================
# OPTIONAL HTTP CONNECTIVITY TEST
# ============================================================

def http_test():
    try:
        response = requests.post(
            "https://api.hyperliquid.xyz/info",
            json={
                "type": "meta",
            },
            timeout=15,
        )

        log(
            f"HYPERLIQUID HTTP TEST | "
            f"status={response.status_code}"
        )

    except Exception as e:
        log(f"HYPERLIQUID HTTP TEST FAILED | {e}")


# ============================================================
# STARTUP
# ============================================================

def start_background_threads():
    log("=" * 70)
    log("STARTING HYPERLIQUID 9-WALLET MONITOR")
    log("=" * 70)
    log(f"WALLETS: {len(WALLETS)}")
    log(f"WEBSOCKET: {WS_URL}")

    http_test()

    heartbeat_thread = threading.Thread(
        target=heartbeat_loop,
        daemon=True,
        name="heartbeat",
    )
    heartbeat_thread.start()

    websocket_thread = threading.Thread(
        target=websocket_loop,
        daemon=True,
        name="hyperliquid-websocket",
    )
    websocket_thread.start()


if __name__ == "__main__":
    start_background_threads()

    port = int(os.environ.get("PORT", "10000"))

    log(f"HTTP HEALTH SERVER STARTING ON 0.0.0.0:{port}")

    app.run(
        host="0.0.0.0",
        port=port,
        threaded=True,
        use_reloader=False,
)
