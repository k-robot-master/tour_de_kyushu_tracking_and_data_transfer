"""
シンプル版：大会デモ用 データ受信→保存→転送 +（任意）テスト注入

【設計の要点】
- 受信（WebSocket）は再接続ループで常時トライ。落ちても自動リトライ。
- 転送（HTTP POST）は WebSocket の接続有無に関係なく常時稼働。
- 保存（ファイル追記）はキュー＋ライタータスクで制御し、受信を塞がない。
- （任意）テスト注入はフラグでON/OFF。WSが落ちていても動作可能。

必要ライブラリ: websockets, aiohttp
  pip install websockets aiohttp
"""

import asyncio
import json
from datetime import datetime

import websockets
from aiohttp import ClientSession, ClientTimeout

# ====== 設定 ======

EXTERNAL_WS_URL = "wss://hawkcast-data.n-sportstracking-lab.com/api/ws/hawkcast/v3/220"
API_KEY         = "cm9479NsDLp4YCKJjE3a55ju"

# EXTERNAL_WS_URL = "ws://localhost:8765"
# API_KEY         = "websocket_key"

# POST_URL        = "https://tdk2025.k-robot.jp/gps_tracking_server/post_hawkcast_tracking_data"
POST_URL        = "https://127.0.0.1/gps_tracking_server/post_hawkcast_tracking_data"

def create_log_filename(prefix="log", ext="json"):
    # 現在日時を YYYYMMDD_HHMMSS 形式に
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{timestamp}_{prefix}.{ext}"
    return filename

# LOG_FILE        = "tracking_data.jsonl_time_test.json"
LOG_FILE        =  create_log_filename("tracking_data")

PING_INTERVAL   = None   # WebSocketの自動ping間隔（秒）
PING_TIMEOUT    = 10   # ping応答の待ち時間（秒）
RETRY_WAIT      = 5    # WS切断時の再接続待ち（秒）
POST_INTERVAL   = 5   # 転送周期（秒）
HTTP_TIMEOUT    = 10   # 転送HTTPの総タイムアウト（秒）

# ====== 負荷テスト用フラグ ======
# ** 大会本番用ではFalseを確認 **
TEST_FILE_WRITE        = False   # ファイル追記ストレス
TEST_ITEMS_PER_TICK    = 100
TEST_TICKS             = 50
TEST_TICK_INTERVAL     = 0.05

TEST_INJECT            = False   # latest_map にダミーデータを注入
TEST_ID_COUNT          = 100
TEST_INJECT_INTERVAL   = 1.0
# ============================

# 最新データをここに溜める（id -> data）
latest_map = {}

def now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


# ---------- 1) 受信ループ ----------
async def receive_loop(ws):
    while True:
        msg = await ws.recv()
        try:
            data = json.loads(msg)
        except json.JSONDecodeError:
            continue

        if data.get("type") != "gps":
            continue

        _id = data.get("id")
        if _id is None:
            continue

        latest_map[_id] = data

        try:
            write_queue.put_nowait(data)
        except asyncio.QueueFull:
            _ = write_queue.get_nowait()
            write_queue.put_nowait(data)


# ---------- 2) ファイルライター ----------
async def file_writer_loop():
    while True:
        data = await write_queue.get()
        try:
            with open(LOG_FILE, "a", encoding="utf-8") as f:
                f.write(json.dumps(data, ensure_ascii=False) + "\n")
                f.flush()
        except Exception as e:
            log("FILE", f"write error: {e}")


import ssl
import aiohttp  # 念のため上に追記しておいて

# ---------- 3) 定期転送 ----------
async def forward_loop():

    timeout = ClientTimeout(total=HTTP_TIMEOUT)

    # ===== 検証用 =====
    # 🔐 自己署名証明書を許容するSSL設定
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE

    # ✅ ClientSessionにSSL無視のコネクタを渡す
    connector = aiohttp.TCPConnector(ssl=ssl_context)
    # ===================

    async with ClientSession(timeout=timeout, connector=connector) as session: # 検証用
        while True:
            await asyncio.sleep(POST_INTERVAL)

            if not latest_map:
                continue

            batch = dict(latest_map)

            try:
                async with session.post(POST_URL, json=batch) as resp:
                    if resp.status == 200:
                        log("DATA_POST", f"OK ({len(batch)} items)")
                    else:
                        body = await resp.text()
                        log("DATA_POST", f"NG {resp.status} {body[:200]}")
            except Exception as e:
                log("DATA_POST", f"Error: {e}")


# ---------- 4) WebSocket接続管理 ----------
async def ws_runner():
    while True:
        try:
            async with websockets.connect(
                EXTERNAL_WS_URL,
                ping_interval=PING_INTERVAL,
                ping_timeout=PING_TIMEOUT,
                close_timeout=5,
            ) as ws_connection:
                
                _last_connected_at = datetime.now()
                log("WS","connected")

                await ws_connection.send(API_KEY)
                log("WS","API key sent")

                # 並行タスク: 受信ループ + keepalive
                recv_task = asyncio.create_task(receive_loop(ws_connection))
                ka_task   = asyncio.create_task(keepalive_loop(ws_connection))

                done, pending = await asyncio.wait(
                    {recv_task, ka_task},
                    return_when=asyncio.FIRST_EXCEPTION
                )
                for t in pending: t.cancel()

        except websockets.ConnectionClosed as e:
            if _last_connected_at:
                delta = (datetime.now() - _last_connected_at).total_seconds()
                log("WS", f"closed after {delta:.1f}s code={e.code} reason={e.reason}")
            await asyncio.sleep(RETRY_WAIT)
        except Exception as e:
            log("WS", f"error: {e}")
            await asyncio.sleep(RETRY_WAIT)

# -------------- keep alive --------------
HEARTBEAT_INTERVAL = 10  # 10秒ごとにping送信

async def keepalive_loop(ws):
    """
    サーバに定期的にpingを送ってpongを待つ。
    """
    while True:
        try:
            waiter = await ws.ping()
            await waiter  # pongが返るまで待機
            log("WS", "ping->pong OK")
        except Exception as e:
            log("WS", f"keepalive error: {e}")
            break
        await asyncio.sleep(HEARTBEAT_INTERVAL)

# ---------- 5) 負荷テスト：ファイル書き出しストレス ----------
def make_fake(i: int, j: int) -> dict:
    return {
        "type": "gps",
        "id": f"test_{i:04d}_{j:03d}",
        "ts": datetime.now().isoformat(),
        "lat": 33.0 + (i % 50) * 0.0001,
        "lon": 130.0 + (j % 50) * 0.0001,
        "spd": (i + j) % 50,
    }

async def file_write_stress():
    log("TEST", "start file write stress")
    for i in range(TEST_TICKS):
        for j in range(TEST_ITEMS_PER_TICK):
            try:
                write_queue.put_nowait(make_fake(i, j))
            except asyncio.QueueFull:
                _ = write_queue.get_nowait()
                write_queue.put_nowait(make_fake(i, j))
        await asyncio.sleep(TEST_TICK_INTERVAL)
    log("TEST", "done file write stress")


# ---------- 6) 負荷テスト：latest_map注入 ----------
def fake_gps_for_id(k: int) -> dict:
    return {
        "type": "gps",
        "id": f"fake_{k:04d}",
        "ts": datetime.now().isoformat(),
        "lat": 33.0 + (k % 50) * 0.0001,
        "lon": 130.0 + (k % 50) * 0.0001,
        "spd": k % 40,
    }

async def injector_latest_map():
    log("TEST", "start injector (latest_map)")
    n = TEST_ID_COUNT
    while True:
        for k in range(n):
            d = fake_gps_for_id(k)
            latest_map[d["id"]] = d
            try:
                write_queue.put_nowait(d)
            except asyncio.QueueFull:
                _ = write_queue.get_nowait()
                write_queue.put_nowait(d)
        await asyncio.sleep(TEST_INJECT_INTERVAL)


# ---------- 7) 全体オーケストレーション ----------
async def main_async():

    global write_queue
    write_queue = asyncio.Queue(maxsize=5000)  # ★ここで作る

    tasks = [
        asyncio.create_task(forward_loop()),
        asyncio.create_task(ws_runner()),
        asyncio.create_task(file_writer_loop()),
    ]

    if TEST_FILE_WRITE:
        tasks.append(asyncio.create_task(file_write_stress()))
    if TEST_INJECT:
        tasks.append(asyncio.create_task(injector_latest_map()))

    await asyncio.gather(*tasks)


# ---------- ロガー ----------
def log(cat: str, msg: str):
    print(now_str(), f"[{cat}] {msg}")


# ---------- エントリーポイント ----------
def main():
    log("BOOT", f"Websocket_URL={EXTERNAL_WS_URL}s POST_URL={POST_URL}s  LOG_FILE={LOG_FILE} ")
    log("BOOT", f"POST_INTERVAL={POST_INTERVAL}s HTTP_TIMEOUT={HTTP_TIMEOUT}s TEST_FILE_WRITE={TEST_FILE_WRITE} TEST_INJECT={TEST_INJECT}")
    
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
