import asyncio
import socket
from aiohttp import web

RTL_TCP_HOST = "193.24.208.6"
RTL_TCP_PORT = 1240

clients = set()

async def rtl_tcp_reader():
    while True:
        try:
            print(f"[RTL-TCP] Connecting to {RTL_TCP_HOST}:{RTL_TCP_PORT}...")
            reader, writer = await asyncio.open_connection(RTL_TCP_HOST, RTL_TCP_PORT)
            print("[RTL-TCP] Connected! Streaming IQ data...")
            while True:
                data = await reader.read(16384)
                if not data:
                    break
                for ws in list(clients):
                    try:
                        await ws.send_bytes(b"SND " + data)
                    except Exception:
                        clients.remove(ws)
        except Exception as e:
            print(f"[RTL-TCP] Error: {e}. Reconnecting in 3s...")
            await asyncio.sleep(3)

async def kiwisdr_ws_handler(request):
    ws = web.WebSocketResponse()
    await ws.prepare(request)
    clients.add(ws)
    print(f"[Kiwi Server] Client connected. Active clients: {len(clients)}")

    try:
        await ws.send_str("MSG kiwi_version=1.711 name=SimTX-Virtual-KiwiSDR")
        await ws.send_str("MSG sample_rate=1250000")

        async for msg in ws:
            if msg.type == web.WSMsgType.TEXT:
                text = msg.data
                if "SET keepalive" in text:
                    await ws.send_str("MSG keepalive_ack")
                elif "SET AR_RATE" in text:
                    await ws.send_str("MSG ar_rate_ok")
                elif "SET mod=" in text:
                    await ws.send_str("MSG mod_ok")
    finally:
        clients.remove(ws)
        print(f"[Kiwi Server] Client disconnected. Remaining: {len(clients)}")
    return ws

async def start_tasks(app):
    app["rtl_task"] = asyncio.create_task(rtl_tcp_reader())

async def cleanup_tasks(app):
    app["rtl_task"].cancel()

app = web.Application()
app.router.add_get('/', kiwisdr_ws_handler)
app.router.add_get('/ws', kiwisdr_ws_handler)
app.router.add_get('/kiwi/', kiwisdr_ws_handler)

app.on_startup.append(start_tasks)
app.on_cleanup.append(cleanup_tasks)

if __name__ == '__main__':
    web.run_app(app, host='0.0.0.0', port=8073)
