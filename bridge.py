import asyncio
import struct
from aiohttp import web

# RTL_TCP ресивер симулятора SimTX
RTL_TCP_HOST = "193.24.208.6"
RTL_TCP_PORT = 1240

clients = set()

async def rtl_tcp_reader():
    """Фоновое чтение IQ-данных из rtl-tcp и рассылка KiwiSDR-клиентам."""
    while True:
        try:
            print(f"[SimTX Source] Подключение к rtl-tcp {RTL_TCP_HOST}:{RTL_TCP_PORT}...")
            reader, writer = await asyncio.open_connection(RTL_TCP_HOST, RTL_TCP_PORT)
            print("[SimTX Source] Успешно подключено к ресиверу!")
            
            while True:
                data = await reader.read(16384)
                if not data:
                    break
                
                # Пересылаем сырые IQ-байты всем веб-клиентам
                for ws in list(clients):
                    try:
                        await ws.send_bytes(b"SND " + data)
                    except Exception:
                        clients.remove(ws)
                        
        except Exception as e:
            print(f"[SimTX Source] Ошибка: {e}. Повтор через 5 сек...")
            await asyncio.sleep(5)

async def kiwisdr_ws_handler(request):
    """Обработчик WebSocket для эмуляции протокола KiwiSDR."""
    ws = web.WebSocketResponse()
    await ws.prepare(request)
    clients.add(ws)
    print(f"[Kiwi Emulator] Клиент подключился. Всего клиентов: {len(clients)}")

    try:
        # Приветственные команды протокола KiwiSDR
        await ws.send_str("MSG kiwi_version=1.400 name=SimTX-Virtual-KiwiSDR")
        await ws.send_str("MSG sample_rate=1250000")

        async for msg in ws:
            if msg.type == web.WSMsgType.TEXT:
                text = msg.data
                # Автоматический ответ на рукопожатия и таймауты
                if "SET keepalive" in text:
                    await ws.send_str("MSG keepalive_ack")
                elif "SET AR_RATE" in text:
                    await ws.send_str("MSG ar_rate_ok")
                elif "SET mod=" in text:
                    await ws.send_str("MSG mod_ok")

    finally:
        clients.remove(ws)
        print(f"[Kiwi Emulator] Клиент отключился. Осталось: {len(clients)}")
    return ws

async def start_tasks(app):
    app['rtl_task'] = asyncio.create_task(rtl_tcp_reader())

async def cleanup_tasks(app):
    app['rtl_task'].cancel()
    await app['rtl_task']

app = web.Application()
app.router.add_get('/', kiwisdr_ws_handler)
app.router.add_get('/ws', kiwisdr_ws_handler)
app.router.add_get('/kiwi/', kiwisdr_ws_handler)

app.on_startup.append(start_tasks)
app.on_cleanup.append(cleanup_tasks)

if __name__ == '__main__':
    web.run_app(app, host='0.0.0.0', port=8088)
EOF
