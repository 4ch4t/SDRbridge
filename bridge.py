import asyncio
import struct
from aiohttp import web

# Настройки rtl-tcp сервера
RTL_TCP_HOST = "127.0.0.1"  # Укажите IP вашего rtl-tcp сервера
RTL_TCP_PORT = 1248         # Стандартный порт rtl-tcp

clients = set()

async def rtl_reader():
    """Фоновая задача: чтение данных из rtl-tcp и рассылка WebSocket-клиентам."""
    while True:
        try:
            print(f"Подключение к rtl-tcp {RTL_TCP_HOST}:{RTL_TCP_PORT}...")
            reader, writer = await asyncio.open_connection(RTL_TCP_HOST, RTL_TCP_PORT)
            print("Успешно подключено к rtl-tcp!")
            
            while True:
                data = await reader.read(16384)
                if not data:
                    break
                
                # Рассылка данных всем подключенным веб-клиентам
                for ws in list(clients):
                    try:
                        await ws.send_bytes(data)
                    except Exception:
                        clients.remove(ws)
                        
        except Exception as e:
            print(f"Ошибка подключения к rtl-tcp: {e}. Повтор через 5 сек...")
            await asyncio.sleep(5)

async def websocket_handler(request):
    """Обработчик WebSocket-подключений от веб-интерфейса."""
    ws = web.WebSocketResponse()
    await ws.prepare(request)
    clients.add(ws)
    print(f"Новый клиент подключен. Всего клиентов: {len(clients)}")
    
    try:
        async for msg in ws:
            pass  # Можно добавить обработку команд перестройки частоты от клиента
    finally:
        clients.remove(ws)
        print(f"Клиент отключился. Всего клиентов: {len(clients)}")
    return ws

async def start_background_tasks(app):
    app['rtl_task'] = asyncio.create_task(rtl_reader())

async def cleanup_background_tasks(app):
    app['rtl_task'].cancel()
    await app['rtl_task']

app = web.Application()
app.router.add_get('/ws', websocket_handler)
app.on_startup.append(start_background_tasks)
app.on_cleanup.append(cleanup_background_tasks)

if __name__ == '__main__':
    web.run_app(app, host='0.0.0.0', port=8080)
EOF