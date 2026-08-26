"""
Serwer-relay komunikatora — WebSocket, do wdrożenia na Render jako Web Service.

Zawiera endpoint /healthz zgodnie z oficjalnym przewodnikiem biblioteki
websockets dla Render (https://websockets.readthedocs.io/en/stable/deploy/render.html)
— dzięki temu Render poprawnie wykrywa, że serwis wystartował.

Po wdrożeniu warto (choć nie jest to obowiązkowe) ustawić w Render:
Settings -> Health & Alerts -> Health Check Path: /healthz
"""

import asyncio
import http
import json
import os
import signal

from websockets.asyncio.server import serve

rooms = {}  # kod_pokoju -> set(websocket)


def health_check(connection, request):
    """Odpowiada zwykłym HTTP 200 na /healthz, żeby Render wiedział, że serwis żyje.
    Zwykłe żądania WebSocket (Upgrade) przechodzą dalej normalnie."""
    if request.path == "/healthz":
        return connection.respond(http.HTTPStatus.OK, "OK\n")


async def handler(websocket):
    room_code = None
    try:
        async for raw in websocket:
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                continue

            mtype = msg.get("type")

            if mtype == "join":
                room_code = (msg.get("room") or "").strip().upper()
                if not room_code:
                    continue
                rooms.setdefault(room_code, set()).add(websocket)
                await websocket.send(json.dumps({"type": "system", "text": "Dołączono do pokoju."}))

            elif mtype == "text" and room_code:
                peers = rooms.get(room_code, set())
                dead = set()
                for peer in peers:
                    if peer is websocket:
                        continue
                    try:
                        await peer.send(json.dumps({
                            "type": "text",
                            "sender": msg.get("sender", "?"),
                            "text": msg.get("text", ""),
                        }))
                    except Exception:
                        dead.add(peer)
                peers -= dead
    finally:
        if room_code and room_code in rooms:
            rooms[room_code].discard(websocket)
            if not rooms[room_code]:
                del rooms[room_code]


async def main():
    port = int(os.environ.get("PORT", 8765))
    async with serve(handler, "0.0.0.0", port, process_request=health_check) as server:
        print(f"Serwer działa na porcie {port}")
        loop = asyncio.get_running_loop()
        stop = loop.create_future()
        loop.add_signal_handler(signal.SIGTERM, stop.set_result, None)
        await stop  # trzyma serwer włączony aż Render wyśle sygnał zamknięcia


if __name__ == "__main__":
    asyncio.run(main())
