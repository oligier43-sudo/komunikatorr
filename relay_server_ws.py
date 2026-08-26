"""
Serwer-relay komunikatora — WebSocket, do wdrożenia na Render jako Web Service.

W przeciwieństwie do PythonAnywhere, Render uruchamia zwykły kontener, więc
działają tu prawdziwe WebSockety — klient dostaje wiadomości natychmiast,
zamiast czekać na kolejne odpytanie serwera.

Jak wdrożyć na Render:
1. Wrzuć ten plik + requirements.txt do repozytorium na GitHubie
   (Render potrzebuje repo, żeby móc automatycznie budować i wdrażać).
2. Na render.com: New -> Web Service -> połącz swoje repo.
3. Environment: Python 3
   Build command:  pip install -r requirements.txt
   Start command:  python relay_server_ws.py
4. Render sam ustawia zmienną środowiskową PORT — kod poniżej ją odczytuje,
   nic nie trzeba zmieniać.
5. Po wdrożeniu adres serwisu to https://twoja-nazwa.onrender.com
   — do połączenia WebSocket używasz wss://twoja-nazwa.onrender.com
   (wklej to jako SERVER_WS_URL w kliencie).

Uwaga: darmowy plan Render usypia serwis po ok. 15 minutach bez ruchu.
Pierwsza wiadomość po przerwie może obudzić serwer z opóźnieniem
kilkunastu-kilkudziesięciu sekund — to normalne, kolejne już będą szybkie.
"""

import asyncio
import json
import os
import websockets

rooms = {}  # kod_pokoju -> set(websocket)


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
                    except websockets.exceptions.ConnectionClosed:
                        dead.add(peer)
                peers -= dead
    finally:
        if room_code and room_code in rooms:
            rooms[room_code].discard(websocket)
            if not rooms[room_code]:
                del rooms[room_code]


async def main():
    port = int(os.environ.get("PORT", 8765))
    async with websockets.serve(handler, "0.0.0.0", port):
        print(f"Serwer działa na porcie {port}")
        await asyncio.Future()  # trzyma serwer włączony w nieskończoność


if __name__ == "__main__":
    asyncio.run(main())
