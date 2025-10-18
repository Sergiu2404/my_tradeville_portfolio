import asyncio
import websockets
from websockets.typing import Subprotocol

TRADEVILLE_PORT = "443"
TRADEVILLE_URI = "wss://api.tradeville.ro"

class TradevilleAPIConnection:
    __tradeville_login_header = {
        "cmd": "login",
        "prm": {
            "coduser": "coduser",

        }
    }
    async def access_api(self):
        async with websockets.connect(
            uri = f"{TRADEVILLE_URI}:{TRADEVILLE_PORT}", subprotocols=["apitv"]
        ) as websocket:
            await websocket.send("req")
            response = await websocket.recv()
            print(response)

