import json
import websockets
import dotenv

TRADEVILLE_PORT = "443"
TRADEVILLE_URI = "wss://api.tradeville.ro"

dotenv.load_dotenv()


class TradevilleAPIConnection:
    '''
    For more details on the websocket connection to tradeville, access api docs below:
    https://portal.tradeville.ro/diverse/api/APIdocs.htm#pfolosire
    '''
    __tradeville_login_header = {
        "cmd": "login",
        "prm": {
            "coduser": dotenv.dotenv_values(".env")["CODUSER"], #"!DemoAPITDV"
            "parola": dotenv.dotenv_values(".env")["PASSWORD"], #"DemoAPITDV"
            "demo": False
        }
    }
    __tradeville_portfolio_header = {
        "cmd": "Portfolio",
        "prm": {
            "data": "null"
        }
    }

    def __to_json(self, object):
        return json.dumps(object)

    async def __login(self, websocket):
        serialized_json = self.__to_json(self.__tradeville_login_header)
        await websocket.send(serialized_json)
        response = await websocket.recv()
        print(response)

    async def portfolio(self):
        async with websockets.connect(
            uri = f"{TRADEVILLE_URI}:{TRADEVILLE_PORT}", subprotocols=["apitv"]
        ) as websocket:
            await self.__login(websocket)
            # request portfolio command
            serialized_json = self.__to_json(self.__tradeville_portfolio_header)
            await websocket.send(serialized_json)
            response = await websocket.recv()
            print(response)
