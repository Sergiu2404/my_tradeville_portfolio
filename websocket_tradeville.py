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

    async def __send_and_receive(self, websocket, tradeville_request_header):
        await self.__login(websocket)
        serialized_json = self.__to_json(tradeville_request_header)
        await websocket.send(serialized_json)
        response = await websocket.recv()

        print(response)
        return response


    async def get_portfolio(self):
        async with websockets.connect(
            uri = f"{TRADEVILLE_URI}:{TRADEVILLE_PORT}", subprotocols=["apitv"]
        ) as websocket:
            response = await self.__send_and_receive(websocket, self.__tradeville_portfolio_header)
            return response

    async def subscribe_to_symbol(self, symbol):
        '''Efectueaza abonarea la unul sau mai multe simboluri'''
        async with websockets.connect(
            uri = f"{TRADEVILLE_URI}:{TRADEVILLE_PORT}", subprotocols=["apitv"]
        ) as websocket:
            await self.__login(websocket)

            tradeville_subscribe_header = {
                "cmd": "subscribe",
                "sym": symbol,
                "prm": {},
                "OK": 1
            }
            serialized_json = self.__to_json(tradeville_subscribe_header)
            await websocket.send(serialized_json)

            connection_confirmation = await websocket.recv()
            print(connection_confirmation)
            try:
                while True:
                    message = await websocket.recv()
                    data = json.loads(message)
                    print("received data: " + data)
            except:
                print("Connection to server closed")

    async def get_bnr_exchange_rate(self, currency, date_start, date_end):
        '''
        Returneaza cursurile valutare comunicate de BNR (pentru o valuta, intr-o perioada)
        :param currency: International currency to see the BNR exchange rate for
        :param date_start: start date in format: dmmmyy (ex: 1oct20)
        :param date_end: end date in format: dmmmyy (ex: 1oct20)
        :return: ...
        '''
        async with websockets.connect(
            uri = f"{TRADEVILLE_URI}:{TRADEVILLE_PORT}", subprotocols=["apitv"]
        ) as websocket:
            tradeville_exchange_rate_header = {
                "cmd": "FXBNR",
                "prm": {
                    "ccy": "EUR",
                    "dstart": date_start,
                    "dend": date_end
                }
            }
            response = await self.__send_and_receive(websocket, tradeville_exchange_rate_header)
            return response

    async def get_account_activity(self, date_start, date_end):
        '''Returneaza activitatea din cont (pentru un simbol, intr-o perioada)'''
        async with websockets.connect(
            uri = f"{TRADEVILLE_URI}:{TRADEVILLE_PORT}", subprotocols=["apitv"]
        ) as websocket:
            tradeville_activity_header = {
                "cmd": "Activity",
                "prm": {
                    "symbol": None,
                    "dstart": date_start,
                    "dend": date_end
                }
            }
            response = await self.__send_and_receive(websocket, tradeville_activity_header)
            return response

    async def search_symbol(self, symbol_substring):
        '''Returneaza date despre simbolurile care contin termenul cautarii in denumire'''
        async with websockets.connect(
            uri = f"{TRADEVILLE_URI}:{TRADEVILLE_PORT}", subprotocols=["apitv"]
        ) as websocket:
            tradeville_search_symbol_header = {
                "cmd": "SearchSymbol",
                "prm": {
                    "search": symbol_substring.lower()
                }
            }
            response = await self.__send_and_receive(websocket, tradeville_search_symbol_header)
            return response

    async def get_symbol_orders(self, symbol):
        '''Returneaza oridinele pe un anumit simbol (daca este specificat un simbol), incepand cu o anumita data (daca este specificata o data)'''
        async with websockets.connect(
            uri = f"{TRADEVILLE_URI}:{TRADEVILLE_PORT}", subprotocols=["apitv"]
        ) as websocket:
            tradeville_symbol_orders_header = {
                "cmd": "Symbol",
                "prm": {
                    "symbol": symbol
                }
            }
            response = await self.__send_and_receive(websocket, tradeville_symbol_orders_header)
            return response

    async def get_symbol_data(self, symbol):
        '''Returneaza datele principale ale unui simbol. In functie de tipul simbolului (actiune/obligatiune/structurat), unele coloane sunt sau nu disponibile (e.g. StrikePrice)'''
        async with websockets.connect(
            uri = f"{TRADEVILLE_URI}:{TRADEVILLE_PORT}", subprotocols=["apitv"]
        ) as websocket:
            tradeville_symbol_header = {
                "cmd": "Symbol",
                "prm": {
                    "symbol": symbol
                }
            }
            response = await self.__send_and_receive(websocket, tradeville_symbol_header)
            return response

    async def get_symbol_market_depth(self, symbol, levels):
        '''Returneaza adancimea de piata a unui simbol'''
        async with websockets.connect(
            uri = f"{TRADEVILLE_URI}:{TRADEVILLE_PORT}", subprotocols=["apitv"]
        ) as websocket:
            tradeville_symbol_market_depth_header = {
                "cmd": "Level2",
                "prm": {
                    "symbol": symbol,
                    "levels": levels
                }
            }
            response = await self.__send_and_receive(websocket, tradeville_symbol_market_depth_header)
            return response

    async def get_symbol_daily_values(self, symbol, date_start, date_end):
        '''Returneaza valorile zilnice ale simbolului, in perioada selectata'''
        async with websockets.connect(
            uri = f"{TRADEVILLE_URI}:{TRADEVILLE_PORT}", subprotocols=["apitv"]
        ) as websocket:
            tradeville_symbol_daily_values_header = {
                "cmd": "DailyValues",
                "prm": {
                    "symbol": symbol,
                    "dstart": date_start,
                    "dend": date_end,
                    "adj": "1"
                }
            }
            response = await self.__send_and_receive(websocket, tradeville_symbol_daily_values_header)
            return response

    async def get_symbol_trades(self, symbol, date_start, date_end):
        '''Returneaza toate tranzactiile efectuate pe un simbol, intr-un anumit interval'''
        async with websockets.connect(
            uri = f"{TRADEVILLE_URI}:{TRADEVILLE_PORT}", subprotocols=["apitv"]
        ) as websocket:
            tradeville_symbol_trades_header = {
                "cmd": "Trades",
                "prm": {
                    "symbol": symbol,
                    "dstart": date_start,
                    "dend": date_end
                }
            }
            response = await self.__send_and_receive(websocket, tradeville_symbol_trades_header)
            return response