# pipeline to extract data from tradeville API and load to postgres db
import asyncio
import dotenv

from websocket_tradeville import TradevilleAPIConnection

TRADEVILLE_PORT = "443"
TRADEVILLE_URI = "wss://api.tradeville.ro"

dotenv.load_dotenv()

tradeville_api_connection = TradevilleAPIConnection()
asyncio.run(tradeville_api_connection.portfolio())