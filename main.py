# pipeline to extract data from tradeville API and load to postgres db
from websocket_tradeville import TradevilleAPIConnection

tradeville_api_connection = TradevilleAPIConnection()
tradeville_api_connection.access_api()