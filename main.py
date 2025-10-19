# pipeline to extract data from tradeville API and load to postgres db
import asyncio
import dotenv

from websocket_tradeville import TradevilleAPIConnection

dotenv.load_dotenv()

tradeville_api_connection = TradevilleAPIConnection()
asyncio.run(tradeville_api_connection.get_portfolio())
asyncio.run(tradeville_api_connection.search_symbol("electric"))
asyncio.run(tradeville_api_connection.get_symbol_data("BRD"))
asyncio.run(tradeville_api_connection.get_symbol_orders("BRD"))
asyncio.run(tradeville_api_connection.get_symbol_trades("BRD", "1oct25", "10oct25"))
asyncio.run(tradeville_api_connection.subscribe_to_symbol("TLV"))
asyncio.run(tradeville_api_connection.get_account_activity("1ian25", "10oct25"))
asyncio.run(tradeville_api_connection.get_bnr_exchange_rate("EUR", "1apr25", "10apr25"))
asyncio.run(tradeville_api_connection.get_symbol_daily_values("SNP", "10ian25", "12ian25"))
asyncio.run(tradeville_api_connection.get_symbol_market_depth("BRD", 10))
