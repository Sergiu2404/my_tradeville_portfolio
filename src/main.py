# pipeline to extract data from tradeville API and load to postgres db
import asyncio
import pandas as pd

from src.data_sources.tradeville_api import TradevilleAPI
from src.data_sources.yahoofinance_api import YahooFinanceAPI

from src.processing.tradeville_data_processing import TradevilleDataProcessing

yahoofinance_api = YahooFinanceAPI()
(industry, sector) = yahoofinance_api.get_symbol_industry_and_sector("SNP.RO")
print(industry + "\n" + sector)
#yahoofinance_api.get_symbol_history_data("INTL", "1mo", "2025-07-01", "2025-10-01")

tradeville_api_connection = TradevilleAPI()
tradeville_data_processing = TradevilleDataProcessing()

response = asyncio.run(tradeville_api_connection.get_portfolio())
portfolio_df = tradeville_data_processing.to_dataframe(response["data"])
print(portfolio_df)

response = asyncio.run(tradeville_api_connection.search_symbol("electric"))
search_symbol_df = tradeville_data_processing.to_dataframe(response["data"])
print(search_symbol_df)

response = asyncio.run(tradeville_api_connection.get_symbol_data("BRD"))
symbol_data = tradeville_data_processing.to_dataframe(response["data"])
print(symbol_data)

response = asyncio.run(tradeville_api_connection.get_symbol_orders("BRD"))
symbol_orders = tradeville_data_processing.to_dataframe(response["data"])
print(symbol_orders)

response = asyncio.run(tradeville_api_connection.get_symbol_trades("BRD", "1oct25", "10oct25"))
symbol_trades = tradeville_data_processing.to_dataframe(response["data"])
print(symbol_trades)

response = asyncio.run(tradeville_api_connection.get_account_activity("1jan25", "10oct25"))
print(response)
account_activity = tradeville_data_processing.to_dataframe(response["data"])
print(account_activity)

response = asyncio.run(tradeville_api_connection.get_bnr_exchange_rate("EUR", "1apr25", "10apr25"))
exchange_rate = tradeville_data_processing.to_dataframe(response["data"])
print(exchange_rate)

response = asyncio.run(tradeville_api_connection.get_symbol_daily_values("SNP", "1oct25", "15oct25"))
daily_values = tradeville_data_processing.to_dataframe(response["data"])
print(daily_values)

# response = asyncio.run(tradeville_api_connection.get_symbol_market_depth("BRD", 10))
# print(response)
# market_depth = tradeville_data_processing.to_dataframe(response["data"])
# print(market_depth)
#
# response = asyncio.run(tradeville_api_connection.subscribe_to_symbol("TLV"))
# subscribe = tradeville_data_processing.to_dataframe(response["data"])
# print(subscribe)