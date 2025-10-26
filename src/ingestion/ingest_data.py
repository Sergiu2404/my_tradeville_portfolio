import pandas as pd

from src.data_sources.tradeville_api import TradevilleAPI
from src.data_sources.yahoofinance_api import YahooFinanceAPI


class MarketDataIngestor:
    def __init__(self, tradeville_api: TradevilleAPI, yahoofinance_api: YahooFinanceAPI):
        self.__tradeville_api = tradeville_api
        self.__yahoofinance_api = yahoofinance_api

    async def get_portfolio_snapshot(self) -> pd.DataFrame:
        response = await self.__tradeville_api.get_portfolio()
        portfolio_data = pd.DataFrame(response["data"])

        portfolio_data["Industry"] = [None] * len(portfolio_data)
        portfolio_data["Sector"] = [None] * len(portfolio_data)

        for idx, row in portfolio_data.iterrows():
            symbol_info = self.__get_symbol_industry_and_sector(row["Symbol"])
            portfolio_data.at[idx, "Industry"] = symbol_info["industry"]
            portfolio_data.at[idx, "Sector"] = symbol_info["sector"]
        return portfolio_data

    async def get_symbol_daily_data(self, symbol, interval, start_date, end_date) -> pd.DataFrame:
        response = await self.__tradeville_api.get_symbol_daily_values(symbol, start_date, end_date)
        daily_data = pd.DataFrame(response["data"])
        return daily_data

    async def get_account_activity(self, date_start, date_end) -> pd.DataFrame:
        response = await self.__tradeville_api.get_account_activity(date_start, date_end)
        account_activity = pd.DataFrame(response["data"])
        return account_activity

    def __get_symbol_industry_and_sector(self, symbol) -> dict[str, str]:
        symbol_info = self.__yahoofinance_api.get_symbol_industry_and_sector(symbol)
        return symbol_info
