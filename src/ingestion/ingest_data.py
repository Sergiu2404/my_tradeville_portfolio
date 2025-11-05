import pandas as pd
import numpy as np

from src.data_sources.tradeville_api import TradevilleAPI
from src.data_sources.yahoofinance_api import YahooFinanceAPI


class MarketDataIngestor:
    def __init__(self, tradeville_api: TradevilleAPI, yahoofinance_api: YahooFinanceAPI):
        self.__tradeville_api = tradeville_api
        self.__yahoofinance_api = yahoofinance_api

    async def get_portfolio_snapshot(self) -> pd.DataFrame:
        portfolio_response = await self.__tradeville_api.get_portfolio()
        portfolio_data = pd.DataFrame(portfolio_response.get("data", {}) or {})

        for col in ["Industry", "Sector", "StockMarket"]:
            if col not in portfolio_data.columns:
                portfolio_data[col] = np.nan

        for idx, row in portfolio_data.iterrows():
            symbol = str(row["Symbol"]).strip().upper()

            stock_market = self.__detect_stock_market(symbol)
            portfolio_data.at[idx, "StockMarket"] = stock_market

            symbol_info = self.__get_symbol_industry_and_sector(symbol)
            portfolio_data.at[idx, "Industry"] = symbol_info.get("industry")
            portfolio_data.at[idx, "Sector"] = symbol_info.get("sector")

        return portfolio_data

    async def get_portfolio_daily_data(self, start_date, end_date):
        symbols = await self.__get_portfolio_symbols()

        all_data = []

        for symbol in symbols:
            try:
                daily_data = await self.__get_symbol_daily_data(symbol, start_date, end_date)
                if not daily_data.empty:
                    daily_data["Symbol"] = symbol
                    all_data.append(daily_data)
            except Exception as e:
                print(f"Failed to fetch daily data for {symbol}: {e}")

        if all_data:
            df = pd.concat(all_data, ignore_index=True)
            df = df.drop(columns=["Open", "Low"], errors="ignore")
            return df
        return pd.DataFrame()


    async def get_account_activity(self, date_start, date_end) -> pd.DataFrame:
        response = await self.__tradeville_api.get_account_activity(date_start, date_end)
        return pd.DataFrame(response.get("data", []) or {})

    async def get_portfolio_dividends_history(self, start_date, end_date):
        symbols = await self.__get_portfolio_symbols()
        all_symbols_dividends = []
        for symbol in symbols:
            try:
                symbol_dividends_history = self.__yahoofinance_api.get_symbol_dividends_history(f"{symbol}.RO", start_date, end_date)
                symbol_dividends_history["Symbol"] = symbol
                all_symbols_dividends.append(symbol_dividends_history)
            except Exception as e1:
                print(f"Dividends for symbol {symbol}.RO not found on yfinance: {e1}, trying {symbol}")
                try:
                    symbol_dividends_history = self.__yahoofinance_api.get_symbol_dividends_history(symbol, start_date, end_date)
                    symbol_dividends_history["Symbol"] = symbol
                    all_symbols_dividends.append(symbol_dividends_history)
                except Exception as e2:
                    print(f"Dividends for symbol {symbol} not found on yfinance: {e2}, search over")
        if all_symbols_dividends:
            df = pd.concat(all_symbols_dividends, ignore_index=True)
            return df
        else:
            return pd.DataFrame()

    async def __get_symbol_daily_data(self, symbol, start_date, end_date) -> pd.DataFrame:
        response = await self.__tradeville_api.get_symbol_daily_values(symbol, start_date, end_date)
        return pd.DataFrame(response["data"])

    async def __get_portfolio_symbols(self):
        response = await self.__tradeville_api.get_portfolio()
        portfolio_data = response.get("data", {}) or {}
        return set(portfolio_data.get("Symbol", []))

    def __get_symbol_industry_and_sector(self, symbol) -> dict:
        '''
        Get symbol info first with .RO suffix (Yahoo Finance pattern for tickers), then fallback to the base symbol
        '''
        try:
            return self.__yahoofinance_api.get_symbol_industry_and_sector(f"{symbol}.RO")
        except Exception as e:
            print(f"Symbol {symbol}.RO not found on yfinance: {e}, trying {symbol}")
            try:
                return self.__yahoofinance_api.get_symbol_industry_and_sector(symbol)
            except Exception as e2:
                print(f"Symbol {symbol} also failed: {e2}")
                return {"industry": np.nan, "sector": np.nan}

    def __detect_stock_market(self, symbol: str) -> str:
        """
        Detect stock market from common Yahoo Finance using tradeville api prefixes.
        """
        exchange_map = {
            "RO.": "RO", "DE.": "DE", "PA.": "FR", "L.": "UK", "MI.": "IT",
            "AT.": "AT", "SW.": "CH", "AS.": "NL", "ST.": "SE", "HE.": "FI",
            "NO.": "NO", "CO.": "DK", "PL.": "PL", "HU.": "HU", "BE.": "BE", "US.": "US"
        }

        for prefix, code in exchange_map.items():
            if symbol.startswith(prefix):
                return code


        if symbol in ["USD", "EUR", "GBP", "RON", "CHF", "JPY"]:
            return "FX"

        return "RO"


# yf = YahooFinanceAPI()
# tr = TradevilleAPI()
# ing = MarketDataIngestor(tr, yf)
# import asyncio
# result = asyncio.run(ing.get_portfolio_dividends_history("2025-02-10", "2025-10-10"))
# print(result.columns)
# print(result)

