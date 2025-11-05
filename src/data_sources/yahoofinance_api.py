import yfinance
import pandas as pd
import numpy as np
from pandas import DataFrame


class YahooFinanceAPI:
    def get_symbol_history_data(self, symbol, interval, date_start, date_end) -> DataFrame:
        data = yfinance.download(tickers=symbol, interval=interval, start=date_start, end=date_end)
        return data
    def get_symbols_history_data(self, symbol, interval, date_start, date_end) -> DataFrame:
        data = yfinance.download(tickers=symbol, interval=interval, start=date_start, end=date_end)
        return data

    def get_symbol_industry_and_sector(self, symbol) -> dict:
        ticker_data = yfinance.Ticker(symbol)
        info = ticker_data.info or {}
        return {
            "industry": info.get("industry", np.nan),
            "sector": info.get("sector", np.nan)
        }

    def get_symbol_dividends_history(self, symbol, start_date, end_date) -> DataFrame:
        '''
        Get dividends history for symbol
        :param symbol: Stock symbol
        :return: Series containing dividends history for given symbol
        '''
        symbol_data = yfinance.Ticker(symbol)
        dividends_series = symbol_data.dividends

        if dividends_series.empty:
            return pd.DataFrame(columns=["Date", "Dividends"])

        df = dividends_series.reset_index() # move index to a col
        df.columns = ["Date", "Dividends"]

        if start_date:
            df = df[df["Date"] >= start_date]
        if end_date:
            df = df[df["Date"] <= end_date]

        return df