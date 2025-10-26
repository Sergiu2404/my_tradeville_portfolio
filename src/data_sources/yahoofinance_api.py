import yfinance
import pandas as pd
from pandas import DataFrame


class YahooFinanceAPI:
    def get_symbol_history_data(self, symbol, interval, date_start, date_end) -> DataFrame:
        data = pd.DataFrame(yfinance.download(tickers=symbol, interval=interval, start=date_start, end=date_end))
        return data
    def get_symbols_history_data(self, symbol, interval, date_start, date_end) -> DataFrame:
        data = pd.DataFrame(yfinance.download(tickers=symbol, interval=interval, start=date_start, end=date_end))
        return data

    def get_symbol_industry_and_sector(self, symbol) -> dict[str, str]:
        ticker_data = yfinance.Ticker(symbol)
        info = ticker_data.info
        data = {
            "symbol": symbol,
            "industry": info.get("industry"),
            "sector": info.get("sector")
        }
        return data

    def get_symbol_dividends_history(self, symbol) -> DataFrame:
        '''
        Get dividends history for symbol
        :param symbol: Stock symbol
        :return: Series containing dividends history for given symbol
        '''
        symbol_data = yfinance.Ticker(symbol)
        dividends_data = pd.DataFrame(symbol_data.dividends)
        return dividends_data

