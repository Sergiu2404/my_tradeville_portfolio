import yfinance

class YahooFinanceAPI:
    def get_symbol_history_data(self, symbol, interval, date_start, date_end):
        data = yfinance.download(tickers=symbol, interval=interval, start=date_start, end=date_end)
        return data

    def get_symbol_industry_and_sector(self, symbol):
        ticker_data = yfinance.Ticker(symbol)
        info = ticker_data.info
        return (info["industry"], info["sector"])

    def get_symbol_dividends_history(self, symbol):
        '''
        Get dividends history for symbol
        :param symbol: Stock symbol
        :return: Series containing dividends history for given symbol
        '''
        symbol_data = yfinance.Ticker(symbol)
        dividends_data = symbol_data.dividends
        return dividends_data

