import yfinance

class YahooFinanceAPI:
    def get_symbol_history_data(self, symbol, interval, date_start, date_end):
        data = yfinance.download(tickers=symbol, interval=interval, start=date_start, end=date_end)
        print(data)
    def get_symbol_industry_and_sector(self, symbol):
        ticker_data = yfinance.Ticker(symbol)
        info = ticker_data.info

        return (info["industry"], info["sector"])
