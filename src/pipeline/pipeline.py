import dotenv

from src.data_validator.validator import Validator
from src.ingestion.ingest_data import MarketDataIngestor
from src.storage.db_context import DbContext
from src.data_sources.tradeville_api import TradevilleAPI
from src.data_sources.yahoofinance_api import YahooFinanceAPI
from dotenv import load_dotenv
import pandas as pd
from datetime import datetime

load_dotenv()

DIVIDENDS_TABLE = "portfolio_dividends"
DIVIDENDS_COLUMNS_MAP = {
    "Date": "DATE",
    "Dividends": "NUMERIC(10, 6)",
    "Symbol": "VARCHAR(10)"
}

PORTFOLIO_SNAPSHOTS_COLUMNS_MAP = {
    "Symbol": "VARCHAR(10)",
    "Quantity": "NUMERIC(20, 1)",
    "AvgPrice": "NUMERIC(20, 6)",
    "MarketPrice": "NUMERIC(20, 4)",
    "PType": "VARCHAR(3)",
    "Ccy": "VARCHAR(5)",
    "Industry": "VARCHAR(100)",
    "Sector": "VARCHAR(100)",
    "StockMarket": "VARCHAR(4)",
    "SnapshotDate": "DATE"
}

PORTFOLIO_SYMBOLS_DAILY_VALUES_COLUMNS_MAP = {
    "Symbol": "VARCHAR(10)",
    "Date": "DATE",
    "Close": "NUMERIC(20, 6)",
    "Volume": "BIGINT",
    "Value": "NUMERIC(20, 6)",
    "Trades": "INT"
}

ACCOUNT_ACTIVITY_TABLE = "account_activity"
ACCOUNT_ACTIVITY_COLUMNS_MAP = {
    "Date": "DATE",
    "OpType": "VARCHAR(20)",
    "Symbol": "VARCHAR(10)",
    "Quantity": "NUMERIC(10, 1)",
    "Price": "NUMERIC(20, 6)",
    "Comission": "NUMERIC(20, 6)",
    "Ammount": "NUMERIC(20, 6)",
    "CashPos": "NUMERIC(20, 6)",
    "InstrPos": "VARCHAR(30)",
    "Profit": "NUMERIC(20, 6)",
    "TranzNo": "VARCHAR(20)",
    "Ccy": "VARCHAR(5)",
    "Obs": "VARCHAR(80)",
    "AvgPrice": "NUMERIC(20, 6)",
    "OrderId": "VARCHAR(20)",
    "Tax": "NUMERIC(20, 6)",
    "Market": "VARCHAR(20)"
}

START_DATE = "2023-01-01" # TODO: replace with the date when the pipeline was ran last time

PORTFOLIO_SNAPSHOTS_TABLE = "portfolio_snapshots"

PORTFOLIO_SYMBOLS_DAILY_VALUES_TABLE = "portfolio_symbols_daily_values"

USER = dotenv.dotenv_values("../../.env")["SUPABASE_USER"]
PASSWORD = dotenv.dotenv_values("../../.env")["SUPABASE_PASSWORD"]
HOST = dotenv.dotenv_values("../../.env")["SUPABASE_HOST"]
PORT = dotenv.dotenv_values("../../.env")["SUPABASE_PORT"]
DBNAME = dotenv.dotenv_values("../../.env")["SUPABASE_DATABASE"]

TODAY = datetime.today().strftime("%Y-%m-%d")

# data = {
#     "a": [1, 2, 3],
#     "b": [4, 5, 6]
# }
# df = pd.DataFrame(data)
# print(df.columns)
# db_context.create_table_from_dataframe(df, "test")
# db_context.insert_df_into_table(df, "test")
# db_context.clear_table("test")
# db_context.drop_table("test")

tradeville = TradevilleAPI()
yahoo_finance = YahooFinanceAPI()
market_data_ingestor = MarketDataIngestor(tradeville, yahoo_finance)

import asyncio


db_context = DbContext(USER, PASSWORD, HOST, PORT, DBNAME)
# db_context.drop_table(DIVIDENDS_TABLE)
# db_context.drop_table(PORTFOLIO_SNAPSHOTS_TABLE)
# db_context.drop_table(PORTFOLIO_SYMBOLS_DAILY_VALUES_TABLE)
# db_context.drop_table(ACCOUNT_ACTIVITY_TABLE)


#get df
# new_portfolio_dividends_df = asyncio.run(market_data_ingestor.get_portfolio_dividends_history(START_DATE, TODAY))

# new_portfolio_snapshot_df = asyncio.run(market_data_ingestor.get_portfolio_snapshot())

# new_portfolio_symbols_daily_values_df = asyncio.run(market_data_ingestor.get_portfolio_daily_data("2023-01-01", TODAY))

# new_account_activity_df = asyncio.run(market_data_ingestor.get_account_activity("2023-01-01", TODAY))


#load from db
# db_context.create_table(DIVIDENDS_COLUMNS_MAP, DIVIDENDS_TABLE)
# dividends_table_df = db_context.execute_select_query(f"SELECT * FROM {DIVIDENDS_TABLE}")

# db_context.create_table(PORTFOLIO_SNAPSHOTS_COLUMNS_MAP, PORTFOLIO_SNAPSHOTS_TABLE)
# latest_record_portfolio_snapshots_table_df = db_context.execute_select_query(f'SELECT * FROM {PORTFOLIO_SNAPSHOTS_TABLE} ORDER BY "SnapshotDate" DESC LIMIT 1')

# db_context.create_table(PORTFOLIO_SYMBOLS_DAILY_VALUES_COLUMNS_MAP, PORTFOLIO_SYMBOLS_DAILY_VALUES_TABLE)
# latest_portfolio_symbols_daily_values_record_df = db_context.execute_select_query(f'SELECT * FROM {PORTFOLIO_SYMBOLS_DAILY_VALUES_TABLE} ORDER BY "Date" DESC LIMIT 1')

# db_context.create_table(ACCOUNT_ACTIVITY_COLUMNS_MAP, ACCOUNT_ACTIVITY_TABLE)
# latest_account_activity_df = db_context.execute_select_query(f'SELECT * FROM {ACCOUNT_ACTIVITY_TABLE} ORDER BY "Date" DESC LIMIT 1')


# validate / filter data
validator = Validator()
# filtered_portfolio_dividends_df = validator.validate_dividends(dividends_table_df, new_portfolio_dividends_df)
# filtered_portfolio_snapshots_df = validator.validate_portfolio_snapshot(latest_record_portfolio_snapshots_table_df, new_portfolio_snapshot_df)
# filtered_portfolio_symbols_daily_values_df = validator.validate_portfolio_symbols_daily_values(latest_portfolio_symbols_daily_values_record_df, new_portfolio_symbols_daily_values_df)
# filtered_account_activity_df = validator.validate_account_activity(latest_account_activity_df, new_account_activity_df)



#save / insert to db
# db_context.insert_df_into_table(filtered_portfolio_dividends_df, DIVIDENDS_TABLE)
# db_context.insert_df_into_table(filtered_portfolio_snapshots_df, PORTFOLIO_SNAPSHOTS_TABLE)
# db_context.insert_df_into_table(filtered_portfolio_symbols_daily_values_df, PORTFOLIO_SYMBOLS_DAILY_VALUES_TABLE)
# db_context.insert_df_into_table(filtered_account_activity_df, ACCOUNT_ACTIVITY_TABLE)