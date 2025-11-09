import os

import dotenv
from dotenv import load_dotenv
from datetime import datetime, timedelta
import pytz

load_dotenv()

DIVIDENDS_TABLE = "portfolio_dividends"
DIVIDENDS_COLUMNS_MAP = {
    "Date": "DATE",
    "Dividends": "NUMERIC(10, 6)",
    "Symbol": "VARCHAR(10)"
}

PORTFOLIO_SNAPSHOTS_TABLE = "portfolio_snapshots"
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

PORTFOLIO_SYMBOLS_DAILY_VALUES_TABLE = "portfolio_symbols_daily_values"
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

START_DATE = "2023-01-01" # TODO: replace with the date when the pipelines was ran last time

USER = os.environ.get("SUPABASE_USER")
PASSWORD = os.environ.get("SUPABASE_PASSWORD")
HOST = os.environ.get("SUPABASE_HOST")
PORT = os.environ.get("SUPABASE_PORT")
DBNAME = os.environ.get("SUPABASE_DATABASE")
# USER = dotenv.dotenv_values("./../.env")["SUPABASE_USER"]
# PASSWORD = dotenv.dotenv_values("./../.env")["SUPABASE_PASSWORD"]
# HOST = dotenv.dotenv_values("./../.env")["SUPABASE_HOST"]
# PORT = dotenv.dotenv_values("./../.env")["SUPABASE_PORT"]
# DBNAME = dotenv.dotenv_values("./../.env")["SUPABASE_DATABASE"]

SUPABASE_URI = os.environ.get("SUPABASE_URI")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
# SUPABASE_URI = dotenv.dotenv_values("./../.env")["SUPABASE_URI"]
# SUPABASE_KEY = dotenv.dotenv_values("./../.env")["SUPABASE_KEY"]

TODAY = datetime.today().strftime("%Y-%m-%d")
SEVEN_DAYS_AGO = datetime.today() - timedelta(days=7)
SEVEN_DAYS_AGO = SEVEN_DAYS_AGO.strftime("%Y-%m-%d")

BUCHAREST_TIMEZONE = pytz.timezone("Europe/Bucharest")


# fetch_bvb_symbols
HUGGING_FACE_BVB_SYMBOLS = "hf://datasets/ThunderDrag/Romania-Stock-Symbols-and-Metadata/romania.csv"
STOCK_SYMBOLS_COLUMNS_MAP = {
    "name": "Name",
    "ticker": "Symbol",
    "market": "Market",
    "sector": "Sector"
}
STOCK_SYMBOLS_COLUMNS_TYPES_MAP = {
    "Name": "VARCHAR(80)",
    "Symbol": "VARCHAR(10)",
    "Market": "VARCHAR(5)",
    "Sector": "VARCHAR(100)"
}

# Tradeville API
TRADEVILLE_PORT = "443"
TRADEVILLE_URI = "wss://api.tradeville.ro"
TRADEVILLE_USER = os.environ.get("USR")
TRADEVILLE_PASSWORD = os.environ.get("PSWD")
# TRADEVILLE_USER = dotenv.dotenv_values("./../.env")["CODUSER"],
# TRADEVILLE_PASSWORD = dotenv.dotenv_values("./../.env")["PASSWORD"],