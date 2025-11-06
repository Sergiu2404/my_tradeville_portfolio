import dotenv
import pandas as pd
import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv
from psycopg2.extras import execute_values

load_dotenv()

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

HUGGING_FACE_BVB_SYMBOLS = "hf://datasets/ThunderDrag/Romania-Stock-Symbols-and-Metadata/romania.csv"
USER = dotenv.dotenv_values("../../.env")["SUPABASE_USER"]
PASSWORD = dotenv.dotenv_values("../../.env")["SUPABASE_PASSWORD"]
HOST = dotenv.dotenv_values("../../.env")["SUPABASE_HOST"]
PORT = dotenv.dotenv_values("../../.env")["SUPABASE_PORT"]
DBNAME = dotenv.dotenv_values("../../.env")["SUPABASE_DATABASE"]

bvb_symbols_df = pd.read_csv(HUGGING_FACE_BVB_SYMBOLS)
bvb_symbols_df = bvb_symbols_df.rename(columns=STOCK_SYMBOLS_COLUMNS_MAP)

try:
    connection = psycopg2.connect(
        user=USER,
        password=PASSWORD,
        host=HOST,
        port=PORT,
        dbname=DBNAME
    )
    print("conneciton succeeded")

    cursor = connection.cursor()

    columns_and_types_sql = ", ".join([
        f"{col} {STOCK_SYMBOLS_COLUMNS_TYPES_MAP[col]}" for col in STOCK_SYMBOLS_COLUMNS_MAP.values()
    ])
    create_table_query = f"""
            CREATE TABLE IF NOT EXISTS stock_symbols (
                id SERIAL PRIMARY KEY,
                {columns_and_types_sql}
            );
    """

    cursor.execute(create_table_query)
    connection.commit()

    cursor.execute("DELETE FROM stock_symbols;")
    connection.commit()

    tuples = [tuple(x) for x in bvb_symbols_df.to_numpy()]
    cols = ','.join(bvb_symbols_df.columns)
    insert_query = sql.SQL("INSERT INTO stock_symbols ({}) VALUES %s").format(sql.SQL(cols))

    execute_values(cursor, insert_query, tuples)
    connection.commit()

    cursor.close()
    connection.close()

except Exception as e:
    print(f"Failed to fetch stocks from {HUGGING_FACE_BVB_SYMBOLS}: {e}")
