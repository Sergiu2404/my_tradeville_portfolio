import dotenv
import pandas as pd
import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv
from psycopg2.extras import execute_values

load_dotenv()

HUGGING_FACE_BVB_SYMBOLS = "hf://datasets/ThunderDrag/Romania-Stock-Symbols-and-Metadata/romania.csv"
USER = dotenv.dotenv_values("../../.env")["SUPABASE_USER"]
PASSWORD = dotenv.dotenv_values("../../.env")["SUPABASE_PASSWORD"]
HOST = dotenv.dotenv_values("../../.env")["SUPABASE_HOST"]
PORT = dotenv.dotenv_values("../../.env")["SUPABASE_PORT"]
DBNAME = dotenv.dotenv_values("../../.env")["SUPABASE_DATABASE"]

bvb_symbols_df = pd.read_csv(HUGGING_FACE_BVB_SYMBOLS)

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

    columns_and_types = ", ".join([f"{col} TEXT" for col in bvb_symbols_df.columns])
    create_table_query = f"CREATE TABLE IF NOT EXISTS stock_symbols ({columns_and_types});"

    cursor.execute(create_table_query)
    connection.commit()

    cursor.execute("DELETE FROM stock_symbols;")
    connection.commit()

    tuples = [tuple(x) for x in bvb_symbols_df.to_numpy()]
    cols = ','.join(list(bvb_symbols_df.columns))
    insert_query = sql.SQL("INSERT INTO stock_symbols ({}) VALUES %s").format(sql.SQL(cols))

    execute_values(cursor, insert_query, tuples)
    connection.commit()

    cursor.close()
    connection.close()

except Exception as e:
    print(f"Failed to fetch stocks from {HUGGING_FACE_BVB_SYMBOLS}: {e}")
