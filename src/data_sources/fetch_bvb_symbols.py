import pandas as pd
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values

from src.config import config

bvb_symbols_df = pd.read_csv(config.HUGGING_FACE_BVB_SYMBOLS)
bvb_symbols_df = bvb_symbols_df.rename(columns=config.STOCK_SYMBOLS_COLUMNS_MAP)

try:
    connection = psycopg2.connect(
        user=config.USER,
        password=config.PASSWORD,
        host=config.HOST,
        port=config.PORT,
        dbname=config.DBNAME
    )
    print("conneciton succeeded")

    cursor = connection.cursor()

    columns_and_types_sql = ", ".join([
        f"{col} {config.STOCK_SYMBOLS_COLUMNS_TYPES_MAP[col]}" for col in config.STOCK_SYMBOLS_COLUMNS_MAP.values()
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
    print(f"Failed to fetch stocks from {config.HUGGING_FACE_BVB_SYMBOLS}: {e}")
