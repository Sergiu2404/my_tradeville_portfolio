import psycopg2
import pandas as pd
from psycopg2 import sql
from psycopg2.extras import execute_values

class PostgresDbContext:
    def __init__(self, user, password, host, port, dbname):
        self.__conn_params = {
            "user": user,
            "password": password,
            "host": host,
            "port": port,
            "dbname": dbname
        }

    def __open_connection(self):
        return psycopg2.connect(**self.__conn_params)

    def create_table(self, columns: dict[str, str], table_name):
        '''
        Create a table based on a list of column names if it doesn't exist
        Adds an auto-incrementing primary key column `id`
        '''
        columns_and_types = sql.SQL(", ").join(
            [
                sql.SQL("{} {}").format(sql.Identifier(col_name), sql.SQL(col_type))
                for col_name, col_type in columns.items()
            ]
        )
        create_table_query = sql.SQL("CREATE TABLE IF NOT EXISTS {} (id SERIAL PRIMARY KEY, {});").format(
            sql.Identifier(table_name),
            columns_and_types
        )

        with self.__open_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(create_table_query)
            conn.commit()

    def clear_table(self, table_name):
        '''
        Delete all rows
        '''
        delete_query = sql.SQL("DELETE FROM {}").format(
            sql.Identifier(table_name)
        )
        with self.__open_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(delete_query)
            conn.commit()

    def drop_table(self, table_name):
        drop_query = sql.SQL("DROP TABLE {}").format(
            sql.Identifier(table_name)
        )
        with self.__open_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(drop_query)
            conn.commit()

    def insert_df_into_table(self, df: pd.DataFrame, table_name):
        '''
        Load df records into table
        '''
        if df.empty:
            print(f"[INFO] No data to insert into {table_name}")
            return

        columns = sql.SQL(", ").join(sql.Identifier(col) for col in df.columns)
        values = list(df.itertuples(index=False, name=None))

        insert_query = sql.SQL("INSERT INTO {} ({}) VALUES %s").format(
            sql.Identifier(table_name),
            columns
        )

        with self.__open_connection() as conn:
            with conn.cursor() as cursor:
                execute_values(cursor, insert_query, values)
            conn.commit()

    def execute_select_query(self, select_query) -> pd.DataFrame:
        with self.__open_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(select_query)
                rows = cursor.fetchall()
                col_names = [description[0] for description in cursor.description]
            conn.commit()
        return pd.DataFrame(rows, columns=col_names)