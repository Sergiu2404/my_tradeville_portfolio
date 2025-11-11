from supabase import create_client, Client
import pandas as pd

class DbContext:
    def __init__(self, supabase_url, supabase_key):
        self.client: Client = create_client(supabase_url, supabase_key)

    def create_table(self, columns, table_name):
        print(f"Skipping table creation {table_name} must exist in Supabase schema.")
        return

    def fetch_table(self, table_name, order_by=None, limit=None):
        query = self.client.table(table_name).select("*")
        if order_by:
            query = query.order(order_by, desc=True)
        if limit:
            query = query.limit(limit)
        data = query.execute().data
        return pd.DataFrame(data)

    def insert_df_into_table(self, df, table_name):
        if df.empty:
            print(f"No data to insert into {table_name}")
            return
        data = df.to_dict(orient="records")
        self.client.table(table_name).insert(data).execute()
        print(f"Inserted {len(df)} rows into {table_name}")
