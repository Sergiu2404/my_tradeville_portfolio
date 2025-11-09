import pandas as pd

from src.pipelines.base_pipeline import BasePipeline
from src.config import config

class PortfolioSymbolsDailyValues(BasePipeline):
    async def run(self):
        new_df = await self.ingestor.get_portfolio_daily_data(config.SEVEN_DAYS_AGO, config.TODAY)

        if new_df.empty:
            print("no new portfolio daily values data to process.")
            return
        self.db.create_table(config.PORTFOLIO_SYMBOLS_DAILY_VALUES_COLUMNS_MAP, config.PORTFOLIO_SYMBOLS_DAILY_VALUES_TABLE)
        existing_df = self.db.fetch_table(config.PORTFOLIO_SYMBOLS_DAILY_VALUES_TABLE, order_by="Date", limit=1)

        filtered_df = self.validator.validate_portfolio_symbols_daily_values(existing_df, new_df)
        if not filtered_df.empty:
            for col, dtype in filtered_df.dtypes.items():
                if "datetime" in str(dtype):
                    filtered_df[col] = filtered_df[col].dt.strftime("%Y-%m-%d %H:%M:%S")
            filtered_df = filtered_df.where(pd.notnull(filtered_df), None)
            self.db.insert_df_into_table(filtered_df, config.PORTFOLIO_SYMBOLS_DAILY_VALUES_TABLE)
        # self.db.insert_df_into_table(filtered_df, config.PORTFOLIO_SYMBOLS_DAILY_VALUES_TABLE)

        print("portfolio daily values pipeline finished")
