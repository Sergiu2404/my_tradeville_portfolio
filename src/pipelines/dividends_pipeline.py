from src.pipelines.base_pipeline import BasePipeline
from src.config import config

class DividendsPipeline(BasePipeline):
    async def run(self):
        new_df = await self.ingestor.get_portfolio_dividends_history(config.SEVEN_DAYS_AGO, config.TODAY)

        if new_df.empty:
            print("no new dividends data to process")
            return

        self.db.create_table(config.DIVIDENDS_COLUMNS_MAP, config.DIVIDENDS_TABLE)
        existing_df = self.db.fetch_table(config.DIVIDENDS_TABLE)

        filtered_df = self.validator.validate_dividends(existing_df, new_df)
        if not filtered_df.empty:
            self.db.insert_df_into_table(filtered_df, config.DIVIDENDS_TABLE)
        # self.db.insert_df_into_table(filtered_df, config.DIVIDENDS_TABLE)

        print("dividends pipeline finished")
