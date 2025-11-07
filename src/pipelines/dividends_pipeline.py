from src.pipelines.base_pipeline import BasePipeline
from src.config import config

class DividendsPipeline(BasePipeline):
    async def run(self):
        new_df = await self.ingestor.get_portfolio_dividends_history(config.SEVEN_DAYS_AGO, config.TODAY)

        self.db.create_table(config.DIVIDENDS_COLUMNS_MAP, config.DIVIDENDS_TABLE)
        dividends_table_df = self.db.execute_select_query(f"SELECT * FROM {config.DIVIDENDS_TABLE}")

        filtered_portfolio_dividends_df = self.validator.validate_dividends(dividends_table_df, new_df)

        self.db.insert_df_into_table(filtered_portfolio_dividends_df, config.DIVIDENDS_TABLE)
        print("dividends pipeline execution finished.")

