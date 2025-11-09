# from src.pipelines.base_pipeline import BasePipeline
# from src.config import config
#
# class PortfolioSymbolsDailyValues(BasePipeline):
#     async def run(self):
#         new_portfolio_symbols_daily_values_df = await self.ingestor.get_portfolio_daily_data(config.SEVEN_DAYS_AGO, config.TODAY)
#
#         self.db.create_table(config.PORTFOLIO_SYMBOLS_DAILY_VALUES_COLUMNS_MAP, config.PORTFOLIO_SYMBOLS_DAILY_VALUES_TABLE)
#         latest_portfolio_symbols_daily_values_record_df = self.db.execute_select_query(
#             f'SELECT * FROM {config.PORTFOLIO_SYMBOLS_DAILY_VALUES_TABLE} ORDER BY "Date" DESC LIMIT 1'
#         )
#
#         filtered_portfolio_symbols_daily_values_df = self.validator.validate_portfolio_symbols_daily_values(
#             latest_portfolio_symbols_daily_values_record_df, new_portfolio_symbols_daily_values_df)
#
#         self.db.insert_df_into_table(filtered_portfolio_symbols_daily_values_df,
#                                         config.PORTFOLIO_SYMBOLS_DAILY_VALUES_TABLE)
#         print("portfolio daily values pipeline finished")
#
from src.pipelines.base_pipeline import BasePipeline
from src.config import config

class PortfolioSymbolsDailyValues(BasePipeline):
    async def run(self):
        new_df = await self.ingestor.get_portfolio_daily_data(config.SEVEN_DAYS_AGO, config.TODAY)

        self.db.create_table(config.PORTFOLIO_SYMBOLS_DAILY_VALUES_COLUMNS_MAP, config.PORTFOLIO_SYMBOLS_DAILY_VALUES_TABLE)
        existing_df = self.db.fetch_table(config.PORTFOLIO_SYMBOLS_DAILY_VALUES_TABLE, order_by="Date", limit=1)

        filtered_df = self.validator.validate_portfolio_symbols_daily_values(existing_df, new_df)
        self.db.insert_df_into_table(filtered_df, config.PORTFOLIO_SYMBOLS_DAILY_VALUES_TABLE)

        print("portfolio daily values pipeline finished")
