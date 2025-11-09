from src.pipelines.base_pipeline import BasePipeline
from src.config import config

class AccountActivityPipeline(BasePipeline):
    async def run(self):
        new_df = await self.ingestor.get_account_activity(config.SEVEN_DAYS_AGO, config.TODAY)

        if new_df.empty:
            print("no new account activity data to process.")
            return

        self.db.create_table(config.ACCOUNT_ACTIVITY_COLUMNS_MAP, config.ACCOUNT_ACTIVITY_TABLE)

        existing_df = self.db.fetch_table(config.ACCOUNT_ACTIVITY_TABLE, order_by="Date", limit=1)

        filtered_df = self.validator.validate_account_activity(existing_df, new_df)
        if not filtered_df.empty:
            self.db.insert_df_into_table(filtered_df, config.ACCOUNT_ACTIVITY_TABLE)
        # self.db.insert_df_into_table(filtered_df, config.ACCOUNT_ACTIVITY_TABLE)

        print("Account activity pipeline finished")
