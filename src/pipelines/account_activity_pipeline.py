from src.pipelines.base_pipeline import BasePipeline
from src.config import config

class AccountActivityPipeline(BasePipeline):
    async def run(self):
        new_account_activity_df = await self.ingestor.get_account_activity(config.SEVEN_DAYS_AGO, config.TODAY)

        self.db.create_table(config.ACCOUNT_ACTIVITY_COLUMNS_MAP, config.ACCOUNT_ACTIVITY_TABLE)
        existing_df = self.db.execute_select_query(
            f'SELECT * FROM {config.ACCOUNT_ACTIVITY_TABLE} ORDER BY "Date" DESC LIMIT 1'
        )

        filtered_df = self.validator.validate_account_activity(existing_df, new_account_activity_df)
        self.db.insert_df_into_table(filtered_df, config.ACCOUNT_ACTIVITY_TABLE)
        print("account activity pipeline finished")
