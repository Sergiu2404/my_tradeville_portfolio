# from src.pipelines.base_pipeline import BasePipeline
# from src.config import config
#
# class PortfolioSnapshotPipeline(BasePipeline):
#     async def run(self):
#         new_portfolio_snapshot_df = await self.ingestor.get_portfolio_snapshot()
#
#         self.db.create_table(config.PORTFOLIO_SNAPSHOTS_COLUMNS_MAP, config.PORTFOLIO_SNAPSHOTS_TABLE)
#         latest_record_portfolio_snapshots_table_df = self.db.execute_select_query(f'SELECT * FROM {config.PORTFOLIO_SNAPSHOTS_TABLE} ORDER BY "SnapshotDate" DESC LIMIT 1')
#
#         filtered_portfolio_snapshots_df = self.validator.validate_portfolio_snapshot(latest_record_portfolio_snapshots_table_df, new_portfolio_snapshot_df)
#
#         self.db.insert_df_into_table(filtered_portfolio_snapshots_df, config.PORTFOLIO_SNAPSHOTS_TABLE)
#         print("portfolio snapshot pipeline finished")
import pandas as pd

from src.pipelines.base_pipeline import BasePipeline
from src.config import config

class PortfolioSnapshotPipeline(BasePipeline):
    async def run(self):
        new_df = await self.ingestor.get_portfolio_snapshot()

        if new_df.empty:
            print("no new portfolio snapshot data to process.")
            return

        self.db.create_table(config.PORTFOLIO_SNAPSHOTS_COLUMNS_MAP, config.PORTFOLIO_SNAPSHOTS_TABLE)
        existing_df = self.db.fetch_table(config.PORTFOLIO_SNAPSHOTS_TABLE, order_by="SnapshotDate", limit=1)

        filtered_df = self.validator.validate_portfolio_snapshot(existing_df, new_df)
        if not filtered_df.empty:
            for col, dtype in filtered_df.dtypes.items():
                if "datetime" in str(dtype):
                    filtered_df[col] = filtered_df[col].dt.strftime("%Y-%m-%d %H:%M:%S")
            filtered_df = filtered_df.where(pd.notnull(filtered_df), None)
            self.db.insert_df_into_table(filtered_df, config.PORTFOLIO_SNAPSHOTS_TABLE)
        # self.db.insert_df_into_table(filtered_df, config.PORTFOLIO_SNAPSHOTS_TABLE)

        print("portfolio snapshot pipeline finished")
