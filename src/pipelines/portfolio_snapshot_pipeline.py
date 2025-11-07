from src.pipelines.base_pipeline import BasePipeline
from src.config import config

class PortfolioSnapshotPipeline(BasePipeline):
    async def run(self):
        new_portfolio_snapshot_df = await self.ingestor.get_portfolio_snapshot()

        self.db.create_table(config.PORTFOLIO_SNAPSHOTS_COLUMNS_MAP, config.PORTFOLIO_SNAPSHOTS_TABLE)
        latest_record_portfolio_snapshots_table_df = self.db.execute_select_query(f'SELECT * FROM {config.PORTFOLIO_SNAPSHOTS_TABLE} ORDER BY "SnapshotDate" DESC LIMIT 1')

        filtered_portfolio_snapshots_df = self.validator.validate_portfolio_snapshot(latest_record_portfolio_snapshots_table_df, new_portfolio_snapshot_df)

        self.db.insert_df_into_table(filtered_portfolio_snapshots_df, config.PORTFOLIO_SNAPSHOTS_TABLE)
        print("portfolio snapshot pipeline finished")