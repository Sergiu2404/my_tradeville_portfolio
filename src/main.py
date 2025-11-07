import asyncio

from src.data_sources.tradeville_api import TradevilleAPI
from src.data_sources.yahoofinance_api import YahooFinanceAPI
from src.data_validator.validator import Validator
from src.ingestion.ingest_data import MarketDataIngestor
from src.pipelines.account_activity_pipeline import AccountActivityPipeline
from src.pipelines.dividends_pipeline import DividendsPipeline
from src.pipelines.portfolio_snapshot_pipeline import PortfolioSnapshotPipeline
from src.pipelines.portfolio_symbols_daily_values import PortfolioSymbolsDailyValues
from src.storage.db_context import DbContext
from src.config import config

async def main():
    db = DbContext(config.USER, config.PASSWORD, config.HOST, config.PORT, config.DBNAME)
    ingestor = MarketDataIngestor(TradevilleAPI(), YahooFinanceAPI())
    validator = Validator()

    pipeline = AccountActivityPipeline(db, ingestor, validator)
    await pipeline.run()
    pipeline = DividendsPipeline(db, ingestor, validator)
    await pipeline.run()
    pipeline = PortfolioSnapshotPipeline(db, ingestor, validator)
    await pipeline.run()
    pipeline = PortfolioSymbolsDailyValues(db, ingestor, validator)
    await pipeline.run()


if __name__ == '__main__':
    asyncio.run(main())


# from src.config import config
# from src.storage.db_context import DbContext
# db_context = DbContext(config.USER, config.PASSWORD, config.HOST, config.PORT, config.DBNAME)
# db_context.drop_table(config.DIVIDENDS_TABLE)
# db_context.drop_table(config.PORTFOLIO_SNAPSHOTS_TABLE)
# db_context.drop_table(config.PORTFOLIO_SYMBOLS_DAILY_VALUES_TABLE)
# db_context.drop_table(config.ACCOUNT_ACTIVITY_TABLE)