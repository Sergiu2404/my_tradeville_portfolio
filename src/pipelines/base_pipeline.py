from src.data_validator.validator import Validator
from src.ingestion.ingest_data import MarketDataIngestor
from src.storage.db_context import DbContext

class BasePipeline:
    def __init__(self, db_context: DbContext, ingestor: MarketDataIngestor, validator: Validator):
        self.db = db_context
        self.ingestor = ingestor
        self.validator = validator

    async def run(self):
        raise NotImplementedError("Each pipeline implements its own run()")