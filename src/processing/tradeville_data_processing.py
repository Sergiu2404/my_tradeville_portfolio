import pandas as pd

class TradevilleDataProcessing:
    def to_dataframe(self, response):
        try:
            df = pd.DataFrame(response)
            return df
        except:
            print("Can't create data frame with available data")
            return None
