import pandas as pd
import pytz

BUCHAREST_TIMEZONE = pytz.timezone("Europe/Bucharest")

class Validator:
    def validate_dividends(self, dividends_table_df: pd.DataFrame, new_dividends_df: pd.DataFrame):
        if dividends_table_df.empty:
            return new_dividends_df

        dividends_table_df["Key"] = (dividends_table_df["Symbol"] +
                                     "_" +
                                     pd.to_datetime(dividends_table_df["Date"])
                                     .dt.date.astype(str)
                                     )
        new_dividends_df["Key"] = (new_dividends_df["Symbol"] +
                                   "_" +
                                   pd.to_datetime(new_dividends_df["Date"])
                                   .dt.date.astype(str)
                                   )

        filtered_df = new_dividends_df[~new_dividends_df["Key"].isin(dividends_table_df["Key"])]
        return filtered_df.drop(columns=["Key"])

    def validate_portfolio_snapshot(self, last_snapshot_df: pd.DataFrame, new_snapshot_df: pd.DataFrame):
        if last_snapshot_df.empty:
            return new_snapshot_df

        latest_date = last_snapshot_df["SnapshotDate"].iloc[0]
        filtered_df = new_snapshot_df[new_snapshot_df["SnapshotDate"] > latest_date]

        return filtered_df

    def validate_portfolio_symbols_daily_values(self, latest_daily_values_record_df: pd.DataFrame, new_daily_values_df: pd.DataFrame):
        if latest_daily_values_record_df.empty:
            return new_daily_values_df

        latest_date = pd.to_datetime(latest_daily_values_record_df["Date"].iloc[0])
        new_daily_values_df["Date"] = pd.to_datetime(new_daily_values_df["Date"])
        filtered_df = new_daily_values_df[new_daily_values_df["Date"] > latest_date]

        return filtered_df

    def validate_account_activity(self, latest_account_activity: pd.DataFrame, new_account_activity: pd.DataFrame):
        if latest_account_activity.empty:
            return new_account_activity

        latest_date = latest_account_activity["Date"].iloc[0]
        filtered_df = new_account_activity[new_account_activity["Date"] > latest_date]

        return filtered_df


