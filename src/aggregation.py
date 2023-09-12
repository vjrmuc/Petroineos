from powerservice import trading
from datetime import datetime, timedelta
import pytz
import pandas as pd
from src.validation import Validation

class Aggregation(object):

    def __init__(self):
        pass

    def get_all_trades_for_date(self, date):
        """
        Combine trades for previous and current day to get the window of trades between 23:00 - 22:59
        :param date:
        :return:
        """
        if not bool(datetime.strptime(date, "%d/%m/%Y")):
            raise Exception(f"""Date is not in the expected format: {date}""")
        trades = trading.get_trades(date)
        date_converted = datetime.strptime(date, "%d/%m/%Y")
        trade_date_prev = date_converted - timedelta(days=1)
        trade_date_prev_string = trade_date_prev.strftime("%d/%m/%Y")
        trades_prev_day = trading.get_trades(trade_date_prev_string)

        df_trades = pd.DataFrame(trades)
        df_exploded = df_trades.explode(["time", "volume"])
        df_exploded = df_exploded[~df_exploded['time'].str.contains('23:.*', na=False)]
        df_trades_prev_day = pd.DataFrame(trades_prev_day)
        df_exploded_trades_prev_day = df_trades_prev_day.explode(["time", "volume"])

        df_exploded_trades_prev_day = df_exploded_trades_prev_day[df_exploded_trades_prev_day["date"].str.contains("23:.*", na=False)]
        trades_all = pd.concat([df_exploded_trades_prev_day, df_exploded])

        return trades_all

    def aggregate(self, location):
        """
        Aggregation is performed here.
        :param location: CSV output location
        :return:
        """
        current_date = datetime.now()
        local_time = current_date.astimezone(pytz.timezone('Europe/London'))
        local_date_str = local_time.strftime("%d/%m/%Y")
        local_date_time_str = local_time.strftime("%Y%m%d_%H%M")
        trades_all = self.get_all_trades_for_date(local_date_str)
        output_file_name = f"""{location}/PowerPosition_{local_date_time_str}.csv"""
        output_file_data_quality_name = f"""{location}/PowerPosition_{local_date_time_str}_data_profiling.csv"""

        validation = Validation()
        validated_df = validation.validate_data(trades_all, output_file_data_quality_name)
        print("after validation")

        validated_df["time"] = validated_df["time"].str.split(':', expand=True)[0]
        validated_df["time"] = validated_df["time"].apply(lambda x: f"{x}:00")

        df_grouped = validated_df.groupby([id, validated_df["time"], validated_df.date])
        df_grouped = df_grouped.agg(volume=pd.NamedAgg(column="volume", aggfunc="sum"),
                                    local_time=pd.NamedAgg(column="time", aggfunc="first"))
        df_grouped = df_grouped[["local_time", "volume"]]
        df_grouped.to_csv(output_file_name)
        print(df_grouped[["local_time", "volume"]])
        return df_grouped

