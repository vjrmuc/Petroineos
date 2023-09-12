from powerservice import trading
from datetime import datetime, timedelta
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
        trade_date_prev = datetime.strptime(date, "%d/%m/%Y") - timedelta(days=1)
        trade_date_prev_string = trade_date_prev.strftime("%d/%m/%Y")
        trades_prev_day = trading.get_trades(trade_date_prev_string)

        df_trades = pd.DataFrame(trades)
        df_exploded = df_trades.explode(["time", "volume"])
        df_exploded = df_exploded[~df_exploded['time'].str.contains('23:.*', na=False)]
        print(df_exploded.head)
        df_trades_prev_day = pd.DataFrame(trades_prev_day)
        df_exploded_trades_prev_day = df_trades_prev_day.explode(["time", "volume"])
        df_exploded_trades_prev_day = df_exploded_trades_prev_day.filter(df_exploded_trades_prev_day["date"].str.contains("23:.*"))
        trades_all = pd.concat([df_exploded_trades_prev_day, df_exploded])
        return trades_all

    def aggregate(self, date):
        """
        Aggregation is performed here.
        :param date: Date for aggregation
        :return:
        """
        trades_all = self.get_all_trades_for_date(date)

        print(trades_all.head().to_markdown())

        validation = Validation()
        validated_df = validation.validate_data(trades_all)
        print(validated_df.head().to_markdown())
        validated_df["time"] = validated_df["time"].str.split(':', expand=True)[0]
        validated_df["time"] = validated_df["time"].apply(lambda x: f"{x}:00")

        df_grouped = validated_df.groupby([id, validated_df["time"], validated_df.date])
        df_grouped = df_grouped.agg(count_col=pd.NamedAgg(column="volume", aggfunc="count"),
                                    volume=pd.NamedAgg(column="volume", aggfunc="sum"))

        return df_grouped

