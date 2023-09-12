from src.aggregation import Aggregation


aggregation = Aggregation()
aggregation.aggregate("29/06/2021")

# df_exploded["time"] = df_exploded["time"].str.split(':', expand=True)[0]
# df_exploded["time"] = df_exploded["time"].apply(lambda x: f"{x}:00")
#
# # df_exploded["hour"] = datetime.strptime(df_exploded['time'], "%H:%M").replace(minute=0)
# # df_exploded["hour"] = pd.to_datetime(df_exploded['time'], format= '%H:%M') + pd.offsets.DateOffset(minute=0)
# # df_exploded["hour"] = pd.to_datetime(df_exploded['time'],format= '%H:%M' ).replace(minute=0)
# df_grouped = df_exploded.groupby([id, df_exploded["time"], df.date])
# df_grouped = df_grouped.agg(count_col=pd.NamedAgg(column="volume", aggfunc="count"), volume=pd.NamedAgg(column="volume", aggfunc="sum"))

# print(df_grouped.head().to_markdown())

