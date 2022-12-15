from functools import partial
import streamlit as st
import pandas as pd
import pathlib as pl
import json
import datetime as dt
import matplotlib.pyplot as plt
import numpy as np

from plots import houly_plot, map_plot
from dashboard_utils import load_df_from_db, filter_df


DEP_TABLE_NAME = "MVG1"
TRANS_TABLE_NAME = "MVG_Trans1"
RENDER_COLS = ["timestamp",
               "station", "line",
               "destination", "delay"]



    # st.dataframe(df.loc[:, RENDER_COLS])

st.title("Overview Page for all station")
st.markdown("""
    This app shows the historic and realtime delays 
    and departures for selected S-Bahn Stations across Munich
""")

st.sidebar.header('User Input Features')

df = load_df_from_db(DEP_TABLE_NAME)

station_names = sorted(df["station"].unique())
selected_stations = st.sidebar.multiselect("station",
                                           station_names,
                                           ["Pasing", "Laim"])

selected_date = st.sidebar.date_input(
    "Filter by Date",
    value=[
        dt.datetime.today(), dt.datetime.today()],
    max_value=dt.datetime.today()
)

is_weekend_selected = st.sidebar.checkbox("include weekend", value=True)
is_workday_selected = st.sidebar.checkbox("include workdays", value=True)


if is_workday_selected and not is_weekend_selected:
    weekend_filter = {"is_weekend": False}
elif not is_workday_selected and is_weekend_selected:
    weekend_filter = {"is_weekend": True}
else:
    weekend_filter = {}

selected_time = st.slider(
     "Select the time of day to filter",
     value=(dt.time(11, 30), dt.time(18, 45)))


filter_dict = {"station": selected_stations,
               "time": selected_time,
               }
if len(selected_date) > 0:
    # print(selected_date)
    date_filter = {"date": selected_date}
    filter_dict.update(date_filter)


if weekend_filter:
    filter_dict.update(weekend_filter)


filtered_df = filter_df(df, filter_dict)

selected_line = st.sidebar.multiselect("line",
                                       sorted(filtered_df["line"].unique()),
                                       sorted(filtered_df["line"].unique()))


# filtered_df["time"] = filtered_df["time"].astype(str)
filtered_df = filtered_df[filtered_df["line"].isin(selected_line)]
# st.dataframe(filtered_df.loc[:, RENDER_COLS])


# st.table(filtered_df.loc[:, ["hour", "delay", "line"]].groupby(["hour", "line"]).mean())

# filtered_df["time"] = filtered_df["time"].astype(str)
filtered_df = filtered_df[filtered_df["line"].isin(selected_line)]
# st.dataframe(filtered_df.loc[:, RENDER_COLS])
houly_plot(filtered_df)

# st.table(filtered_df.loc[:, ["hour", "delay", "line"]].groupby(["hour", "line"]).mean())
# grouped = filtered_df.loc[:, ["delay", "station", "lon", "lat"]].groupby("station").count()
# df_test = grouped.reset_index()

# for _, stn in df_test.iterrows():
#     filtered_df.loc[filtered_df["station"] == stn.station, ["delay"]] = filtered_df.loc[filtered_df["station"] == stn.station, ["delay"]] / stn.delay

# print(filtered_df[filtered_df["station"]=="Laim"]["delay"].sum(),filtered_df[filtered_df["station"]=="Puchheim"]["delay"].sum())
# other_df = filtered_df.loc[:, ["delay", "lon", "lat"]]
# map_plot(filtered_df)
# st.bar_chart(filtered_df)