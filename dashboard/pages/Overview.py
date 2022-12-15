from functools import partial
import streamlit as st
import pandas as pd
import pathlib as pl
import json
import datetime as dt
import matplotlib.pyplot as plt
import numpy as np
import sys 
import json


sys.path.append(str(pl.Path(__file__).parent.parent))

from plots import map_plot
from dashboard_utils import load_df_from_db, filter_df

DEP_TABLE_NAME = "MVG1"
TRANS_TABLE_NAME = "MVG_Trans1"
RENDER_COLS = ["timestamp",
               "station", "line",
               "destination", "delay"]


st.sidebar.header('User Input Features')

df = load_df_from_db(DEP_TABLE_NAME)

station_names = sorted(df["station"].unique())
# selected_stations = st.sidebar.multiselect("station",
#                                            station_names,
#                                            ["Pasing"])

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
     value=(dt.time(0, 00), dt.time(23, 59)))


filter_dict = {"time": selected_time}

if len(selected_date) > 0:
    # print(selected_date)
    date_filter = {"date": selected_date}
    filter_dict.update(date_filter)

print(filter_dict)

if weekend_filter:
    filter_dict.update(weekend_filter)


filtered_df = filter_df(df, filter_dict)

# selected_line = st.sidebar.multiselect("line",
#                                        sorted(filtered_df["line"].unique()),
#                                        sorted(filtered_df["line"].unique()))


# filtered_df["time"] = filtered_df["time"].astype(str)
# filtered_df = filtered_df[filtered_df["line"].isin(selected_line)]
# st.dataframe(filtered_df.loc[:, RENDER_COLS])

map_plot(filtered_df)