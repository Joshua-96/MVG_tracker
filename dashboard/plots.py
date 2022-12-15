import matplotlib.pyplot as plt
import streamlit as st
import numpy as np
import pandas as pd
import pydeck as pdk
from pydeck.types import String

def houly_plot(df: pd.DataFrame):
    lines = df.line.unique()
    fig, ax = plt.subplots()
    df["hour"] = df["timestamp"].dt.hour
    hours = sorted(df["hour"].unique())
    cum_delays = np.zeros((len(hours)), "float16")
    default_dict = {h: 0.0 for h in hours}
    temp_df = (df.loc[:, ["line", "hour", "delay"]]
               .groupby(["line", "hour"])
               .mean()
               .reset_index())
    temp_df = temp_df[temp_df["line"].isin(lines)]
    temp_df["delay"] = temp_df["delay"].fillna(0)

    for l in lines:
        delay = temp_df.loc[temp_df["line"] == l, ["delay"]].values[:,0]
        ax.bar(hours, delay, 0.5, bottom=cum_delays, label=l) 
        cum_delays = cum_delays + np.array(list(default_dict.values()))

    ax.set_ylabel("Average Delay")
    ax.legend()
    # plt.plot(df_grouped["delay"])
    # plt.xlabel("time of day")
    # plt.ylabel("average delay")
    return st.pyplot(fig)


def map_plot(df):
    tooltip = {
    "html": "<b>Elevation Value:</b> {delay} <br/> <b>Station Value:</b> {station}",
    "style": {
        "backgroundColor": "steelblue",
        "color": "white"
        }
    }

    grouped = df.loc[:, ["delay", "station", "lon", "lat"]].groupby("station").mean()
    df_test = grouped.reset_index()
    df_test["delay"] = df_test["delay"].round(2)
    df_test.dropna(inplace=True)
    # df["delay"] = (df["delay"]*10000).astype("uint32")
    # print(df["delay"])
    # df_test = pd.concat([df_test, df_test])
    layer = pdk.Layer(
                'ColumnLayer',
                data=df_test,
                get_position='[lon, lat]',
                get_elevation=['delay'],
                radius=300,
                elevation_scale=100,
                elevation_range=[0, 10],
                pickable=True,
                extruded=True,
                aggregation=String("SUM"),
                # elevationAggregation=String('MAX'),
                # colorAggregation="MEAN"
            ),
            # pdk.Layer(
            #     'ScatterplotLayer',
            #     data=[:,["delay", "lon", "lat"]],
            #     get_position='[lon, lat]',
            #     get_color='[200, 30, 0, 160]',
            #     get_radius=200,
            # ),
    # df =df.loc[:, ["delay", "station", "lon", "lat"]]
    # df["delay"] = (df["delay"].clip(0, 120) * 100).astype("uint16")
    st.pydeck_chart(pdk.Deck(
        map_style='mapbox://styles/mapbox/navigation-night-v1',
        initial_view_state=pdk.ViewState(
            latitude=48.149852,
            longitude=11.461872,
            zoom=11,
            pitch=50,
        ),
        layers=[layer],
        tooltip=tooltip
    )
)