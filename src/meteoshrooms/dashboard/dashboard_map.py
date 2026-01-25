import logging

import polars as pl
import streamlit as st
from plotly import express as px
from plotly.graph_objs import Figure

from meteoshrooms.dashboard.dashboard_utils import create_scatter_map_kwargs
from meteoshrooms.dashboard.dashboard_utils_streamlit import (
    META_STATIONS,
    create_station_frame_for_map_for_streamlit,
    update_selection,
)
from meteoshrooms.dashboard.log import init_logging

init_logging(__name__)
root_logger: logging.Logger = logging.getLogger(__name__)


def create_map_section(
    _metrics: pl.LazyFrame, param_short_code: str, time_period: int | None
):
    with st.container():
        fig: Figure = draw_map(_metrics, param_short_code, time_period)
        st.plotly_chart(
            fig,
            width='stretch',
            key='stations_selected_map',
            on_select=update_selection,
        )

        root_logger.debug('map created')


@st.cache_data
def draw_map(_metrics: pl.LazyFrame, param_short_code: str, time_period: int | None):
    if not time_period:
        time_period = 7
    station_frame_for_map: pl.DataFrame = create_station_frame_for_map_for_streamlit(
        META_STATIONS, _metrics, time_period
    )

    return px.scatter_map(
        station_frame_for_map,
        **create_scatter_map_kwargs(time_period, param_short_code),
    )
