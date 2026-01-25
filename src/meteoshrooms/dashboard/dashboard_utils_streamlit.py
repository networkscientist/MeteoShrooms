from typing import Sequence

import polars as pl
import streamlit as st

from meteoshrooms.dashboard.constants import (
    SIDEBAR_MAX_SELECTIONS,
    WEATHER_SHORT_LABEL_DICT,
)
from meteoshrooms.dashboard.dashboard_timeseries_chart import create_area_chart_frame
from meteoshrooms.dashboard.dashboard_utils import (
    create_metrics_names_dict,
    create_station_frame_for_map,
    create_station_names,
    load_metadata_to_frame,
    root_logger,
)
from meteoshrooms.dashboard.dataframe_io import load_metric_data, load_weather_data


@st.cache_data
def create_station_names_to_streamlit(
    _frame_with_stations: pl.LazyFrame,
) -> tuple[str, ...]:
    return create_station_names(_frame_with_stations)


@st.cache_data
def load_weather_data_to_streamlit() -> pl.DataFrame:
    return load_weather_data()


@st.cache_data
def load_metric_data_to_streamlit() -> pl.DataFrame:
    return load_metric_data()


@st.cache_data
def load_metadata_to_frame_to_streamlit(meta_type: str) -> pl.DataFrame:
    return load_metadata_to_frame(meta_type)


@st.cache_data
def create_metrics_names_dict_to_streamlit(
    meta_params_df: pl.DataFrame,
) -> dict[str, str]:
    return create_metrics_names_dict(meta_params_df)


META_PARAMETERS: pl.DataFrame = load_metadata_to_frame_to_streamlit('parameters')

META_STATIONS: pl.LazyFrame = load_metadata_to_frame_to_streamlit('stations').lazy()

METRICS_NAMES_DICT: dict[str, str] = create_metrics_names_dict_to_streamlit(
    META_PARAMETERS
)


@st.cache_data
def create_station_frame_for_map_for_streamlit(
    _frame_with_stations: pl.LazyFrame, _metrics: pl.LazyFrame, time_period: int
) -> pl.DataFrame:
    return create_station_frame_for_map(_frame_with_stations, _metrics, time_period)


def update_selection():
    try:
        if len(st.session_state.stations_selected_map.selection.points) > 0:
            new_selection = {
                pt['hovertext']
                for pt in st.session_state.stations_selected_map.selection.points
            }
            st.session_state.stations_selected_last_time = (
                st.session_state.stations_options_multiselect
            )
            root_logger.debug(len(st.session_state.stations_options_multiselect))
            if len(st.session_state.stations_options_multiselect) == 5:
                raise IndexError(
                    'You have already selected the maximum number of stations (5).'
                )
            if any(
                pt not in st.session_state.stations_options_multiselect
                for pt in new_selection
            ):
                root_logger.debug(new_selection)
                st.session_state.stations_options_multiselect = sorted(
                    st.session_state.stations_options_multiselect
                    + list(
                        new_selection.difference(
                            set(st.session_state.stations_options_multiselect)
                        )
                    )
                )[0:5]
        else:  # Pass for the time being
            pass
    except Exception as e:
        st.error(e)


def create_stations_options_selected(station_name_list) -> list:
    return st.multiselect(
        label='Stations',
        options=station_name_list,
        default='Airolo',
        max_selections=SIDEBAR_MAX_SELECTIONS,
        placeholder='Choose Station(s)',
        key='stations_options_multiselect',
    )


@st.cache_data
def create_area_chart(
    _df_weather: pl.LazyFrame,
    stations_options_selected: Sequence[str],
    time_period: int | None,
    param_short_code: str,
):
    if not time_period:
        time_period: int = 7
    st.area_chart(
        data=create_area_chart_frame(
            _df_weather,
            stations_options_selected,
            time_period,
            WEATHER_COLUMN_NAMES_DICT,
        ),
        x='Time',
        y='Precipitation',
        color='Station',
        x_label='Time',
        y_label=f'{WEATHER_SHORT_LABEL_DICT[param_short_code]} (mm)',
    )


WEATHER_COLUMN_NAMES_DICT: dict[str, str] = dict(
    {'reference_timestamp': 'Time', 'station_name': 'Station'} | METRICS_NAMES_DICT
)
