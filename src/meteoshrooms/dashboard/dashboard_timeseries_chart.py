from datetime import datetime, timedelta
from typing import Sequence
from zoneinfo import ZoneInfo

import polars as pl

from meteoshrooms.constants import TIMEZONE_SWITZERLAND_STRING
from meteoshrooms.data_preparation.constants import EXPR_WEATHER_AGGREGATION_TYPES


def create_area_chart_frame(
    frame_weather: pl.LazyFrame,
    stations_options_selected: Sequence[str],
    time_period: int,
    weather_column_names_dict,
) -> pl.LazyFrame:
    return (
        frame_weather.filter(
            (
                pl.col('reference_timestamp')
                >= (
                    datetime.now(tz=ZoneInfo(TIMEZONE_SWITZERLAND_STRING))
                    - timedelta(days=time_period)
                )
            )
            & (pl.col('station_name').is_in(stations_options_selected))
        )
        .sort('reference_timestamp')
        .group_by_dynamic('reference_timestamp', every='6h', group_by='station_name')
        .agg(EXPR_WEATHER_AGGREGATION_TYPES)
        .with_columns(pl.selectors.numeric().round(1))
        .rename(weather_column_names_dict)
    )
