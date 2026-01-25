import logging
import re
from pathlib import Path
from typing import Any

import polars as pl
from plotly import express as px

from meteoshrooms.constants import (
    DATA_PATH,
    parameter_description_extraction_pattern,
)
from meteoshrooms.dashboard.constants import (
    COLUMNS_FOR_MAP_FRAME,
    METRICS_STRINGS,
    WEATHER_SHORT_LABEL_DICT,
)
from meteoshrooms.dashboard.log import init_logging

init_logging(__name__)
root_logger: logging.Logger = logging.getLogger(__name__)


def load_metadata_to_frame(meta_type: str, data_path=DATA_PATH) -> pl.DataFrame:
    """Load metadata

    Returns
    -------
        Metadata in Polars DataFrame
    """
    return pl.read_parquet(
        Path(data_path, f'meta_{meta_type.lower()}.parquet')
    ).unique()


def create_metrics_names_dict(meta_params_df: pl.DataFrame) -> dict[str, str]:
    return {m: create_meta_map(meta_params_df).get(m, '') for m in METRICS_STRINGS}


def create_station_names(frame_with_stations: pl.LazyFrame) -> tuple[str, ...]:
    return tuple(
        frame_with_stations.unique(subset=('station_name',))
        .sort('station_name')
        .select('station_name')
        .collect()
        .to_series()
        .to_list()
    )


def create_meta_map(metadata: pl.DataFrame):
    meta_map: dict = {
        r['parameter_shortname']: (
            result.group()
            if (
                result := re.search(
                    parameter_description_extraction_pattern,
                    r['parameter_description_en'],
                )
            )
            is not None
            else None
        )
        for r in collect_meta_params_to_dicts(metadata)
    }
    return meta_map


def collect_meta_params_to_dicts(metadata: pl.DataFrame) -> tuple[dict[str, Any], ...]:
    return tuple(
        metadata.to_dicts(),
    )


def create_scatter_map_kwargs(
    time_period, param_short_code
) -> dict[str, str | dict[str, bool] | list[str | Any] | int | None]:
    return {
        'lat': 'station_coordinates_wgs84_lat',
        'lon': 'station_coordinates_wgs84_lon',
        'color': (WEATHER_SHORT_LABEL_DICT.get(param_short_code, 'Station Type')),
        'hover_name': 'station_name',
        'hover_data': {
            'Station Type': False,
            'station_coordinates_wgs84_lat': False,
            'station_coordinates_wgs84_lon': False,
            'Short Code': True,
            'Altitude': True,
        },
        'color_continuous_scale': px.colors.cyclical.IceFire,
        'size_max': 15,
        'zoom': 6,
        'map_style': 'carto-positron',
        'title': (WEATHER_SHORT_LABEL_DICT.get(param_short_code, 'Stations')),
        'subtitle': (
            f'Over the last {time_period} days'
            if param_short_code in WEATHER_SHORT_LABEL_DICT
            else None
        ),
    }


def create_station_frame_for_map(
    _frame_with_stations: pl.LazyFrame, _metrics: pl.LazyFrame, time_period: int
) -> pl.DataFrame:
    return (
        _frame_with_stations.with_columns(
            pl.col('station_type_en').alias('Station Type'),
            pl.col('station_abbr').alias('Short Code'),
            Altitude=pl.col('station_height_masl')
            .cast(pl.Int16)
            .cast(pl.String)
            .add(' m.a.s.l'),
        )
        .select(pl.col(COLUMNS_FOR_MAP_FRAME))
        .join(
            _metrics.filter(pl.col('time_period') == time_period),
            left_on='Short Code',
            right_on='station_abbr',
        )
        .rename(WEATHER_SHORT_LABEL_DICT)
        .collect()
    )
