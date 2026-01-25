from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import polars as pl

from meteoshrooms.constants import (
    DATA_PATH,
    TIMEZONE_SWITZERLAND_STRING,
)


def load_weather_data(data_path=DATA_PATH) -> pl.DataFrame:
    return (
        pl.read_parquet(Path(data_path, 'weather_data.parquet'))
        .with_columns(
            pl.col('reference_timestamp').dt.replace_time_zone(
                TIMEZONE_SWITZERLAND_STRING, non_existent='null'
            )
        )
        .sort(pl.col('reference_timestamp'))
        .filter(
            (
                pl.col('reference_timestamp')
                >= (
                    datetime.now(tz=ZoneInfo(TIMEZONE_SWITZERLAND_STRING))
                    - timedelta(days=30)
                )
            )
        )
    )


def load_metric_data(data_path=DATA_PATH) -> pl.DataFrame:
    return pl.read_parquet(Path(DATA_PATH, 'metrics.parquet')).pivot(
        'parameter',
        index=('station_abbr', 'station_name', 'time_period'),
        values='value',
    )
