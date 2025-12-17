"""Prepare data for the MeteoShrooms dashboard"""

import logging
import tempfile
from collections.abc import Sequence
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Iterable, Mapping
from zoneinfo import ZoneInfo

import polars as pl
import polars.exceptions
import requests
import typer
from requests.adapters import HTTPAdapter, Retry
from rich.progress import track
from typing_extensions import Annotated

from meteoshrooms.constants import DATA_PATH, TIMEZONE_SWITZERLAND_STRING
from meteoshrooms.data_preparation.constants import (
    ARGS_LOAD_META_DATAINVENTORY,
    ARGS_LOAD_META_PARAMETERS,
    ARGS_LOAD_META_STATIONS,
    DTYPE_DICT,
    EXPR_WEATHER_AGGREGATION_TYPES,
    META_FILE_PATH_DICT,
    METEO_CSV_ENCODING,
    PARAMETER_AGGREGATION_TYPES,
    SINK_PARQUET_KWARGS,
    STATION_TYPE_ERROR_STRING,
    TIME_PERIODS,
    TIMEFRAME_STRINGS,
    TIMEFRAME_VALUE_ERROR_STRING,
    TIMEZONE_EXPRESSION,
    URL_GEO_ADMIN_BASE,
    URL_GEO_ADMIN_STATION_TYPE_BASE,
)

logger: logging.Logger = logging.getLogger(__name__)
console_handler = logging.StreamHandler()
logger.addHandler(console_handler)
log_formatter = logging.Formatter(
    '{asctime} - {levelname} - {message}',
    style='{',
    datefmt='%Y-%m-%d %H%M',
)
console_handler.setFormatter(log_formatter)

EXPR_METRICS_AGGREGATION_TYPE_WHEN_THEN: pl.Expr = (
    pl.when(pl.col('parameter').is_in(PARAMETER_AGGREGATION_TYPES['sum']))
    .then(pl.lit('sum'))
    .otherwise(pl.lit('mean'))
    .alias('type')
)

app = typer.Typer()


def load_metadata_to_lazyframe(
    meta_type: str,
    meta_schema: Mapping[str, type[pl.DataType]],
    meta_cols_to_keep: Sequence[str],
    data_path: Path,
) -> pl.LazyFrame:
    """Load metadata from a Parquet file.

    Parameters
    ----------
    data_path
    meta_type: str
        Metadata source, one of 'parameters', 'frame_meta' or 'datainventory'
    file_path_dict: dict[str, list[str]]
        Dict with URLs to metadata
    meta_schema: dict
        Dict with polars schema, structured as 'column_name': polars.Datatype
    meta_cols_to_keep: Sequence[str]
        Column names to keep in metadata DataFrame

    Returns
    -------
        Metadata loaded into a LazyFrame
    """
    frame_meta: pl.LazyFrame = pl.concat(
        [
            pl.read_csv(
                file_path,
                encoding=METEO_CSV_ENCODING,
                separator=';',
                schema=meta_schema,
                columns=meta_cols_to_keep,
            )
            for file_path in META_FILE_PATH_DICT[meta_type]
        ]
    ).lazy()
    frame_meta.sink_parquet(Path(data_path, f'meta_{meta_type}.parquet'))
    logger.debug('frame_meta written to parquet')
    return frame_meta


def load_metadata_per_type(meta_type: str, data_path, meta_schema, meta_cols_to_keep):
    metadata = load_metadata_to_lazyframe(
        meta_type, meta_schema, meta_cols_to_keep, data_path
    )
    logger.debug(f'meta_{meta_type} generated as {type(metadata)}')
    return metadata


class DataPreparation:
    def __init__(
        self,
        download_path,
        data_path: Path | None = None,
        update_flag=False,
    ):
        self.download_path = download_path
        if data_path:
            self.data_path = data_path
        else:
            self.data_path = DATA_PATH
        self.update_flag = update_flag

    def create_weather_schema_dict(self) -> dict[Any, type[pl.DataType]]:
        self.weather_schema_dict = {
            colname: DTYPE_DICT[datatype]
            for colname, datatype in self.meta_parameters.select(
                pl.col('parameter_shortname'), pl.col('parameter_datatype')
            )
            .collect()
            .iter_rows()
        }
        logger.debug(
            f'weather_schema_dict generated as {type(self.weather_schema_dict)}'
        )
        return self.weather_schema_dict

    def load_meta_parameters(self):
        self.meta_parameters = load_metadata_per_type(
            'parameters', self.data_path, *ARGS_LOAD_META_PARAMETERS
        )
        return self.meta_parameters

    def load_meta_datainventory(self):
        self.meta_datainventory = load_metadata_per_type(
            'datainventory', self.data_path, *ARGS_LOAD_META_DATAINVENTORY
        )
        return self.meta_datainventory

    def load_meta_stations(self):
        self.meta_stations = load_metadata_per_type(
            'stations', self.data_path, *ARGS_LOAD_META_STATIONS
        )
        return self.meta_stations

    def load_metadata(self):
        return (
            self.load_meta_parameters(),
            self.load_meta_datainventory(),
            self.load_meta_stations(),
        )

    def load_weather_data(self):
        if not hasattr(self, 'weather_schema_dict'):
            self.weather_schema_dict = self.create_weather_schema_dict()
        self.weather_data: pl.LazyFrame = load_weather(
            self.meta_stations,
            schema_dict_lazyframe=self.weather_schema_dict,
            down_path=self.download_path,
            update_data=self.update_flag,
        )
        logger.debug(f'weather_data generated as {type(self.weather_data)}')
        weather_data_file_path: Path = Path(self.data_path, 'weather_data.parquet')
        self.weather_data.sink_parquet(weather_data_file_path, **SINK_PARQUET_KWARGS)
        logger.debug(f'weather_data written to {weather_data_file_path}')
        return self.weather_data

    def load_metrics(self):
        self.metrics: pl.LazyFrame = create_metrics(self.weather_data, TIME_PERIODS)
        logger.debug(f'metrics generated as {type(self.metrics)}')
        metrics_file_path: Path = Path(self.data_path, 'metrics.parquet')
        self.metrics.sink_parquet(metrics_file_path, **SINK_PARQUET_KWARGS)
        logger.debug(f'metrics written to {metrics_file_path}')
        return self.metrics


def combine_urls_parts_to_string(
    station: pl.Series, station_type_string: str, timeframe: str
) -> pl.Series:
    return (
        f'{URL_GEO_ADMIN_BASE}/{URL_GEO_ADMIN_STATION_TYPE_BASE}{station_type_string}/'
        + station
        + f'/ogd-smn{station_type_string}_'
        + station
        + f'_h_{timeframe}.csv'
    )


def generate_download_urls(
    station_series: pl.Series, station_type: str, timeframe: str
) -> pl.Series:
    check_generate_download_urls_arguments_or_raise_error(station_type, timeframe)
    station_type_string = str()
    match station_type:
        case 'rainfall':
            station_type_string = '-precip'
        case 'weather':
            station_type_string = ''
    return combine_urls_parts_to_string(station_series, station_type_string, timeframe)


def check_generate_download_urls_arguments_or_raise_error(
    station_type: str, timeframe: str
) -> None:
    if timeframe not in TIMEFRAME_STRINGS:
        raise ValueError(TIMEFRAME_VALUE_ERROR_STRING)
    if not isinstance(station_type, str):
        raise TypeError(STATION_TYPE_ERROR_STRING)


def expr_filter_column_timedelta(col_name: str, delta_time: int) -> pl.Expr:
    return pl.col(col_name) >= pl.lit(
        datetime.now(tz=ZoneInfo(TIMEZONE_SWITZERLAND_STRING))
        - timedelta(days=delta_time)
    )


def load_weather(
    metadata: pl.LazyFrame,
    schema_dict_lazyframe: Mapping[str, type[pl.DataType]],
    down_path: Path,
    update_data=False,
) -> pl.LazyFrame:
    # Create stations dataframe
    stations: pl.DataFrame = filter_unique_station_names(metadata).collect()
    # Create dict for lazyframe kwargs
    kwargs_lazyframe: dict = create_kwargs_lazyframe(schema_dict_lazyframe)
    # Generate polars.Series with station names for both stations types
    station_series_precipitation: pl.Series = filter_stations_to_series(
        stations, station_type='Automatic precipitation stations'
    )
    station_series_weather: pl.Series = filter_stations_to_series(
        stations, station_type='Automatic weather stations'
    )
    # Download most recent CSV files for both station types
    download_files(
        generate_file_path_series(
            station_series_precipitation, station_series_weather, timeframe='now'
        ),
        down_path,
    )
    # If data only needs to be updated, do that
    if update_data:
        return update_weather_data(
            down_path,
            kwargs_lazyframe,
            metadata,
            station_series_precipitation,
            station_series_weather,
        )
    urls_weather: pl.Series = pl.concat(
        generate_download_urls(station_series_weather, 'weather', period)
        for period in TIMEFRAME_STRINGS
    )
    urls_rainfall: pl.Series = pl.concat(
        generate_download_urls(station_series_precipitation, 'rainfall', period)
        for period in TIMEFRAME_STRINGS
    )
    download_files(
        generate_file_path_series(
            station_series_precipitation, station_series_weather, timeframe='recent'
        ),
        down_path,
    )
    # download_files(
    #     pl.concat(
    #         generate_download_urls(station_series, station_type, 'recent')
    #         for station_series, station_type in zip(
    #             (station_series_weather, station_series_precipitation),
    #             ('weather', 'rainfall'),
    #             strict=False,
    #         )
    #     ),
    #     down_path,
    # )
    try:
        weather: pl.LazyFrame = create_rainfall_weather_lazyframes(
            down_path, urls_weather, kwargs_lazyframe
        )
        rainfall: pl.LazyFrame = create_rainfall_weather_lazyframes(
            down_path, urls_rainfall, kwargs_lazyframe
        )
    except polars.exceptions.ComputeError:
        weather = create_rainfall_weather_dataframes(
            down_path, urls_weather, kwargs_lazyframe
        )
        rainfall = create_rainfall_weather_dataframes(
            down_path, urls_rainfall, kwargs_lazyframe
        )
    return concat_rainfall_weather_lazyframes(metadata, rainfall, weather)


def generate_file_path_series(
    station_series_precipitation: pl.Series,
    station_series_weather: pl.Series,
    timeframe: str,
) -> pl.Series:
    return pl.concat(
        generate_download_urls(station_series, station_type, timeframe)
        for station_series, station_type in zip(
            (station_series_weather, station_series_precipitation),
            ('weather', 'rainfall'),
            strict=False,
        )
    )


def create_kwargs_lazyframe(
    schema_dict_lazyframe: Mapping[str, type[pl.DataType]],
) -> dict[str, str | bool | Mapping[str, type[pl.DataType]]]:
    return {
        'separator': ';',
        'try_parse_dates': True,
        'schema_overrides': schema_dict_lazyframe,
    }


def update_weather_data(
    down_path: Path,
    kwargs_lazyframe: dict,
    metadata: pl.LazyFrame,
    station_series_precipitation: pl.Series,
    station_series_weather: pl.Series,
) -> pl.LazyFrame:
    weather: pl.LazyFrame = pl.scan_parquet(Path(DATA_PATH, 'weather_data.parquet'))
    urls_weather: pl.Series = generate_download_urls(
        station_series_weather, 'weather', 'now'
    )
    urls_rainfall: pl.Series = generate_download_urls(
        station_series_precipitation, 'rainfall', 'now'
    )
    weather_now: pl.LazyFrame = create_rainfall_weather_lazyframes(
        down_path, urls_weather, kwargs_lazyframe
    )
    rainfall_now: pl.LazyFrame = create_rainfall_weather_lazyframes(
        down_path, urls_rainfall, kwargs_lazyframe
    )
    weather_new: pl.LazyFrame = concat_rainfall_weather_lazyframes(
        metadata, rainfall_now, weather_now
    )
    weather_max_timestamp: datetime = (
        weather.select('reference_timestamp').max().collect().item()
    )
    return (
        pl.concat(
            (
                weather_new.filter(
                    pl.col('reference_timestamp') > weather_max_timestamp
                )
                .select(weather.drop('station_name').collect_schema().names())
                .join(
                    metadata.select(('station_abbr', 'station_name')),
                    on=['station_abbr'],
                ),
                weather,
            )
        )
        .filter(expr_filter_column_timedelta('reference_timestamp', 31))
        .unique()
    )


def concat_rainfall_weather_lazyframes(
    metadata: pl.LazyFrame, frame_rainfall: pl.LazyFrame, frame_weather: pl.LazyFrame
) -> pl.LazyFrame:
    return (
        pl.concat([frame_rainfall, frame_weather], how='diagonal')
        .sort('reference_timestamp')
        .filter(expr_filter_column_timedelta('reference_timestamp', 31))
        .group_by_dynamic('reference_timestamp', every='1h', group_by='station_abbr')
        .agg(*EXPR_WEATHER_AGGREGATION_TYPES)
        .join(
            metadata.select(('station_abbr', 'station_name')),
            on=['station_abbr'],
        )
        .unique()
    )


def filter_unique_station_names(metadata: pl.LazyFrame) -> pl.LazyFrame:
    return (
        metadata.select('station_abbr', 'station_type_en')
        .unique('station_abbr')
        .sort('station_abbr')
    )


def create_rainfall_weather_lazyframes(
    down_path: Path, station_urls: pl.Series, kwargs_lazyframe: dict
) -> pl.LazyFrame:
    """Create LazyFrame from CSV urls

    Parameters
    ----------
    station_series: pl.Series
        Station names
    station_type: str
        Station type, one of 'rainfall' or 'weather'
    timeframe: str
        Time range, one of 'recent' or 'now'
    kwargs_lazyframe: dict
        Arguments to pass to LazyFrame constructor

    Returns
    -------
        Polars LazyFrame with rainfall/weather data
    """

    try:
        return (
            scan_csv_from_urls(down_path, kwargs_lazyframe, station_urls)
            .with_columns(TIMEZONE_EXPRESSION)
            .collect()
            .lazy()
        )
    except polars.exceptions.ComputeError:
        raise


def scan_csv_from_urls(
    down_path: Path, kwargs_lazyframe: dict, station_urls
) -> pl.LazyFrame:
    return pl.scan_csv(
        tuple(Path(down_path, Path(url).name) for url in station_urls),
        **kwargs_lazyframe,
    )


def download_files(urls: Iterable[str], down_path: Path):
    with requests.Session() as s:
        retries = Retry(
            total=5, backoff_factor=0.1, status_forcelist=[500, 502, 503, 504]
        )
        s.mount('https://', HTTPAdapter(max_retries=retries))
        for url in track(urls, description='Downloading....'):
            try:
                r = s.get(url)
                with Path(Path(down_path, Path(url).name)).open('wb') as f:
                    f.write(r.content)
                logger.debug(f'file {Path(down_path, Path(url).name)} written.')
            except Exception as e:
                print('Exception in download_url():', e)


def create_rainfall_weather_dataframes(
    down_path: Path,
    station_urls,
    kwargs_lazyframe: dict,
) -> pl.LazyFrame:
    """Create LazyFrame from CSV urls

    Parameters
    ----------
    station_series: pl.Series
        Station names
    station_type: str
        Station type, one of 'rainfall' or 'weather'
    timeframe: str
        Time range, one of 'recent' or 'now'
    kwargs_lazyframe: dict
        Arguments to pass to LazyFrame constructor

    Returns
    -------
        Polars LazyFrame with rainfall/weather data
    """
    return (
        read_csv_from_urls(down_path, kwargs_lazyframe, station_urls)
        .with_columns(TIMEZONE_EXPRESSION)
        .lazy()
    )


def read_csv_from_urls(
    down_path: Path, kwargs_lazyframe: dict, station_urls
) -> pl.DataFrame:
    return pl.concat(
        tuple(
            pl.read_csv(
                Path(down_path, Path(url).name),
                **kwargs_lazyframe,
            )
            for url in station_urls
        ),
        how='diagonal',
    )


def create_metrics(
    weather_data: pl.LazyFrame, time_periods: Mapping[int, datetime]
) -> pl.LazyFrame:
    return (
        concat_metrics_frame(time_periods, weather_data)
        .unpivot(
            index=('station_abbr', 'station_name', 'time_period'),
            variable_name='parameter',
        )
        .drop_nulls('value')
        .with_columns(EXPR_METRICS_AGGREGATION_TYPE_WHEN_THEN)
    )


def concat_metrics_frame(
    time_periods: Mapping[int, datetime], weather_data: pl.LazyFrame
) -> pl.LazyFrame:
    return pl.concat(
        tuple(
            weather_data.filter(pl.col('reference_timestamp') >= datetime_period)
            .drop(
                'reference_timestamp',
            )
            .group_by(('station_abbr', 'station_name'))
            .agg(*EXPR_WEATHER_AGGREGATION_TYPES)
            .with_columns(pl.lit(period).alias('time_period').cast(pl.Int8))
            for period, datetime_period in time_periods.items()
        )
    )


def filter_stations_to_series(stations: pl.DataFrame, station_type: str) -> pl.Series:
    """Filter stations according to station type

    Parameters
    ----------
    stations: pl.DataFrame
        Weather stations DataFrame
    station_type
        Station type, one of 'rainfall' or 'weather'

    Returns
    -------
        Polars Series with station names
    """
    return (
        stations.filter(pl.col('station_type_en') == station_type)
        .select('station_abbr')
        .to_series()
        .str.to_lowercase()
    )


def hello(name: str):
    print(f'Hello {name}')


def goodbye(name: str, formal: bool = False):
    if formal:
        print(f'Goodbye Ms. {name}. Have a good day.')
    else:
        print(f'Bye {name}!')


@app.command()
def main(
    metrics: Annotated[
        bool,
        typer.Option(
            '--metrics',
            '-m',
            help='metrics_help',
        ),
    ] = False,
    update: Annotated[
        bool, typer.Option('--update', '-u', help='update values')
    ] = False,
    verbose_warn: Annotated[
        bool,
        typer.Option(
            '--verbose',
            '-v',
            help='Verbose output. Can be multiplied, e.g -vv or -vvv for even more verbosity',
        ),
    ] = False,
    verbose_info: Annotated[
        bool, typer.Option('--verbose_info', '-vv', help='verbose info', hidden=True)
    ] = False,
    verbose_debug: Annotated[
        bool, typer.Option('--verbose_debug', '-vvv', help='verbose debug', hidden=True)
    ] = False,
    data_path: Annotated[Path | None, typer.Argument()] = None,
):
    if verbose_warn:
        logger.setLevel(logging.WARNING)
    if verbose_info:
        logger.setLevel(logging.INFO)
    if verbose_debug:
        logger.setLevel(logging.DEBUG)
    logger.debug('Logger created')
    if metrics:
        logger.info('Metrics generation activated')
    if update:
        logger.info('Update of existing data activated')
    with tempfile.TemporaryDirectory() as tmpdir:
        down_path: Path = Path(tmpdir)
        logger.info(f'Download path: {down_path}')
        new_data = DataPreparation(
            download_path=down_path, update_flag=update, data_path=data_path
        )
        new_data.load_metadata()
        new_data.load_weather_data()
        if metrics:
            new_data.load_metrics()
    logger.info('Files successfully downloaded')


if __name__ == '__main__':
    app()
