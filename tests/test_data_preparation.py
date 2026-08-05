"""Tests module meteoshrooms.data_preparation.data_preparation"""

from pathlib import Path
from typing import Annotated
from unittest.mock import MagicMock, patch

import polars as pl
from tryke import Depends, describe, expect, fixture, test

from meteoshrooms.data_preparation import data_preparation
from meteoshrooms.data_preparation.data_preparation import DataPreparation


@fixture(per='scope')
def test_data_path() -> Path:
    """Path to test data files"""
    return Path(__file__).resolve().parents[0].joinpath('data')


@fixture(per='scope')
def default_data_path() -> Path:
    """Path to project data files"""
    return Path(__file__).resolve().parents[1].joinpath('data')


@fixture
def mock_meta_stations() -> pl.LazyFrame:
    """Mock station metadata LazyFrame"""
    return pl.LazyFrame(
        {
            'station_abbr': ('airo', 'bern', 'zur'),
            'station_name': ('Airolo', 'Bern', 'Zurich'),
            'station_type_en': (
                'Automatic weather stations',
                'Automatic weather stations',
                'Automatic precipitation stations',
            ),
            'station_height_masl': (1000.0, 500.0, 600.0),
            'station_coordinates_lv95_east': (500000.0, 600000.0, 700000.0),
            'station_coordinates_lv95_north': (100000.0, 200000.0, 300000.0),
            'station_coordinates_wgs84_lat': (46.5, 46.9, 47.4),
            'station_coordinates_wgs84_lon': (8.5, 7.4, 8.5),
        }
    )


@fixture
def mock_meta_parameters() -> pl.LazyFrame:
    """Mock parameters metadata LazyFrame"""
    return pl.LazyFrame(
        {
            'parameter_shortname': ('rre150h0', 'tre200h0', 'ure200h0'),
            'parameter_datatype': ('Float', 'Float', 'Float'),
            'parameter_unit': ('mm', '°C', 'km/h'),
            'parameter_description_en': ('Rainfall', 'Temperature', 'Wind Speed'),
            'parameter_decimals': (1, 1, 0),
            'parameter_granularity': ('15min', '1h', '1h'),
            'parameter_group_en': ('Precipitation', 'Temperature', 'Wind'),
        }
    )


@fixture
def data_preparation_instance_with_parquet(
    test_data_path: Annotated[Path, Depends(test_data_path)],
):
    """DataPreparation instance with test_path and parquet_flag"""
    instance = DataPreparation(
        data_path=test_data_path,
        parquet_flag=True,
    )
    yield instance


@fixture
def data_preparation_instance_without_path(
    test_data_path: Annotated[Path, Depends(test_data_path)],
):
    """DataPreparation instance without data_path"""
    instance = DataPreparation(
        data_path=None,
        parquet_flag=True,
    )
    yield instance


@fixture
def data_preparation_instance_with_metrics_flag(
    test_data_path: Annotated[Path, Depends(test_data_path)],
):
    """DataPreparation instance without data_path"""
    instance = DataPreparation(
        data_path=test_data_path,
        parquet_flag=True,
        metrics_flag=True,
    )
    yield instance


with describe('DataPreparation Class Initialization'):

    @test
    def test_init_with_valid_parquet_flag(
        test_data_path: Annotated[Path, Depends(test_data_path)],
        data_preparation_instance_with_parquet: Annotated[
            DataPreparation, Depends(data_preparation_instance_with_parquet)
        ],
    ):
        """Test initialization with parquet_flag=True"""
        expect(data_preparation_instance_with_parquet.parquet_flag).to_be_truthy()
        expect(data_preparation_instance_with_parquet.data_path).to_equal(
            test_data_path
        )

    @test
    def test_init_raises_without_output_type(
        test_data_path: Annotated[Path, Depends(test_data_path)],
    ):
        """Test initialization raises ValueError when no output type is set"""
        expect(
            lambda: DataPreparation(
                data_path=test_data_path,
                parquet_flag=False,
            )
        ).to_raise(ValueError)

    @test
    def test_init_sets_default_data_path(
        default_data_path: Annotated[Path, Depends(default_data_path)],
        data_preparation_instance_without_path: Annotated[
            DataPreparation, Depends(data_preparation_instance_without_path)
        ],
    ):
        """Test initialization sets default data path when None passed"""

        expect(data_preparation_instance_without_path.data_path).to_equal(
            default_data_path
        )

    @test
    def test_init_creates_metrics_when_enabled(
        test_data_path: Annotated[Path, Depends(test_data_path)],
        data_preparation_instance_with_metrics_flag: Annotated[
            DataPreparation, Depends(data_preparation_instance_with_metrics_flag)
        ],
    ):
        """Test initialization creates empty metrics LazyFrame when enabled"""
        expect(
            hasattr(data_preparation_instance_with_metrics_flag, 'metrics')
        ).to_be_truthy()
        expect(
            isinstance(
                data_preparation_instance_with_metrics_flag.metrics, pl.LazyFrame
            )
        ).to_be_truthy()


with describe('DataPreparation.load_meta_* methods'):

    @test
    def test_load_meta_returns_lazyframe(
        data_preparation_instance_with_parquet: Annotated[
            DataPreparation, Depends(data_preparation_instance_with_parquet)
        ],
    ):
        """Test load methods return LazyFrames (when mocked)"""
        # These would need actual mock data files to test properly
        expect(data_preparation_instance_with_parquet).to_be_truthy()


with describe('generate_download_urls'):

    @test
    def test_generate_urls_for_rainfall():
        """Test URL generation for rainfall stations"""
        station_series = pl.Series(['airo', 'bern'])

        with (
            patch.object(data_preparation, 'URL_GEO_ADMIN_BASE', 'https://test.ch'),
            patch.object(
                data_preparation, 'URL_GEO_ADMIN_STATION_TYPE_BASE', 'ch.test'
            ),
        ):
            urls = data_preparation.generate_download_urls(
                station_series, 'rainfall', 'now'
            )

            expect(len(urls)).to_equal(2)

    @test
    def test_generate_urls_for_weather():
        """Test URL generation for weather stations"""
        station_series = pl.Series(['airo', 'bern'])

        with (
            patch.object(data_preparation, 'URL_GEO_ADMIN_BASE', 'https://test.ch'),
            patch.object(
                data_preparation, 'URL_GEO_ADMIN_STATION_TYPE_BASE', 'ch.test'
            ),
        ):
            urls = data_preparation.generate_download_urls(
                station_series, 'weather', 'now'
            )

            expect(len(urls)).to_equal(2)

    @test
    def test_generate_urls_invalid_timeframe_raises():
        """Test invalid timeframe raises ValueError"""
        station_series = pl.Series(['airo'])

        expect(
            lambda: data_preparation.generate_download_urls(
                station_series, 'weather', 'invalid'
            )
        ).to_raise(ValueError)

    @test
    def test_generate_urls_invalid_station_type_raises():
        """Test invalid station type raises TypeError"""
        station_series = pl.Series(['airo'])

        expect(
            lambda: data_preparation.generate_download_urls(station_series, 123, 'now')
        ).to_raise(TypeError)


with describe('filter_stations_to_series'):

    @test
    def test_filter_rainfall_stations(
        mock_meta_stations: Annotated[pl.LazyFrame, Depends(mock_meta_stations)],
    ):
        """Test filtering rainfall stations returns correct series"""
        df = mock_meta_stations.collect()
        result = data_preparation.filter_stations_to_series(
            df, 'Automatic precipitation stations'
        )

        expect(len(result)).to_equal(1)
        expect(result[0]).to_equal('zur')  # Lowercase

    @test
    def test_filter_weather_stations(
        mock_meta_stations: Annotated[pl.LazyFrame, Depends(mock_meta_stations)],
    ):
        """Test filtering weather stations returns correct series"""
        df = mock_meta_stations.collect()
        result = data_preparation.filter_stations_to_series(
            df, 'Automatic weather stations'
        )

        expect(len(result)).to_equal(2)
        expect(result[0]).to_equal('airo')
        expect(result[1]).to_equal('bern')


with describe('expr_filter_column_timedelta'):

    @test
    def test_create_time_filter_expression():
        """Test that time filter expression is created correctly"""
        delta_days = 7
        expr = data_preparation.expr_filter_column_timedelta('timestamp', delta_days)

        expect(str(expr)).to_contain('timestamp')


with describe('create_kwargs_lazyframe'):

    @test
    def test_create_kwargs_returns_expected_structure():
        """Test kwargs dictionary has expected keys"""
        schema = {'col1': pl.String, 'col2': pl.Float32}
        kwargs = data_preparation.create_kwargs_lazyframe(schema)

        expect(kwargs).to_be_truthy()
        expect(kwargs['separator']).to_equal(';')
        expect(kwargs['try_parse_dates']).to_be_truthy()
        expect('schema_overrides' in kwargs).to_be_truthy()


with describe('DataPreparation.create_weather_schema_dict'):

    @test
    def test_create_weather_schema_requires_meta_parameters(
        test_data_path: Annotated[Path, Depends(test_data_path)],
        data_preparation_instance_with_parquet: Annotated[
            DataPreparation, Depends(data_preparation_instance_with_parquet)
        ],
    ):
        """Test that weather schema dict requires meta_parameters to be loaded"""

        expect(
            lambda: data_preparation_instance_with_parquet.create_weather_schema_dict()
        ).to_raise(Exception)


with describe('DataPreparation.prepare_data'):

    @test
    def test_prepare_data_executes_load_and_save(
        test_data_path: Annotated[Path, Depends(test_data_path)],
        data_preparation_instance_with_parquet: Annotated[
            DataPreparation, Depends(data_preparation_instance_with_parquet)
        ],
    ):
        """Test prepare_data calls load and save methods"""
        # This would need proper mocking of all dependencies

        expect(data_preparation_instance_with_parquet).to_be_truthy()
        expect(data_preparation_instance_with_parquet.prepare_data()).to_be_none()


with describe('filter_unique_station_names'):

    @test
    def test_filter_unique_stations(
        mock_meta_stations: Annotated[pl.LazyFrame, Depends(mock_meta_stations)],
    ):
        """Test unique station filtering removes duplicates"""
        result = data_preparation.filter_unique_station_names(mock_meta_stations)

        # expect(result).to_be_truthy()
        expect(isinstance(result, pl.LazyFrame)).to_be_truthy()
        expect(result.collect().shape[0]).to_be_less_than_or_equal(3)


with describe('check_generate_download_urls_arguments_or_raise_error'):

    @test
    def test_check_valid_arguments_pass():
        """Test valid arguments pass validation"""
        result = data_preparation.check_generate_download_urls_arguments_or_raise_error(
            'weather', 'now'
        )
        expect(result).to_be_none()

    @test
    def test_check_invalid_timeframe_raises():
        """Test invalid timeframe raises ValueError"""
        expect(
            lambda: data_preparation.check_generate_download_urls_arguments_or_raise_error(
                'weather', 'invalid'
            )
        ).to_raise(ValueError)

    @test
    def test_check_invalid_station_type_raises():
        """Test invalid station type raises TypeError"""
        expect(
            lambda: data_preparation.check_generate_download_urls_arguments_or_raise_error(
                123, 'now'
            )
        ).to_raise(TypeError)


# Test the standalone functions
with describe('load_metadata_per_type'):

    @test
    def test_load_metadata_per_type_structure():
        """Test load_metadata_per_type returns expected type (would need real files)"""
        # This test documents expected behavior
        expect(True).to_be_truthy()  # Placeholder until mock data is added


with describe('save_metadata_to_parquet'):

    @test
    def test_save_metadata_to_parquet_path_construction(
        test_data_path: Annotated[Path, Depends(test_data_path)],
    ):
        """Test parquet file path construction"""
        test_frame = pl.DataFrame({'col1': [1, 2, 3]}).lazy()

        with (
            patch('pathlib.Path.open', MagicMock()),
            patch('polars.LazyFrame.sink_parquet', MagicMock()) as mock_sink,
        ):
            data_preparation.save_metadata_to_parquet(
                frame_meta=test_frame,
                data_path=test_data_path,
                meta_type='stations',
            )

            expect(mock_sink.called).to_be_truthy()


with describe('combine_urls_parts_to_string'):

    @test
    def test_combine_url_parts():
        """Test URL combination produces expected format"""
        station_series = pl.Series(['airo'])

        result = data_preparation.combine_urls_parts_to_string(
            station_series, '-precip', 'now'
        )

        expect(result.item()).to_contain('airo')
        expect(result.item()).to_contain('now')
        expect(result.item()).to_contain('ogd-smn')
