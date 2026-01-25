"""Tests module meteoshrooms.data_preparation.data_preparation.py"""

from pathlib import Path

import polars as pl
import polars.selectors as cs
import pytest
from polars.testing import assert_frame_equal

from meteoshrooms.data_preparation import data_preparation
from meteoshrooms.data_preparation.data_preparation import DataPreparation


@pytest.fixture(autouse=True)
def test_data_path() -> Path:
    data_path: Path = Path(__file__).resolve().parents[0].joinpath('data')
    return data_path


@pytest.fixture(scope='class')
def create_temp_down_path(request, tmp_path_factory):
    # cls = request.node.cls
    # cls.temporary_data_path = tmp_path_factory.mktemp('data')
    # yield
    # del cls.temporary_data_path
    return tmp_path_factory.mktemp('data')


@pytest.fixture(autouse=True)
def meta_file_path_dict(test_data_path):
    """Creates Dictionary with local metadata file paths"""
    meta_file_path_dict: dict[str, list[str]] = {
        'stations': [
            str(
                Path(
                    test_data_path, f'ogd-smn{meta_suffix}_meta_stations_test_data.csv'
                )
            )
            for ogd_smn_prefix, meta_suffix in zip(
                ['', '-precip', '-tower'], ['', '-precip', '-tower'], strict=False
            )
        ],
        'parameters': [
            str(
                Path(
                    test_data_path,
                    f'ogd-smn{meta_suffix}_meta_parameters_test_data.csv',
                )
            )
            for ogd_smn_prefix, meta_suffix in zip(
                ['', '-precip', '-tower'], ['', '-precip', '-tower'], strict=False
            )
        ],
        'datainventory': [
            str(
                Path(
                    test_data_path,
                    f'ogd-smn{meta_suffix}_meta_datainventory_test_data.csv',
                )
            )
            for ogd_smn_prefix, meta_suffix in zip(
                ['', '-precip', '-tower'], ['', '-precip', '-tower'], strict=False
            )
        ],
    }
    return meta_file_path_dict


@pytest.fixture(autouse=True)
def replace_meta_filepath_dict(monkeypatch, meta_file_path_dict):
    """Disable all HTTP requests by removing requests.sessions.Session.request."""
    monkeypatch.setattr(data_preparation, 'META_FILE_PATH_DICT', meta_file_path_dict)


@pytest.fixture
def lf_meta_stations_test_result(test_data_path):
    """Loads stations test result into LazyFrame"""
    return pl.scan_csv(
        str(Path(test_data_path, 'ogd-smn_meta_stations_test_result.csv'))
    )


@pytest.fixture
def lf_meta_parameters_test_result(test_data_path):
    """Loads parameters test result into LazyFrame"""
    return pl.scan_csv(
        str(Path(test_data_path, 'ogd-smn_meta_parameters_test_result.csv'))
    ).cast({cs.integer(): pl.Int8})


@pytest.fixture
def lf_meta_datainventory_test_result(test_data_path):
    """Loads datainventory test result into LazyFrame"""
    return pl.scan_csv(
        str(Path(test_data_path, 'ogd-smn_meta_datainventory_test_result.csv'))
    ).cast({cs.integer(): pl.Int8, cs.starts_with('data_'): pl.Datetime})


# @pytest.fixture(scope='class')
# def attach_lf_meta_stations(request, meta_file_path_dict, temporary_data_path):
#     """Returns meta_stations created by load_metadata()"""
#     cls = request.node.cls
#     cls.lf_meta_stations = cls.load_metadata()
#     yield
#     del cls.lf_meta_stations


# @pytest.fixture(scope='class')
# def attach_testdata_instance(request, temporary_data_path, test_data_path):
#     cls = request.node.cls
#     cls.testdata_instance = DataPreparation(
#         download_path=temporary_data_path, data_path=test_data_path, update_flag=False
#     )
#     yield
#     del cls.testdata_instance


# @pytest.fixture(scope='class')
# def attach_lf_meta_parameters(request, meta_file_path_dict, temporary_data_path):
#     """Returns meta_parameters created by load_metadata()"""
#     cls = request.node.cls
#     cls.lf_meta_parameters = cls.load_metadata()
#     yield
#     del cls.lf_meta_parameters


# @pytest.fixture(scope='class')
# def attach_lf_meta_datainventory(request, meta_file_path_dict, temporary_data_path):
#     """Returns meta_datainventory created by load_metadata()"""
#     cls = request.node.cls
#     cls.lf_meta_datainventory = cls.load_metadata()
#     yield
#     del cls.lf_meta_datainventory


@pytest.mark.usefixtures(
    # 'attach_lf_meta_stations',
    # 'attach_lf_meta_parameters',
    # 'attach_lf_meta_datainventory',
    # 'attach_testdata_instance',
)
class TestLoadMetadata:
    """Tests function load_metadata()"""

    @pytest.fixture(autouse=True)
    def setup(self, test_data_path: Path, replace_meta_filepath_dict):
        self.testdata_instance = self.create_testdata_instance(
            data_path=test_data_path,
            parquet_flag=True,
            postgres_flag=False,
            weather_flag=True,
            metrics_flag=True,
            update_flag=False,
        )
        self.testdata_instance.load_meta_parameters()
        self.testdata_instance.load_meta_datainventory()
        self.testdata_instance.load_meta_stations()

    def create_testdata_instance(
        self,
        data_path: Path,
        parquet_flag: bool,
        postgres_flag: bool,
        weather_flag: bool,
        metrics_flag: bool,
        update_flag: bool,
    ):
        return DataPreparation(
            data_path=data_path,
            parquet_flag=parquet_flag,
            postgres_flag=postgres_flag,
            weather_flag=weather_flag,
            metrics_flag=metrics_flag,
            update_flag=update_flag,
        )

    def test_load_metadata_stations_is_lazyframe(self):
        """Tests whether returned frame is LazyFrame"""
        assert isinstance(self.testdata_instance.meta_stations, pl.LazyFrame)

    def test_load_metadata_stations_assert_equal_lazyframes(
        self, meta_file_path_dict, lf_meta_stations_test_result, monkeypatch
    ):
        """Tests if returned frame is equal to test data"""
        assert_frame_equal(
            self.testdata_instance.meta_stations, lf_meta_stations_test_result
        )

    def test_load_metadata_parameters_is_lazyframe(self):
        """Tests whether returned frame is LazyFrame"""
        assert isinstance(self.testdata_instance.meta_parameters, pl.LazyFrame)

    def test_load_metadata_parameters_assert_equal_lazyframes(
        self, meta_file_path_dict, lf_meta_parameters_test_result
    ):
        """Tests if returned frame is equal to test data"""
        assert_frame_equal(
            self.testdata_instance.meta_parameters, lf_meta_parameters_test_result
        )

    def test_load_metadata_datainventory_is_lazyframe(self):
        """Tests whether returned frame is LazyFrame"""
        assert isinstance(self.testdata_instance.meta_datainventory, pl.LazyFrame)

    def test_load_metadata_datainventory_assert_equal_lazyframes(
        self, meta_file_path_dict, lf_meta_datainventory_test_result
    ):
        """Tests if returned frame is equal to test data"""
        assert_frame_equal(
            self.testdata_instance.meta_datainventory, lf_meta_datainventory_test_result
        )
