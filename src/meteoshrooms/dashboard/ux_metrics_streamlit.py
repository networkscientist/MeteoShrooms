from typing import Sequence

import polars as pl
import streamlit as st
from streamlit.delta_generator import DeltaGenerator

from meteoshrooms.dashboard.constants import NUM_DAYS_VAL, WEATHER_SHORT_LABEL_DICT
from meteoshrooms.dashboard.dashboard_utils_streamlit import (
    META_PARAMETERS,
    WEATHER_COLUMN_NAMES_DICT,
)
from meteoshrooms.dashboard.ux_metrics import (
    calculate_metric_delta,
    calculate_metric_value,
    create_metric_kwargs,
    get_metric_emoji,
)


def create_metric_section(
    metrics: pl.LazyFrame, station_name: str, metrics_list: Sequence[str]
):
    st.subheader(station_name)

    cols_metric: list[DeltaGenerator] = st.columns(len(metrics_list))
    for col, metric_name in zip(
        cols_metric,
        metrics_list,
        strict=False,
    ):
        val: float | None = calculate_metric_value(
            metrics, metric_name, station_name, number_days=NUM_DAYS_VAL
        )

        metric_label: str = WEATHER_SHORT_LABEL_DICT[metric_name]
        if val is not None:
            delta: str | None = calculate_metric_delta(
                metric_name, metrics, station_name, val
            )
            col.metric(
                label=metric_label,
                value=convert_metric_value_to_string_for_metric_section(
                    metric_name, val
                ),
                delta=delta,
                **create_metric_kwargs(
                    metric_name, META_PARAMETERS, WEATHER_COLUMN_NAMES_DICT
                ),
            )
        else:
            col.metric(
                label=metric_label,
                value='-',
                **create_metric_kwargs(
                    metric_name, META_PARAMETERS, WEATHER_COLUMN_NAMES_DICT
                ),
            )


def convert_metric_value_to_string_for_metric_section(
    metric_name: str, val: float
) -> str:
    return ' '.join(
        (
            str(round(val, 1)),
            (get_metric_emoji(val) if metric_name == 'rre150h0' else ''),
        )
    )
