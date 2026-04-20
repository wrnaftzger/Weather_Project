from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd
from dash import Dash, Input, Output, callback, dcc, html

from visualization.globe_model import (
    build_model_status_text,
    load_corrected_station_snapshot,
    train_error_model_bundle,
)
from visualization.globe_plotting import (
    DEFAULT_VARIABLE,
    VARIABLE_SWITCH_OPTIONS,
    create_globe_figure,
)


def serialize_city_df(df):
    data = df.copy()
    for col in ("issue_time", "valid_time"):
        if col in data.columns:
            data[col] = pd.to_datetime(data[col]).dt.strftime("%Y-%m-%d %H:%M:%S")
    return data.to_dict("records")


def build_app():
    city_df, model_bundle = load_corrected_station_snapshot()
    app = Dash(__name__)
    app.title = "Weather Globe"

    app.layout = html.Div(
        [
            html.Div(
                [
                    html.Div(
                        [
                            html.H2("Weather Globe", className="globe-title"),
                            html.Div(
                                "Switch variables to explore global forecast conditions. "
                                "Use arrow keys to rotate and tilt the globe.",
                                className="globe-subtitle",
                            ),
                            html.Div(
                                build_model_status_text(model_bundle, len(city_df)),
                                id="model-status",
                                className="model-status",
                            ),
                        ],
                        className="globe-header",
                    ),
                    html.Div(
                        [
                            html.Div("Variable", className="control-label"),
                            dcc.Tabs(
                                id="variable-tabs",
                                value=DEFAULT_VARIABLE,
                                className="variable-tabs",
                                children=[
                                    dcc.Tab(
                                        label=option["label"],
                                        value=option["value"],
                                        className="variable-tab",
                                        selected_className="variable-tab--selected",
                                    )
                                    for option in VARIABLE_SWITCH_OPTIONS
                                ],
                            ),
                        ],
                        className="globe-controls",
                    ),
                    html.Div(
                        [
                            html.Button("Refresh data", id="refresh-btn", n_clicks=0, className="refresh-btn"),
                            html.Div(
                                id="refresh-status",
                                className="refresh-status",
                            ),
                        ],
                        className="refresh-panel",
                    ),
                ],
                className="toolbar-panel",
            ),
            dcc.Store(id="city-data", data=serialize_city_df(city_df)),
            dcc.Graph(id="globe-graph", className="globe-graph"),
        ],
        className="app-shell",
    )

    @callback(
        Output("city-data", "data"),
        Output("refresh-status", "children"),
        Output("model-status", "children"),
        Input("refresh-btn", "n_clicks"),
        prevent_initial_call=True,
    )
    def refresh_city_data(_):
        train_error_model_bundle.cache_clear()
        latest_df, latest_model_bundle = load_corrected_station_snapshot()
        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        refresh_text = f"Last refreshed: {timestamp}"
        model_text = build_model_status_text(latest_model_bundle, len(latest_df))
        return serialize_city_df(latest_df), refresh_text, model_text

    @callback(
        Output("globe-graph", "figure"),
        Input("city-data", "data"),
        Input("variable-tabs", "value"),
    )
    def update_globe(city_data, variable_key):
        df = pd.DataFrame(city_data)
        for col in (
            "lat",
            "lng",
            "forecast_temp",
            "predicted_error",
            "corrected_temp",
            "apparent_temperature",
            "relative_humidity_2m",
            "precipitation",
            "wind_speed_10m",
            "wind_gusts_10m",
        ):
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        for col in ("issue_time", "valid_time"):
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce")
        required_cols = ["lat", "lng"]
        if "valid_time" in df.columns:
            required_cols.append("valid_time")
        df = df.dropna(subset=required_cols)
        return create_globe_figure(
            city_df=df,
            variable_key=variable_key,
        )

    return app
