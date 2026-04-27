from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd
from dash import Dash, Input, Output, callback, dcc, html

from visualization.globe_model import (
    build_model_status_text,
    load_corrected_station_snapshot,
    train_error_model_bundle,
)
from visualization.globe_data import fetch_linear_model_predictions
from visualization.globe_plotting import (
    DEFAULT_VARIABLE,
    VARIABLE_CONFIG,
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
                            html.Div(
                                [
                                    html.Div("Data Source", className="control-label"),
                                    dcc.RadioItems(
                                        id="data-source",
                                        value="standard",
                                        className="control-radio",
                                        options=[
                                            {"label": "Current Weather", "value": "standard"},
                                            {"label": "R Linear Model", "value": "linear"},
                                        ],
                                    ),
                                ],
                                className="control-group",
                            ),
                            html.Div(
                                id="linear-controls",
                                style={"display": "none"},
                                children=[
                                    html.Div("Forecast Day", className="control-label"),
                                    dcc.Slider(
                                        id="forecast-day-slider",
                                        min=1,
                                        max=7,
                                        step=1,
                                        value=1,
                                        marks={i: str(i) for i in range(1, 8)},
                                        className="forecast-slider",
                                    ),
                                    html.Button(
                                        "Update Predictions",
                                        id="update-predictions-btn",
                                        n_clicks=0,
                                        className="refresh-btn",
                                    ),
                                ],
                                className="control-group",
                            ),
                        ],
                        className="controls-row",
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
            html.Div(
                [
                    html.Div(
                        [
                            html.H3("Cities", className="sidebar-title"),
                            dcc.Input(
                                id="city-search",
                                placeholder="Search cities...",
                                type="text",
                                className="city-search-input",
                            ),
                            html.Div(id="city-list", className="city-list"),
                        ],
                        className="city-sidebar",
                    ),
                    html.Div(
                        dcc.Graph(id="globe-graph", className="globe-graph"),
                        className="globe-container",
                    ),
                ],
                className="main-content",
            ),
            dcc.Store(id="city-data", data=serialize_city_df(city_df)),
            dcc.Store(id="linear-data", data=None),
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
        Output("linear-controls", "style"),
        Input("data-source", "value"),
    )
    def toggle_linear_controls(data_source):
        if data_source == "linear":
            return {"display": "block"}
        return {"display": "none"}

    @callback(
        Output("linear-data", "data"),
        Output("refresh-status", "children"),
        Input("forecast-day-slider", "value"),
        Input("update-predictions-btn", "n_clicks"),
        prevent_initial_call=True,
    )
    def update_linear_predictions(forecast_day, n_clicks):
        from dash import callback_context

        trigger_id = callback_context.triggered[0]["prop_id"].split(".")[0]

        if trigger_id == "update-predictions-btn":
            return (
                dcc.no_update,
                "Note: R predictions must be updated externally. "
                "Run predict_weather.R locally or set up a scheduled task.",
            )

        try:
            predictions_df = fetch_linear_model_predictions(forecast_day)
            return (
                serialize_city_df(predictions_df),
                f"Loaded predictions for day {forecast_day}",
            )
        except FileNotFoundError as e:
            return None, f"No predictions found: {str(e)}. Run predict_weather.R first."

    @callback(
        Output("city-list", "children"),
        Input("city-data", "data"),
        Input("city-search", "value"),
        Input("variable-tabs", "value"),
    )
    def update_city_list(city_data, search_term, variable_key):
        df = pd.DataFrame(city_data)
        config = VARIABLE_CONFIG.get(variable_key, VARIABLE_CONFIG[DEFAULT_VARIABLE])
        value_col = config["column"]
        fmt = config["fmt"]
        unit = config["unit"]

        if search_term:
            search_pattern = search_term.lower()
            df = df[
                df["city"].fillna("").str.lower().str.contains(search_pattern) |
                df["country"].fillna("").str.lower().str.contains(search_pattern)
            ]

        max_display = 50
        display_df = df.head(max_display)

        items = []
        for _, row in display_df.iterrows():
            city = row.get("city", "Unknown")
            country = row.get("country", "")
            value = row.get(value_col)

            if pd.notna(value):
                value_str = f"{value:{fmt}}{unit}"
            else:
                value_str = "N/A"

            items.append(
                html.Div(
                    [
                        html.Span(city, className="city-item-name"),
                        html.Span(country, className="city-item-country"),
                        html.Span(value_str, className="city-item-value"),
                    ],
                    className="city-list-item",
                )
            )

        if not items:
            return html.Div("No cities found", className="city-list-empty")

        if len(df) > max_display:
            items.append(
                html.Div(f"... and {len(df) - max_display} more", className="city-list-more")
            )

        return items

    @callback(
        Output("globe-graph", "figure"),
        Input("city-data", "data"),
        Input("linear-data", "data"),
        Input("variable-tabs", "value"),
        Input("data-source", "value"),
    )
    def update_globe(city_data, linear_data, variable_key, data_source):
        if data_source == "linear" and linear_data:
            df = pd.DataFrame(linear_data)
        else:
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
            show_countries=True,
        )

    return app
