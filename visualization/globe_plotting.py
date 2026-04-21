from __future__ import annotations

import numpy as np
import pandas as pd
import plotly.graph_objects as go

from visualization.globe_kriging import create_regression_kriging_surface


DEFAULT_NEIGHBORS = 12
DEFAULT_RESOLUTION = 120
DEFAULT_VARIABLE = "corrected_temp"

TEMPERATURE_COLORSCALE = [
    [0.00, "#3B0F70"],
    [0.10, "#5B2A86"],
    [0.20, "#4355B9"],
    [0.30, "#2F74D0"],
    [0.40, "#31A7D8"],
    [0.50, "#6DDBD6"],
    [0.60, "#C8F7C5"],
    [0.70, "#FFE082"],
    [0.80, "#FFB347"],
    [0.90, "#F46D43"],
    [1.00, "#B3003C"],
]

VARIABLE_CONFIG = {
    "corrected_temp": {
        "column": "corrected_temp",
        "label": "Temperature",
        "unit": "C",
        "fmt": ".1f",
        "colorscale": TEMPERATURE_COLORSCALE,
        "padding": 6.0,
        "min_span": 20.0,
        "min_value": -35.0,
        "max_value": 45.0,
    },
    "wind_speed_10m": {
        "column": "wind_speed_10m",
        "label": "Wind Speed",
        "unit": "km/h",
        "fmt": ".1f",
        "colorscale": "Turbo",
        "padding": 2.0,
        "min_span": 12.0,
        "min_value": 0.0,
    },
    "wind_gusts_10m": {
        "column": "wind_gusts_10m",
        "label": "Wind Gusts",
        "unit": "km/h",
        "fmt": ".1f",
        "colorscale": "Plasma",
        "padding": 2.0,
        "min_span": 12.0,
        "min_value": 0.0,
    },
    "relative_humidity_2m": {
        "column": "relative_humidity_2m",
        "label": "Humidity",
        "unit": "%",
        "fmt": ".0f",
        "colorscale": "YlGnBu",
        "padding": 2.0,
        "min_span": 20.0,
        "min_value": 0.0,
        "max_value": 100.0,
    },
    "precipitation": {
        "column": "precipitation",
        "label": "Precipitation",
        "unit": "mm",
        "fmt": ".1f",
        "colorscale": "PuBu",
        "padding": 1.0,
        "min_span": 4.0,
        "min_value": 0.0,
    },
}

VARIABLE_SWITCH_OPTIONS = [
    {"label": "Temperature", "value": "corrected_temp"},
    {"label": "Wind Speed", "value": "wind_speed_10m"},
    {"label": "Wind Gusts", "value": "wind_gusts_10m"},
    {"label": "Humidity", "value": "relative_humidity_2m"},
    {"label": "Precipitation", "value": "precipitation"},
]


def lat_lon_to_xyz(lat_deg, lon_deg, radius=1.0):
    lat = np.radians(lat_deg)
    lon = np.radians(lon_deg)
    x = radius * np.cos(lat) * np.cos(lon)
    y = radius * np.cos(lat) * np.sin(lon)
    z = radius * np.sin(lat)
    return x, y, z


def _unit_suffix(unit: str) -> str:
    if not unit:
        return ""
    if unit == "%":
        return unit
    return f" {unit}"


def _compute_color_limits(surface_values, station_values, config):
    combined = np.concatenate([surface_values.ravel(), station_values])
    combined = combined[np.isfinite(combined)]
    if combined.size == 0:
        return 0.0, 1.0

    q_low, q_high = np.percentile(combined, [2, 98])
    data_min = float(np.min(combined))
    data_max = float(np.max(combined))
    padding = float(config.get("padding", 1.0))

    cmin = min(data_min, float(q_low)) - padding
    cmax = max(data_max, float(q_high)) + padding

    min_value = config.get("min_value")
    if min_value is not None:
        cmin = max(cmin, float(min_value))

    max_value = config.get("max_value")
    if max_value is not None:
        cmax = min(cmax, float(max_value))

    min_span = float(config.get("min_span", 1.0))
    if cmax - cmin < min_span:
        midpoint = float(np.mean(combined))
        half = min_span / 2.0
        cmin = midpoint - half
        cmax = midpoint + half
        if min_value is not None:
            cmin = max(cmin, float(min_value))
            cmax = max(cmax, cmin + min_span)
        if max_value is not None:
            cmax = min(cmax, float(max_value))
            cmin = min(cmin, cmax - min_span)

    if cmax <= cmin:
        cmax = cmin + max(min_span, 1.0)

    return float(cmin), float(cmax)


def _marker_sizes(values, variable_key):
    sizes = np.full(values.shape, 4.2, dtype=float)
    if variable_key not in {"wind_speed_10m", "wind_gusts_10m", "precipitation"}:
        return sizes

    finite = np.isfinite(values)
    if not bool(finite.any()):
        return sizes

    scale = float(np.percentile(values[finite], 95))
    if not np.isfinite(scale) or scale <= 0:
        return sizes

    sizes[finite] = 3.8 + 5.8 * np.clip(values[finite] / scale, 0.0, 1.0)
    return sizes


def create_globe_figure(
    city_df,
    variable_key=DEFAULT_VARIABLE,
    neighbors=DEFAULT_NEIGHBORS,
    resolution=DEFAULT_RESOLUTION,
):
    key = variable_key if variable_key in VARIABLE_CONFIG else DEFAULT_VARIABLE
    config = VARIABLE_CONFIG[key]
    value_col = config["column"]
    if value_col not in city_df.columns:
        key = DEFAULT_VARIABLE
        config = VARIABLE_CONFIG[key]
        value_col = config["column"]

    plot_df = city_df.copy()
    for col in ("lat", "lng", value_col):
        plot_df[col] = pd.to_numeric(plot_df[col], errors="coerce")
    plot_df = plot_df.dropna(subset=["lat", "lng", value_col]).reset_index(drop=True)

    if plot_df.empty:
        fig = go.Figure()
        fig.update_layout(
            title=f"Global {config['label']} Globe (data unavailable)",
            margin=dict(l=0, r=0, t=50, b=0),
            paper_bgcolor="#020617",
            plot_bgcolor="#020617",
            font=dict(color="#e2e8f0"),
            showlegend=False,
            xaxis=dict(visible=False),
            yaxis=dict(visible=False),
            annotations=[
                dict(
                    text="Weather data is temporarily unavailable. Please try refresh shortly.",
                    x=0.5,
                    y=0.5,
                    xref="paper",
                    yref="paper",
                    showarrow=False,
                    font=dict(size=16, color="#e2e8f0"),
                )
            ],
        )
        return fig

    lat_mesh, lon_mesh, surface_values = create_regression_kriging_surface(
        station_df=plot_df,
        neighbors=int(neighbors),
        resolution=int(resolution),
        value_col=value_col,
    )

    x_sphere, y_sphere, z_sphere = lat_lon_to_xyz(lat_mesh, lon_mesh, radius=1.0)
    x_atmos, y_atmos, z_atmos = lat_lon_to_xyz(lat_mesh, lon_mesh, radius=1.03)
    x_city, y_city, z_city = lat_lon_to_xyz(
        plot_df["lat"].to_numpy(),
        plot_df["lng"].to_numpy(),
        radius=1.02,
    )
    station_values = plot_df[value_col].to_numpy(dtype=float)
    cmin, cmax = _compute_color_limits(surface_values, station_values, config)
    unit = _unit_suffix(config["unit"])
    marker_sizes = _marker_sizes(station_values, key)
    fmt = config["fmt"]

    fig = go.Figure()
    fig.add_trace(
        go.Surface(
            x=x_sphere,
            y=y_sphere,
            z=z_sphere,
            surfacecolor=surface_values,
            colorscale=config["colorscale"],
            cmin=cmin,
            cmax=cmax,
            colorbar=dict(
                title=f"{config['label']} ({config['unit']})",
                x=0.92,
                thickness=14,
            ),
            customdata=np.dstack([lat_mesh, lon_mesh]),
            hovertemplate=(
                "Lat: %{customdata[0]:.1f} deg<br>"
                "Lon: %{customdata[1]:.1f} deg<br>"
                f"Kriged {config['label'].lower()}: %{{surfacecolor:{fmt}}}{unit}<extra></extra>"
            ),
            showscale=True,
            opacity=1.0,
            lighting=dict(ambient=0.68, diffuse=0.62, specular=0.25, roughness=0.75),
        )
    )
    fig.add_trace(
        go.Surface(
            x=x_atmos,
            y=y_atmos,
            z=z_atmos,
            surfacecolor=np.ones_like(surface_values, dtype=float),
            colorscale=[[0.0, "rgba(125, 211, 252, 0.18)"], [1.0, "rgba(125, 211, 252, 0.18)"]],
            showscale=False,
            hoverinfo="skip",
            opacity=0.25,
            lighting=dict(ambient=0.95, diffuse=0.25, specular=0.05, roughness=1.0),
        )
    )
    fig.add_trace(
        go.Scatter3d(
            x=x_city,
            y=y_city,
            z=z_city,
            mode="markers",
            text=plot_df["city"],
            customdata=np.stack(
                [
                    plot_df["country"].fillna("N/A").astype(str),
                    station_values,
                ],
                axis=1,
            ),
            marker=dict(
                size=marker_sizes,
                color=station_values,
                colorscale=config["colorscale"],
                cmin=cmin,
                cmax=cmax,
                line=dict(width=0.6, color="#020617"),
                opacity=0.96,
            ),
            hovertemplate=(
                "<b>%{text}</b><br>"
                "Country: %{customdata[0]}<br>"
                f"{config['label']}: %{{customdata[1]:{fmt}}}{unit}<extra></extra>"
            ),
            name="Cities",
        )
    )

    latest_valid_time = pd.to_datetime(plot_df["valid_time"].max(), errors="coerce")
    if pd.isna(latest_valid_time):
        sample_time = "latest update"
    else:
        sample_time = latest_valid_time.strftime("%Y-%m-%d %H:%M UTC")

    fig.update_layout(
        title=f"Global {config['label']} Globe ({sample_time})",
        margin=dict(l=0, r=0, t=50, b=0),
        scene=dict(
            xaxis=dict(visible=False, showbackground=False),
            yaxis=dict(visible=False, showbackground=False),
            zaxis=dict(visible=False, showbackground=False),
            aspectmode="data",
            bgcolor="#020617",
            camera=dict(eye=dict(x=1.55, y=1.55, z=0.85)),
            dragmode="turntable",
        ),
        paper_bgcolor="#020617",
        plot_bgcolor="#020617",
        font=dict(color="#e2e8f0"),
        showlegend=False,
    )
    return fig
