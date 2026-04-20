from __future__ import annotations

from functools import lru_cache

import numpy as np


@lru_cache(maxsize=16)
def build_grid(resolution):
    lon_count = int(resolution)
    lat_count = int(resolution // 2) + 1
    grid_lat = np.linspace(-90.0, 90.0, lat_count)
    grid_lon = np.linspace(-180.0, 180.0, lon_count)
    lon_mesh, lat_mesh = np.meshgrid(grid_lon, grid_lat)
    return lat_mesh, lon_mesh


def build_trend_features(lat_deg, lon_deg):
    lat = np.radians(np.asarray(lat_deg, dtype=float))
    lon = np.radians(np.asarray(lon_deg, dtype=float))
    return np.column_stack(
        [
            np.ones_like(lat),
            np.sin(lat),
            np.cos(lat),
            np.sin(lon),
            np.cos(lon),
            np.sin(lat) * np.cos(lon),
            np.sin(lat) * np.sin(lon),
        ]
    )


def fit_spatial_trend(station_lats, station_lons, station_temps, grid_lats, grid_lons):
    x_station = build_trend_features(station_lats, station_lons)
    y_station = np.asarray(station_temps, dtype=float)
    beta, _, _, _ = np.linalg.lstsq(x_station, y_station, rcond=None)
    trend_station = x_station @ beta

    x_grid = build_trend_features(grid_lats.ravel(), grid_lons.ravel())
    trend_grid = (x_grid @ beta).reshape(grid_lats.shape)
    return trend_station, trend_grid


def pairwise_great_circle(lat_a_rad, lon_a_rad, lat_b_rad, lon_b_rad):
    cos_d = (
        np.sin(lat_a_rad)[:, None] * np.sin(lat_b_rad)[None, :]
        + np.cos(lat_a_rad)[:, None]
        * np.cos(lat_b_rad)[None, :]
        * np.cos(lon_a_rad[:, None] - lon_b_rad[None, :])
    )
    return np.arccos(np.clip(cos_d, -1.0, 1.0))


def estimate_kriging_range_rad(station_lats, station_lons):
    n_points = len(station_lats)
    if n_points < 2:
        return np.radians(20.0)

    sample_size = min(n_points, 400)
    sample_idx = np.linspace(0, n_points - 1, sample_size, dtype=int)
    lat_rad = np.radians(np.asarray(station_lats, dtype=float)[sample_idx])
    lon_rad = np.radians(np.asarray(station_lons, dtype=float)[sample_idx])

    distances = pairwise_great_circle(lat_rad, lon_rad, lat_rad, lon_rad)
    np.fill_diagonal(distances, np.inf)
    nearest = np.min(distances, axis=1)
    nearest = nearest[np.isfinite(nearest)]

    if nearest.size == 0:
        return np.radians(20.0)

    median_nn = float(np.median(nearest))
    if not np.isfinite(median_nn) or median_nn <= 0:
        return np.radians(20.0)

    return float(np.clip(median_nn * 4.0, np.radians(5.0), np.radians(45.0)))


def ordinary_krige_residuals(
    station_lats,
    station_lons,
    residuals,
    grid_lats,
    grid_lons,
    neighbors,
):
    residuals = np.asarray(residuals, dtype=float)
    n_points = residuals.size

    if n_points == 0:
        return np.zeros_like(grid_lats, dtype=float)
    if n_points == 1:
        return np.full_like(grid_lats, residuals[0], dtype=float)

    station_lat_rad = np.radians(np.asarray(station_lats, dtype=float))
    station_lon_rad = np.radians(np.asarray(station_lons, dtype=float))

    range_rad = estimate_kriging_range_rad(station_lats, station_lons)
    range_rad = max(range_rad, 1e-6)

    sill = float(np.var(residuals))
    if not np.isfinite(sill) or sill < 1e-4:
        sill = 1e-4
    nugget = 0.05 * sill

    k = min(max(int(neighbors), 4), n_points)

    flat_lat = np.radians(grid_lats.ravel())
    flat_lon = np.radians(grid_lons.ravel())
    output = np.empty(flat_lat.shape[0], dtype=float)

    for idx_grid, (lat0, lon0) in enumerate(zip(flat_lat, flat_lon)):
        cos_d = (
            np.sin(lat0) * np.sin(station_lat_rad)
            + np.cos(lat0) * np.cos(station_lat_rad) * np.cos(lon0 - station_lon_rad)
        )
        d_target = np.arccos(np.clip(cos_d, -1.0, 1.0))

        if k < n_points:
            nn_idx = np.argpartition(d_target, k - 1)[:k]
        else:
            nn_idx = np.arange(n_points)

        lat_k = station_lat_rad[nn_idx]
        lon_k = station_lon_rad[nn_idx]
        residual_k = residuals[nn_idx]
        d_target_k = d_target[nn_idx]

        d_pp = pairwise_great_circle(lat_k, lon_k, lat_k, lon_k)
        cov_pp = sill * np.exp(-d_pp / range_rad)
        cov_pp.flat[:: cov_pp.shape[0] + 1] += nugget

        cov_target = sill * np.exp(-d_target_k / range_rad)
        m = cov_pp.shape[0]

        system = np.zeros((m + 1, m + 1), dtype=float)
        system[:m, :m] = cov_pp
        system[:m, m] = 1.0
        system[m, :m] = 1.0

        rhs = np.empty(m + 1, dtype=float)
        rhs[:m] = cov_target
        rhs[m] = 1.0

        try:
            weights = np.linalg.solve(system, rhs)[:m]
        except np.linalg.LinAlgError:
            weights = np.linalg.lstsq(system, rhs, rcond=None)[0][:m]

        output[idx_grid] = float(np.dot(weights, residual_k))

    return output.reshape(grid_lats.shape)


def create_regression_kriging_surface(station_df, neighbors, resolution, value_col="corrected_temp"):
    lat_mesh, lon_mesh = build_grid(resolution)

    station_lats = station_df["lat"].to_numpy(dtype=float)
    station_lons = station_df["lng"].to_numpy(dtype=float)
    station_temps = station_df[value_col].to_numpy(dtype=float)

    trend_station, trend_grid = fit_spatial_trend(
        station_lats=station_lats,
        station_lons=station_lons,
        station_temps=station_temps,
        grid_lats=lat_mesh,
        grid_lons=lon_mesh,
    )
    residuals = station_temps - trend_station

    residual_surface = ordinary_krige_residuals(
        station_lats=station_lats,
        station_lons=station_lons,
        residuals=residuals,
        grid_lats=lat_mesh,
        grid_lons=lon_mesh,
        neighbors=neighbors,
    )

    return lat_mesh, lon_mesh, trend_grid + residual_surface
