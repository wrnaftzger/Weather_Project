from __future__ import annotations

from functools import lru_cache

import numpy as np
import pandas as pd

from visualization.globe_data import (
    fetch_historical_error_training_data,
    fetch_latest_station_forecasts,
)

try:
    from xgboost import XGBRegressor
except ImportError:  # pragma: no cover - optional dependency for serverless footprint
    XGBRegressor = None

MODEL_LOOKBACK_DAYS = 120
MODEL_SAMPLE_LIMIT = 120000
MIN_MODEL_ROWS = 1000


class _NumpyLinearRegressor:
    """Lightweight ridge-regression fallback when xgboost is unavailable."""

    def __init__(self, l2: float = 1e-3):
        self.l2 = float(l2)
        self.intercept_ = 0.0
        self.coef_ = np.array([], dtype=float)

    def fit(self, x: np.ndarray, y: np.ndarray):
        x = np.asarray(x, dtype=float)
        y = np.asarray(y, dtype=float)

        design = np.column_stack([np.ones(x.shape[0], dtype=float), x])
        reg = np.eye(design.shape[1], dtype=float) * self.l2
        reg[0, 0] = 0.0  # do not regularize intercept

        lhs = design.T @ design + reg
        rhs = design.T @ y
        try:
            beta = np.linalg.solve(lhs, rhs)
        except np.linalg.LinAlgError:
            beta = np.linalg.lstsq(lhs, rhs, rcond=None)[0]

        self.intercept_ = float(beta[0])
        self.coef_ = np.asarray(beta[1:], dtype=float)
        return self

    def predict(self, x: np.ndarray) -> np.ndarray:
        x = np.asarray(x, dtype=float)
        return self.intercept_ + x @ self.coef_


def build_error_features(df: pd.DataFrame) -> pd.DataFrame:
    valid_time = pd.to_datetime(df["valid_time"], errors="coerce", utc=True)
    if "issue_time" in df.columns:
        issue_time = pd.to_datetime(df["issue_time"], errors="coerce", utc=True)
    else:
        issue_time = pd.Series(pd.NaT, index=df.index)

    lead_source = df["lead_time_hours"] if "lead_time_hours" in df.columns else pd.Series(np.nan, index=df.index)
    lead_time = pd.to_numeric(lead_source, errors="coerce")
    fallback_lead = (valid_time - issue_time) / pd.Timedelta(hours=1)
    lead_time = lead_time.fillna(fallback_lead).fillna(0.0)

    hour = valid_time.dt.hour.fillna(0).astype(float)
    day_of_year = valid_time.dt.dayofyear.fillna(1).astype(float)

    return pd.DataFrame(
        {
            "forecast_temp": pd.to_numeric(df["forecast_temp"], errors="coerce"),
            "lat": pd.to_numeric(df["lat"], errors="coerce"),
            "lng": pd.to_numeric(df["lng"], errors="coerce"),
            "lead_time_hours": lead_time,
            "hour_sin": np.sin(2.0 * np.pi * hour / 24.0),
            "hour_cos": np.cos(2.0 * np.pi * hour / 24.0),
            "doy_sin": np.sin(2.0 * np.pi * day_of_year / 365.25),
            "doy_cos": np.cos(2.0 * np.pi * day_of_year / 365.25),
        },
        index=df.index,
    )


@lru_cache(maxsize=1)
def train_error_model_bundle(
    lookback_days: int = MODEL_LOOKBACK_DAYS,
    sample_limit: int = MODEL_SAMPLE_LIMIT,
):
    train_df = fetch_historical_error_training_data(lookback_days, sample_limit)
    train_df["error"] = train_df["actual_temp"] - train_df["forecast_temp"]

    feature_df = build_error_features(train_df)
    target = pd.to_numeric(train_df["error"], errors="coerce")
    valid_mask = feature_df.notna().all(axis=1) & target.notna()

    if int(valid_mask.sum()) < MIN_MODEL_ROWS:
        raise RuntimeError(
            f"Not enough training rows ({int(valid_mask.sum())}) to fit bias model."
        )

    feature_cols = list(feature_df.columns)
    x_train = feature_df.loc[valid_mask, feature_cols].to_numpy(dtype=float)
    y_train = target.loc[valid_mask].to_numpy(dtype=float)

    if XGBRegressor is not None:
        model_name = "XGBoost bias model"
        model = XGBRegressor(
            n_estimators=350,
            learning_rate=0.05,
            max_depth=6,
            subsample=0.8,
            colsample_bytree=0.8,
            objective="reg:squarederror",
            reg_lambda=1.0,
            random_state=42,
            n_jobs=4,
            tree_method="hist",
        )
    else:
        model_name = "Linear bias model (NumPy fallback)"
        model = _NumpyLinearRegressor(l2=1e-2)

    model.fit(x_train, y_train)

    train_pred = model.predict(x_train)
    train_mae = float(np.mean(np.abs(y_train - train_pred)))
    train_rmse = float(np.sqrt(np.mean((y_train - train_pred) ** 2)))

    return {
        "model": model,
        "model_name": model_name,
        "feature_cols": feature_cols,
        "samples": int(valid_mask.sum()),
        "mae": train_mae,
        "rmse": train_rmse,
    }


def apply_station_corrections(station_df: pd.DataFrame, model_bundle: dict) -> pd.DataFrame:
    feature_df = build_error_features(station_df)
    feature_cols = model_bundle["feature_cols"]
    valid_mask = feature_df[feature_cols].notna().all(axis=1)

    predicted_error = np.zeros(len(station_df), dtype=float)
    if bool(valid_mask.any()):
        x_features = feature_df.loc[valid_mask, feature_cols].to_numpy(dtype=float)
        predicted_error[valid_mask.to_numpy()] = model_bundle["model"].predict(x_features)

    out = station_df.copy()
    out["forecast_temp"] = pd.to_numeric(out["forecast_temp"], errors="coerce")
    out["predicted_error"] = predicted_error
    out["corrected_temp"] = out["forecast_temp"] + out["predicted_error"]
    out["lat"] = pd.to_numeric(out["lat"], errors="coerce")
    out["lng"] = pd.to_numeric(out["lng"], errors="coerce")
    return out.dropna(subset=["lat", "lng", "corrected_temp"]).reset_index(drop=True)


def load_corrected_station_snapshot():
    model_bundle = train_error_model_bundle()
    station_df = fetch_latest_station_forecasts()
    corrected_df = apply_station_corrections(station_df, model_bundle)
    if corrected_df.empty:
        raise RuntimeError("No station rows available after correction step.")
    return corrected_df, model_bundle


def build_model_status_text(model_bundle, station_count):
    model_name = model_bundle.get("model_name", "Bias model")
    return (
        f"{model_name} | rows: {model_bundle['samples']:,} | "
        f"train MAE: {model_bundle['mae']:.2f} C | "
        f"train RMSE: {model_bundle['rmse']:.2f} C | "
        f"stations corrected: {station_count:,}"
    )
