from __future__ import annotations

from functools import lru_cache

import numpy as np
import pandas as pd

from visualization.globe_data import (
    fetch_station_forecasts_with_r_predictions,
    fetch_latest_station_forecasts,
)

MODEL_LOOKBACK_DAYS = 120
MODEL_SAMPLE_LIMIT = 120000
MIN_MODEL_ROWS = 1000


@lru_cache(maxsize=1)
def train_error_model_bundle(
    lookback_days: int = MODEL_LOOKBACK_DAYS,
    sample_limit: int = MODEL_SAMPLE_LIMIT,
):
    """Returns model info for display - R model handles bias correction now."""
    return {
        "model_name": "R Linear Model (bias correction)",
        "samples": 0,
        "mae": 0.0,
        "rmse": 0.0,
    }


def apply_station_corrections(station_df: pd.DataFrame, model_bundle: dict) -> pd.DataFrame:
    """Apply R-based corrections - no Python bias correction needed.
    
    The station_df from fetch_station_forecasts_with_r_predictions() already
    contains r_predicted_temp, r_bias, and r_corrected_temp from the R model.
    """
    out = station_df.copy()
    
    # Use R-corrected temperature if available, otherwise use original
    out["corrected_temp"] = out.apply(
        lambda row: row["r_corrected_temp"] 
        if pd.notna(row.get("r_corrected_temp")) 
        else row.get("forecast_temp"),
        axis=1
    )
    
    out["predicted_error"] = out.get("r_bias", np.nan)
    out["lat"] = pd.to_numeric(out["lat"], errors="coerce")
    out["lng"] = pd.to_numeric(out["lng"], errors="coerce")
    
    return out.dropna(subset=["lat", "lng", "corrected_temp"]).reset_index(drop=True)


def load_corrected_station_snapshot():
    """Load station data with R model bias corrections applied."""
    # Use the new function that fetches both forecast + R predictions
    try:
        station_df = fetch_station_forecasts_with_r_predictions()
    except Exception:
        # Fallback to old method if R predictions not available
        station_df = fetch_latest_station_forecasts()
        station_df["r_predicted_temp"] = np.nan
        station_df["r_bias"] = np.nan
        station_df["r_corrected_temp"] = np.nan
    
    model_bundle = train_error_model_bundle()
    corrected_df = apply_station_corrections(station_df, model_bundle)
    
    if corrected_df.empty:
        raise RuntimeError("No station rows available after correction step.")
    return corrected_df, model_bundle


def build_model_status_text(model_bundle, station_count):
    model_name = model_bundle.get("model_name", "R Linear Model")
    return (
        f"{model_name} | "
        f"stations: {station_count:,}"
    )
