#!/usr/bin/env python3
"""
DBSCAN-based anomaly detection on mock file transfer logs.

This script:
- Loads mock transfer data
- Engineers features suitable for distance-based clustering
- Applies DBSCAN to identify anomalous (noise) records
- Outputs cluster labels for inspection

Noise points (cluster = -1) are interpreted as suspicious activity.
"""

import json
from datetime import datetime, timezone

import numpy as np
import pandas as pd
from sklearn.cluster import DBSCAN
from sklearn.preprocessing import StandardScaler


# -----------------------------
# Configuration
# -----------------------------

DATA_FILE = r"C:\Users\fcu1\Downloads\mockDataV2.json"


# DBSCAN hyperparameters
EPS = 0.9
MIN_SAMPLES = 8


# -----------------------------
# Helper functions
# -----------------------------

def hour_fraction_from_epoch_ms(epoch_ms: int) -> float:
    """
    Convert epoch milliseconds (UTC) into a fractional hour of day.
    Example: 13.5 = 13:30 UTC
    """
    dt = datetime.fromtimestamp(epoch_ms / 1000, tz=timezone.utc)
    return dt.hour + dt.minute / 60 + dt.second / 3600


def load_data(path: str) -> pd.DataFrame:
    """
    Load JSON log data into a Pandas DataFrame.
    """
    with open(path, "r", encoding="utf-8") as f:
        raw = json.load(f)

    df = pd.DataFrame.from_dict(raw, orient="index")
    return df


# -----------------------------
# Main processing pipeline
# -----------------------------

def main() -> None:
    # ---- Load data ----
    df = load_data(DATA_FILE)

    # ---- Feature engineering ----

    # Convert start time to hour-of-day (UTC)
    df["hour"] = df["transfer_start_ms"].apply(hour_fraction_from_epoch_ms)

    # Cyclical encoding of time:
    # Ensures 23:59 and 00:01 are close in feature space
    df["hour_sin"] = np.sin(2 * np.pi * df["hour"] / 24)
    df["hour_cos"] = np.cos(2 * np.pi * df["hour"] / 24)

    # Select features for DBSCAN
    # NOTE:
    # - ext_id is excluded (categorical, redundant)
    # - raw timestamps are excluded (distance meaningless)
    features = df[
        [
            "file_len",
            "ext_danger",
            "transfer_delta_s",
            "hour_sin",
            "hour_cos",
        ]
    ]

    # ---- Feature scaling ----
    # DBSCAN relies on distance; scaling is critical
    scaler = StandardScaler()
    X = scaler.fit_transform(features)

    # ---- DBSCAN clustering ----
    dbscan = DBSCAN(
        eps=EPS,
        min_samples=MIN_SAMPLES,
    )

    df["cluster"] = dbscan.fit_predict(X)

    # ---- Results summary ----
    print("\nCluster label counts:")
    print(df["cluster"].value_counts().sort_index())

    # Noise points are labeled as -1
    outliers = df[df["cluster"] == -1]

    print(f"\nDetected {len(outliers)} anomalous records (noise points).\n")

    # Show a few suspicious examples
    print("Sample anomalous records:")
    print(
        outliers[
            [
                "file_len",
                "ext_danger",
                "transfer_delta_s",
                "hour",
            ]
        ].head()
    )

    # Optional: save results for further analysis
    df.to_csv("mockData_with_clusters.csv", index=True)
    print("\nSaved results to mockData_with_clusters.csv")


# -----------------------------
# Entry point
# -----------------------------

if __name__ == "__main__":
    main()
