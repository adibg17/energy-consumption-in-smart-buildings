#!/usr/bin/env python3
# model/preprocess_and_train.py

import os
import argparse
import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error, mean_absolute_error
from sklearn.model_selection import train_test_split
import joblib

parser = argparse.ArgumentParser()
parser.add_argument("--parquet_dir", default="data_parquet", help="Parquet directory (or HDFS path if accessible)")
parser.add_argument("--raw_csv", default="data/household_power_consumption.txt", help="Fallback raw csv")
parser.add_argument("--model_out", default="model/peak_model.joblib", help="Where to save model")
args = parser.parse_args()

def load_data():
    # try reading parquet first
    if os.path.exists(args.parquet_dir):
        print("Reading parquet files from", args.parquet_dir)
        df = pd.read_parquet(args.parquet_dir)
    else:
        print("Parquet path not found, reading raw csv")
        df = pd.read_csv(args.raw_csv, sep=';', na_values='?', low_memory=False)
    return df

def prepare_features(df):
    # Ensure we have a timestamp column
    if "timestamp" not in df.columns:
        # dataset uses Date and Time
        df['timestamp'] = pd.to_datetime(df['Date'] + ' ' + df['Time'], format='%d/%m/%Y %H:%M:%S', errors='coerce')
    df = df.sort_values('timestamp').dropna(subset=['timestamp'])

    # Use Global_active_power (in kilowatts) as main signal
    # Create global_active_kW (original is in kilowatts)
    df['gap'] = pd.to_numeric(df['Global_active_power'], errors='coerce')
    df = df.dropna(subset=['gap'])

    # Resample to hourly aggregate (sum or mean). Peak load (max) per hour is a target for "peak detection"
    df.set_index('timestamp', inplace=True)
    hourly = df['gap'].resample('1H').agg(['mean', 'max', 'sum']).rename(columns={'mean':'gap_mean','max':'gap_max','sum':'gap_sum'})
    hourly = hourly.reset_index()

    # Feature engineering: lag features (previous hours), rolling windows, hour of day, day of week
    hourly['hour'] = hourly['timestamp'].dt.hour
    hourly['dayofweek'] = hourly['timestamp'].dt.dayofweek
    hourly['is_weekend'] = hourly['dayofweek'].isin([5,6]).astype(int)

    for lag in [1,2,3,6,12,24]:
        hourly[f'gap_mean_lag_{lag}'] = hourly['gap_mean'].shift(lag)
        hourly[f'gap_max_lag_{lag}'] = hourly['gap_max'].shift(lag)

    hourly['rolling_3h_max'] = hourly['gap_max'].rolling(3).max().shift(1)
    hourly['rolling_24h_mean'] = hourly['gap_mean'].rolling(24).mean().shift(1)

    # Drop rows with NA (due to shifts)
    hourly = hourly.dropna()

    # Target: predict the next-hour peak (gap_max one hour ahead)
    hourly['target_peak_next'] = hourly['gap_max'].shift(-1)
    hourly = hourly.dropna(subset=['target_peak_next'])

    # Features list
    features = [c for c in hourly.columns if c not in ['timestamp','gap_mean','gap_max','gap_sum','target_peak_next']]
    X = hourly[features]
    y = hourly['target_peak_next']

    return X, y, hourly

def train_and_save(X, y, out_path):
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, shuffle=False)
    model = RandomForestRegressor(n_estimators=100, random_state=42, n_jobs=-1)
    print("Training model...")
    model.fit(X_train, y_train)
    preds = model.predict(X_test)
    mse = mean_squared_error(y_test, preds)
    mae = mean_absolute_error(y_test, preds)
    print(f"Test MSE: {mse:.4f}, MAE: {mae:.4f}")

    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    joblib.dump(model, out_path)
    print("Saved model to", out_path)
    return model

if __name__ == "__main__":
    df = load_data()
    X, y, hourly = prepare_features(df)
    model = train_and_save(X, y, args.model_out)
