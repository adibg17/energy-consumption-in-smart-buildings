#!/usr/bin/env python3
# model/serve_model.py

from flask import Flask, request, jsonify
import joblib
import numpy as np
import pandas as pd

MODEL_PATH = "model/peak_model.joblib"
model = joblib.load(MODEL_PATH)

app = Flask(__name__)

@app.route("/predict", methods=["POST"])
def predict():
    """
    Expect JSON with keys = features used in training (see preprocess script).
    Example:
    {
      "hour": 14,
      "dayofweek": 2,
      "is_weekend": 0,
      "gap_mean_lag_1": 0.5,
      ...
    }
    """
    data = request.get_json()
    df = pd.DataFrame([data])
    preds = model.predict(df)
    return jsonify({"predicted_next_hour_peak": float(preds[0])})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
