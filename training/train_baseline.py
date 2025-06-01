# training/train_baseline.py
from pathlib import Path

import joblib
import lightgbm as lgb
import pandas as pd

INFILE = Path("training/hourly_train.parquet")
MODEL = Path("models/lgbm_wait_1h.joblib")
MODEL.parent.mkdir(parents=True, exist_ok=True)

df = pd.read_parquet(INFILE)

X = df[["posted_wait", "temp_f", "precip_prob"]]
y = df["target"]

train_data = lgb.Dataset(X, label=y)

params = dict(
    objective="regression",
    metric="l1",  # MAE
    learning_rate=0.05,
    num_leaves=64,
    verbose=-1,
)

model = lgb.train(params, train_data, num_boost_round=400)
joblib.dump(model, MODEL)
print("✅ saved model →", MODEL)
print("MAE (train):", model.best_score["training"]["l1"])
