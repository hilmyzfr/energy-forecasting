import sys
import numpy as np
import pandas as pd
from fastapi import FastAPI
from pydantic import BaseModel
from sklearn.neighbors import KNeighborsRegressor
from sklearn.preprocessing import StandardScaler

sys.path.append('src')
from preprocess import load_data, add_features, split_data, fetch_temperature

# ── Load and train model on startup ──────────────────────────────────────
print("Loading data and training model...")
df = load_data()
temperature = fetch_temperature('2006-01-01', '2017-12-31')
data = add_features(df, temperature)
train_data, test_data, features, target = split_data(data)

X_train = train_data[features]
y_train = train_data[target]

scaler = StandardScaler()
X_train_scaled = scaler.fit_transform(X_train)

knn = KNeighborsRegressor(n_neighbors=10)
knn.fit(X_train_scaled, y_train)
print("Model ready.")

# ── API ───────────────────────────────────────────────────────────────────
app = FastAPI()

class PredictionRequest(BaseModel):
    date: str  # format: YYYY-MM-DD
    lag_1: float  # yesterday's actual consumption
    lag_7: float  # same day last week actual consumption

@app.get("/health")
def health():
    return {"status": "ok"}

@app.post("/predict")
def predict(request: PredictionRequest):
    date = pd.Timestamp(request.date)
    
    # fetch live temperature for the requested date
    temp_df = fetch_temperature(request.date, request.date)
    temperature = temp_df['temperature'].values[0]
    
    # build features
    rolling_7 = (request.lag_1 + request.lag_7) / 2  # approximation
    
    X = np.array([[
        date.dayofweek,
        date.month,
        int(date.dayofweek >= 5),
        int(date in __import__('holidays').Germany()),
        temperature,
        request.lag_1,
        request.lag_7,
        rolling_7
    ]])
    
    X_scaled = scaler.transform(X)
    prediction = knn.predict(X_scaled)[0]
    
    return {
        "date": request.date,
        "predicted_consumption_gwh": round(prediction, 2),
        "temperature_c": round(temperature, 1),
        "is_holiday": int(date in __import__('holidays').Germany())
    }