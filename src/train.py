import json
import time
import numpy as np
import pandas as pd
from sklearn.neighbors import KNeighborsRegressor
from sklearn.neural_network import MLPRegressor
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import mean_absolute_error, mean_squared_error

from preprocess import load_data, add_features, split_data, fetch_temperature


def dow_average_baseline(train, test, n_weeks=5, decay=0.85):
    predictions = []
    all_data = pd.concat([train, test])
    
    for date, row in test.iterrows():
        dow = date.dayofweek
        month = date.month
        same_dow_month = all_data[
            (all_data.index.dayofweek == dow) &
            (all_data.index.month == month) &
            (all_data.index < date)
        ]
        last_n = same_dow_month.iloc[-n_weeks:]
        weights = np.array([decay ** i for i in range(len(last_n)-1, -1, -1)])
        weights = weights / weights.sum()
        pred = np.average(last_n['Consumption'].values, weights=weights)
        predictions.append(pred)
    
    return np.array(predictions)


def evaluate(y_true, y_pred, train_time, inference_time):
    mae = mean_absolute_error(y_true, y_pred)
    rmse = np.sqrt(mean_squared_error(y_true, y_pred))
    return {
        'MAE': round(mae, 2),
        'RMSE': round(rmse, 2),
        'train_time_sec': round(train_time, 3),
        'inference_time_sec': round(inference_time, 3)
    }


def train():
    df = load_data()
    print("Fetching temperature data...")
    temperature = fetch_temperature('2006-01-01', '2017-12-31')
    data = add_features(df, temperature)
    train_data, test_data, features, target = split_data(data)

    X_train = train_data[features]
    y_train = train_data[target]
    X_test = test_data[features]
    y_test = test_data[target]

    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)

    # baseline
    t0 = time.time()
    dow_pred = dow_average_baseline(train_data, test_data)
    baseline_time = time.time() - t0
    dow_metrics = evaluate(y_test, dow_pred, train_time=0, inference_time=baseline_time)
    print(f"Baseline  — MAE: {dow_metrics['MAE']}, RMSE: {dow_metrics['RMSE']}, "
          f"inference: {dow_metrics['inference_time_sec']}s")

    # KNN
    t0 = time.time()
    knn = KNeighborsRegressor(n_neighbors=10)
    knn.fit(X_train_scaled, y_train)
    knn_train_time = time.time() - t0

    t0 = time.time()
    knn_pred = knn.predict(X_test_scaled)
    knn_inference_time = time.time() - t0

    knn_metrics = evaluate(y_test, knn_pred, knn_train_time, knn_inference_time)
    print(f"KNN       — MAE: {knn_metrics['MAE']}, RMSE: {knn_metrics['RMSE']}, "
          f"train: {knn_metrics['train_time_sec']}s, "
          f"inference: {knn_metrics['inference_time_sec']}s")

    # MLP
    t0 = time.time()
    mlp = MLPRegressor(hidden_layer_sizes=(64, 32), activation='relu',
                       max_iter=1000, random_state=42)
    mlp.fit(X_train_scaled, y_train)
    mlp_train_time = time.time() - t0

    t0 = time.time()
    mlp_pred = mlp.predict(X_test_scaled)
    mlp_inference_time = time.time() - t0

    mlp_metrics = evaluate(y_test, mlp_pred, mlp_train_time, mlp_inference_time)
    print(f"MLP       — MAE: {mlp_metrics['MAE']}, RMSE: {mlp_metrics['RMSE']}, "
          f"train: {mlp_metrics['train_time_sec']}s, "
          f"inference: {mlp_metrics['inference_time_sec']}s")

    # save metrics
    metrics = {
        'baseline': dow_metrics,
        'knn': knn_metrics,
        'mlp': mlp_metrics
    }
    with open('metrics/results.json', 'w') as f:
        json.dump(metrics, f, indent=2)
    print("\nMetrics saved to metrics/results.json")


if __name__ == '__main__':
    train()