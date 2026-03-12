# Energy Consumption Forecaster

Forecasting daily electricity consumption for Germany using Open Power System Data.
I built this to sharpen the forecasting skills I developed at enercity, where I worked
on consumption models across a portfolio of several thousand industrial customers.

## What it does

Send a date and recent consumption values — the API returns a predicted consumption
in GWh. Temperature is fetched live from Open-Meteo, and German public holidays are
checked automatically.

Every prediction also runs a plausibility check. At enercity we reviewed hundreds of
customer model outputs daily by hand. This automates that process and only surfaces
the ones worth looking at.

## Dataset

Open Power System Data — daily electricity consumption for Germany, 2012–2017.
Source: https://open-power-system-data.org

## Models

Three models compared on 2017 holdout data (365 days):

1. **Day-of-week baseline** — weighted average of the last 5 same-weekday, same-month
   values. More recent weeks weighted higher. Standard baseline in operational forecasting.
2. **KNN** — k-nearest neighbours on time, lag and weather features.
3. **MLP** — two-layer neural network with the same feature set.

## Results

| Model | MAE | RMSE | Train time | Inference |
|---|---|---|---|---|
| Day-of-week baseline | 48 GWh | 78 GWh | - | 0.11s |
| KNN | 25 GWh | 35 GWh | 0.02s | 0.006s |
| MLP | 20 GWh | 32 GWh | 1.85s | ~0s |

KNN runs in the API — fast to train, near-instant inference, and only slightly behind
MLP on accuracy. Adding temperature and holiday features dropped MAE from 29 to 25
compared to time and lag features alone.

## Model Explainability

SHAP and LIME explain individual predictions (`src/explain.py`):

- SHAP KernelExplainer — global feature importance and per-prediction waterfall plots
- LIME — local surrogate explanations as a cross-check

### SHAP Summary Plot
![SHAP Summary](reports/shap_summary.png)

### SHAP Waterfall Plot
![SHAP Waterfall](reports/shap_waterfall.png)

### LIME Explanation
![LIME](reports/lime_explanation.png)

## Data Pipeline

Built with dbt on DuckDB for local development:

- `stg_energy` — staging view for raw consumption data
- `stg_weather` — staging view for weather data
- `fct_energy_features` — mart table with all engineered features
- 16 data quality tests covering nulls, uniqueness and accepted values

Feature engineering is also implemented in PySpark (`src/spark_features.py`) to
handle larger datasets and mirror production pipeline patterns.

## Features used

- Day of week, month, is_weekend
- is_holiday (German public holidays)
- temperature (live from Open-Meteo)
- lag_1 — yesterday's consumption
- lag_7 — same day last week
- rolling_7 — 7-day rolling average

## API

- `GET /health` — check if the service is running
- `POST /predict` — get a prediction with plausibility check

### Request
```json
{
  "date": "2024-03-08",
  "lag_1": 1350.0,
  "lag_7": 1380.0,
  "special_event": false,
  "model": "knn"
}
```

`model` options: `knn`, `mlp`, `baseline`, `all`

`special_event` — set to true if the customer flagged unusual consumption (shutdown,
production surge, closure). Suppresses the prediction warning, but input checks
still run regardless.

### Response
```json
{
  "date": "2024-03-08",
  "model": "all",
  "predictions_gwh": {
    "knn": 1386.85,
    "mlp": 1337.34,
    "baseline": 1463.37
  },
  "temperature_c": 2.5,
  "is_holiday": 0,
  "plausibility": {
    "is_plausible": true,
    "warning": null,
    "expected_range": [1160.93, 1741.4],
    "deviation_pct": 4.4,
    "special_event_mode": false,
    "data_issue": false
  }
}
```

## Plausibility check

Two checks run on every prediction:

1. **Input check** — flags if lag_1 deviates more than 50% from the historical mean
   for that weekday and month. Usually signals a data pipeline issue. Always runs,
   cannot be suppressed.
2. **Prediction check** — flags if the output deviates more than 20% from recent
   same-weekday values. Suppressed if special_event is true.

## How to run

**Train models:**
```bash
pip install -r requirements.txt
python3 src/train.py
```

**Run dbt pipeline:**
```bash
cd dbt_energy
dbt run
dbt test
```

**Run API locally:**
```bash
uvicorn src.api:app --reload
```

**Run with Docker:**
```bash
docker build -t energy-forecaster .
docker run -p 8000:8000 energy-forecaster
```

## Stack

Python · Scikit-learn · PySpark · dbt · DuckDB · FastAPI · Docker · SHAP · LIME · Open-Meteo API

## Next steps

- LLM email parser to extract special event flags from customer notifications automatically
- Streamlit dashboard for visualising predictions and plausibility flags
- GitHub Actions for automated testing