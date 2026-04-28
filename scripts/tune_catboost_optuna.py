import sys
import json

import optuna
import pandas as pd

from catboost import CatBoostRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, mean_absolute_percentage_error, r2_score

import os
import sys

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.features.build_features import build_features


def prepare_data():
    data = pd.read_parquet("data/processed/move_ru_clean.parquet")
    data = build_features(data)

    target = "price_per_m2"

    num_features = [
        "rooms",
        "area_total",
        "area_kitchen",
        "area_living",
        "metro_time_min",
        "floor",
        "total_floors",
        "year_built",
        "completion_year",
        "photo_count",
        "is_studio",
        "has_balcony",
        "has_elevator",
        "has_parking",
        "has_renovation",
        "kitchen_ratio",
        "living_ratio",
        "floor_ratio",
        "is_first_floor",
        "is_last_floor",
        "house_age",
        "is_new_building",
    ]

    cat_features = [
        "okrug",
        "district",
        "metro",
        "house_type",
        "seller_type",
    ]

    data[num_features] = data[num_features].fillna(-1)
    data[cat_features] = data[cat_features].fillna("unknown")

    X = data[num_features + cat_features]
    y = data[target]

    return train_test_split(X, y, test_size=0.2, random_state=42), cat_features


def main():
    print("[*] Подготовка данных")

    (X_train, X_test, y_train, y_test), cat_features = prepare_data()

    cat_features_idx = [X_train.columns.get_loc(col) for col in cat_features]

    def objective(trial):
        params = {
            "iterations": trial.suggest_int("iterations", 300, 1200),
            "depth": trial.suggest_int("depth", 4, 10),
            "learning_rate": trial.suggest_float("learning_rate", 0.01, 0.15, log=True),
            "l2_leaf_reg": trial.suggest_float("l2_leaf_reg", 1.0, 20.0),
            "random_strength": trial.suggest_float("random_strength", 0.0, 10.0),
            "bagging_temperature": trial.suggest_float("bagging_temperature", 0.0, 5.0),
            "loss_function": "MAE",
            "eval_metric": "MAE",
            "random_seed": 42,
            "verbose": 0,
        }

        model = CatBoostRegressor(**params)

        model.fit(
            X_train,
            y_train,
            cat_features=cat_features_idx,
            eval_set=(X_test, y_test),
            use_best_model=True,
            early_stopping_rounds=100,
        )

        preds = model.predict(X_test)
        return mean_absolute_error(y_test, preds)

    print("[*] Запуск Optuna")

    study = optuna.create_study(direction="minimize")
    study.optimize(objective, n_trials=30)

    print("[+] Лучший MAE:", study.best_value)
    print("[+] Лучшие параметры:", study.best_params)

    best_params = study.best_params | {
        "loss_function": "MAE",
        "eval_metric": "MAE",
        "random_seed": 42,
        "verbose": 100,
    }

    print("[*] Финальное обучение")

    model = CatBoostRegressor(**best_params)

    model.fit(
        X_train,
        y_train,
        cat_features=cat_features_idx,
        eval_set=(X_test, y_test),
        use_best_model=True,
        early_stopping_rounds=100,
    )

    preds = model.predict(X_test)

    metrics = {
        "mae": mean_absolute_error(y_test, preds),
        "mape": mean_absolute_percentage_error(y_test, preds),
        "r2": r2_score(y_test, preds),
        "best_params": study.best_params,
    }

    print("[+] Метрики:", metrics)

    model.save_model("models/catboost_optuna.cbm")

    with open("reports/metrics_optuna.json", "w") as f:
        json.dump(metrics, f, indent=4)


if __name__ == "__main__":
    main()