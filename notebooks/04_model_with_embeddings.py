import os
import sys

import pandas as pd

from catboost import CatBoostRegressor, Pool
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, mean_absolute_percentage_error, r2_score


PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.features.build_features import build_features


data = pd.read_parquet("data/processed/move_ru_with_embeddings.parquet")

target = "price_per_m2"
y = data[target].copy()

data = build_features(data)

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

embedding_features = [
    "description_embedding",
]

data[num_features] = data[num_features].fillna(-1)
data[cat_features] = data[cat_features].fillna("unknown")

features = num_features + cat_features + embedding_features

X = data[features]

X_train, X_test, y_train, y_test = train_test_split(
    X,
    y,
    test_size=0.2,
    random_state=42,
)

cat_features_idx = [X.columns.get_loc(col) for col in cat_features]
embedding_features_idx = [X.columns.get_loc(col) for col in embedding_features]

train_pool = Pool(
    X_train,
    y_train,
    cat_features=cat_features_idx,
    embedding_features=embedding_features_idx,
)

test_pool = Pool(
    X_test,
    y_test,
    cat_features=cat_features_idx,
    embedding_features=embedding_features_idx,
)

model = CatBoostRegressor(
    iterations=1000,
    depth=8,
    learning_rate=0.05,
    loss_function="MAE",
    eval_metric="MAE",
    random_seed=42,
    verbose=100,
)

model.fit(
    train_pool,
    eval_set=test_pool,
    use_best_model=True,
    early_stopping_rounds=100,
)

preds = model.predict(test_pool)

mae = mean_absolute_error(y_test, preds)
mape = mean_absolute_percentage_error(y_test, preds)
r2 = r2_score(y_test, preds)

print("MAE:", round(mae, 2))
print("MAPE:", round(mape, 4))
print("R2:", round(r2, 4))

os.makedirs("models", exist_ok=True)
model.save_model("models/catboost_with_embeddings.cbm")