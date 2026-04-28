import os
import sys

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.services.price_checker import PriceChecker


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


checker = PriceChecker(
    model_path="models/catboost_optuna.cbm",
    num_features=num_features,
    cat_features=cat_features,
    threshold_percent=10.0,
)


listing = {
    "price": 21_200_000,
    "rooms": 1,
    "area_total": 34.0,
    "area_kitchen": 8.0,
    "area_living": 24.0,
    "metro_time_min": 6,
    "floor": 5,
    "total_floors": 10,
    "year_built": None,
    "completion_year": None,
    "photo_count": 46,
    "is_studio": 0,
    "has_balcony": 1,
    "has_elevator": 0,
    "has_parking": 0,
    "has_renovation": 1,

    "okrug": "ЮЗАО",
    "district": "Академический",
    "metro": "Академическая",
    "house_type": "unknown",
    "seller_type": "owner",
}


result = checker.check_price(listing)

print(result)