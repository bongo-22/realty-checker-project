import pandas as pd

from catboost import CatBoostRegressor
from src.features.build_features import build_features


class PriceChecker:
    def __init__(
        self,
        model_path: str,
        num_features: list[str],
        cat_features: list[str],
        threshold_percent: float = 10.0,
    ):
        self.model = CatBoostRegressor()
        self.model.load_model(model_path)

        self.num_features = num_features
        self.cat_features = cat_features
        self.features = num_features + cat_features
        self.threshold_percent = threshold_percent

    def _prepare_input(self, listing: dict) -> pd.DataFrame:
        df = pd.DataFrame([listing])

        df = build_features(df)

        for feature in self.num_features:
            if feature not in df.columns:
                df[feature] = -1

        for feature in self.cat_features:
            if feature not in df.columns:
                df[feature] = "unknown"

        df[self.num_features] = df[self.num_features].fillna(-1)
        df[self.cat_features] = df[self.cat_features].fillna("unknown")

        return df[self.features]

    def predict_price_per_m2(self, listing: dict) -> float:
        X = self._prepare_input(listing)
        prediction = self.model.predict(X)[0]
        return float(prediction)

    def check_price(self, listing: dict) -> dict:
        predicted_price_per_m2 = self.predict_price_per_m2(listing)

        actual_price = listing.get("price")
        area_total = listing.get("area_total")

        if actual_price is None or area_total in (None, 0):
            raise ValueError("Нужны price и area_total для проверки цены")

        actual_price_per_m2 = actual_price / area_total

        diff_percent = (
            (actual_price_per_m2 - predicted_price_per_m2)
            / predicted_price_per_m2
            * 100
        )

        if diff_percent > self.threshold_percent:
            verdict = "overpriced"
            comment = "Цена выглядит завышенной относительно модели."
        elif diff_percent < -self.threshold_percent:
            verdict = "underpriced"
            comment = "Цена выглядит заниженной относительно модели."
        else:
            verdict = "fair"
            comment = "Цена выглядит близкой к рыночной."

        return {
            "actual_price": round(actual_price, 2),
            "area_total": round(area_total, 2),
            "actual_price_per_m2": round(actual_price_per_m2, 2),
            "predicted_price_per_m2": round(predicted_price_per_m2, 2),
            "diff_percent": round(diff_percent, 2),
            "verdict": verdict,
            "comment": comment,
        }