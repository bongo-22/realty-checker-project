import pandas as pd


def build_features(data: pd.DataFrame) -> pd.DataFrame:
    df = data.copy()

    # =========================
    # FEATURE ENGINEERING
    # =========================

    df["kitchen_ratio"] = df["area_kitchen"] / df["area_total"]
    df["living_ratio"] = df["area_living"] / df["area_total"]
    df["floor_ratio"] = df["floor"] / df["total_floors"]

    df["is_first_floor"] = (df["floor"] == 1).astype(int)
    df["is_last_floor"] = (df["floor"] == df["total_floors"]).astype(int)

    current_year = 2026
    df["house_age"] = current_year - df["year_built"]

    df["is_new_building"] = (
        (df["completion_year"].notna()) |
        (df["year_built"] >= 2020)
    ).astype(int)

    # =========================
    # DROP LEAKAGE
    # =========================

    df = df.drop(columns=[
        "price",
        "url",
        "listing_id",
        "image_urls",
        "parse_errors",
        "title",
        "description",
    ])

    return df