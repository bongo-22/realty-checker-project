from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

import pandas as pd
import pandera.errors as pa_errors

from src.schemas.listings_schema import LISTINGS_SCHEMA


RAW_PATH = ROOT / "data" / "raw" / "move_ru_listings.csv"
PROCESSED_DIR = ROOT / "data" / "processed"
CLEAN_PARQUET_PATH = PROCESSED_DIR / "move_ru_clean.parquet"
INVALID_CSV_PATH = PROCESSED_DIR / "move_ru_invalid.csv"


NUMERIC_COLUMNS = [
    "price",
    "rooms",
    "area_total",
    "area_kitchen",
    "area_living",
    "price_per_m2",
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
]

TEXT_COLUMNS = [
    "url",
    "listing_id",
    "title",
    "deal_type",
    "object_type",
    "address",
    "okrug",
    "district",
    "metro",
    "house_type",
    "seller_type",
    "image_urls",
    "description",
    "parse_errors",
]

REQUIRED_COLUMNS = [
    "url",
    "listing_id",
    "title",
    "deal_type",
    "object_type",
    "price",
    "rooms",
    "area_total",
    "area_kitchen",
    "area_living",
    "price_per_m2",
    "address",
    "okrug",
    "district",
    "metro",
    "metro_time_min",
    "floor",
    "total_floors",
    "house_type",
    "year_built",
    "completion_year",
    "seller_type",
    "photo_count",
    "image_urls",
    "is_studio",
    "has_balcony",
    "has_elevator",
    "has_parking",
    "has_renovation",
    "description",
    "parse_errors",
]


def ensure_dirs() -> None:
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)


def read_raw_csv(path: Path) -> pd.DataFrame:
    print(f"[*] Читаю raw CSV: {path}")
    df = pd.read_csv(path)
    print(f"[+] Загружено строк: {len(df)}")
    print(f"[+] Колонки: {list(df.columns)}")
    return df


def ensure_required_columns(df: pd.DataFrame) -> pd.DataFrame:
    missing = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing:
        raise ValueError(f"В CSV отсутствуют обязательные колонки: {missing}")

    return df[REQUIRED_COLUMNS].copy()


def preprocess_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    for col in NUMERIC_COLUMNS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    for col in TEXT_COLUMNS:
        if col in df.columns:
            df[col] = df[col].astype("string")
            df[col] = df[col].str.strip()

    # Пустые строки -> NA
    for col in TEXT_COLUMNS:
        if col in df.columns:
            df[col] = df[col].replace({"": pd.NA, "nan": pd.NA, "None": pd.NA})

    # listing_id как строка без .0
    if "listing_id" in df.columns:
        df["listing_id"] = df["listing_id"].astype("string").str.replace(r"\.0$", "", regex=True)

    print(f"[+] После preprocess: {len(df)} строк")
    return df


def drop_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    before = len(df)

    if "listing_id" in df.columns:
        df = df.drop_duplicates(subset=["listing_id"], keep="first")
    else:
        df = df.drop_duplicates()

    after = len(df)
    print(f"[+] После удаления дублей: {after} строк")
    print(f"[+] Удалено дублей: {before - after}")
    return df


def apply_basic_filters(df: pd.DataFrame) -> pd.DataFrame:
    before = len(df)

    mask = pd.Series(True, index=df.index)

    mask &= df["price"].notna()
    mask &= df["area_total"].notna()

    mask &= df["price"].between(500_000, 500_000_000, inclusive="both")
    mask &= df["area_total"].between(10, 500, inclusive="both")

    if "rooms" in df.columns:
        mask &= (df["rooms"].isna()) | (df["rooms"].between(0, 10, inclusive="both"))

    if "floor" in df.columns:
        mask &= (df["floor"].isna()) | (df["floor"].between(1, 150, inclusive="both"))

    if "total_floors" in df.columns:
        mask &= (df["total_floors"].isna()) | (df["total_floors"].between(1, 200, inclusive="both"))

    if "floor" in df.columns and "total_floors" in df.columns:
        mask &= (
            df["floor"].isna()
            | df["total_floors"].isna()
            | (df["floor"] <= df["total_floors"])
        )

    df = df[mask].copy()

    after = len(df)
    print(f"[+] После базовых фильтров: {after} строк")
    print(f"[+] Отсеяно базовыми фильтрами: {before - after}")
    return df


def format_schema_errors(exc: pa_errors.SchemaErrors) -> str:
    failure_cases = exc.failure_cases.copy()

    if failure_cases.empty:
        return "unknown schema error"

    for col in failure_cases.columns:
        failure_cases[col] = failure_cases[col].astype(str)

    messages = []
    for _, row in failure_cases.iterrows():
        column = row.get("column", "")
        check = row.get("check", "")
        failure_case = row.get("failure_case", "")
        messages.append(f"{column}: {check}: {failure_case}")

    return " | ".join(messages)


def split_valid_invalid(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    valid_rows = []
    invalid_rows = []

    print("[*] Проверяю строки через Pandera...")

    for _, row in df.iterrows():
        row_df = pd.DataFrame([row.to_dict()])

        try:
            validated = LISTINGS_SCHEMA.validate(row_df, lazy=True)
            valid_rows.append(validated.iloc[0].to_dict())
        except pa_errors.SchemaErrors as exc:
            bad_row = row.to_dict()
            bad_row["schema_errors"] = format_schema_errors(exc)
            invalid_rows.append(bad_row)
        except Exception as exc:
            bad_row = row.to_dict()
            bad_row["schema_errors"] = str(exc)
            invalid_rows.append(bad_row)

    df_valid = pd.DataFrame(valid_rows)
    df_invalid = pd.DataFrame(invalid_rows)

    print(f"[+] Валидных строк: {len(df_valid)}")
    print(f"[+] Невалидных строк: {len(df_invalid)}")

    return df_valid, df_invalid


def save_results(df_valid: pd.DataFrame, df_invalid: pd.DataFrame) -> None:
    print(f"[*] Сохраняю clean parquet: {CLEAN_PARQUET_PATH}")
    df_valid.to_parquet(CLEAN_PARQUET_PATH, index=False)

    print(f"[*] Сохраняю invalid csv: {INVALID_CSV_PATH}")
    df_invalid.to_csv(INVALID_CSV_PATH, index=False)

    print("[+] Готово")


def main() -> None:
    ensure_dirs()

    df = read_raw_csv(RAW_PATH)
    df = ensure_required_columns(df)
    df = preprocess_dataframe(df)
    df = drop_duplicates(df)
    df = apply_basic_filters(df)
    df_valid, df_invalid = split_valid_invalid(df)
    save_results(df_valid, df_invalid)


if __name__ == "__main__":
    main()