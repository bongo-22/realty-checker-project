import ast
import os
from io import BytesIO
from pathlib import Path
from typing import List, Optional
from urllib.parse import urlparse

import boto3
import pandas as pd
import requests
from botocore.exceptions import ClientError


RAW_CSV_PATH = Path("data/raw/move_ru_listings.csv")
OUTPUT_CSV_PATH = Path("data/intermediate/move_ru_listings_with_s3.csv")

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept-Language": "ru-RU,ru;q=0.9",
}


def parse_image_urls(value) -> List[str]:
    if value is None:
        return []

    if isinstance(value, list):
        return [str(x).strip() for x in value if str(x).strip()]

    if pd.isna(value):
        return []

    value = str(value).strip()
    if not value:
        return []

    try:
        parsed = ast.literal_eval(value)
        if isinstance(parsed, list):
            return [str(x).strip() for x in parsed if str(x).strip()]
    except (ValueError, SyntaxError):
        pass

    return []


def parse_s3_uris(value) -> List[str]:
    if value is None:
        return []

    if isinstance(value, list):
        return [str(x).strip() for x in value if str(x).strip()]

    if pd.isna(value):
        return []

    value = str(value).strip()
    if not value:
        return []

    try:
        parsed = ast.literal_eval(value)
        if isinstance(parsed, list):
            return [str(x).strip() for x in parsed if str(x).strip()]
    except (ValueError, SyntaxError):
        pass

    return []


def validate_image_urls(urls: List[str]) -> List[str]:
    valid = []
    for url in urls:
        low = url.lower()
        path = urlparse(low).path
        if low.startswith("http") and path.endswith((".jpg", ".jpeg", ".png", ".webp")):
            valid.append(url)
    return valid


def load_raw_dataset(csv_path: Path) -> pd.DataFrame:
    if not csv_path.exists():
        raise FileNotFoundError(f"Файл не найден: {csv_path}")
    return pd.read_csv(csv_path)


def load_existing_output(csv_path: Path) -> pd.DataFrame:
    if not csv_path.exists():
        return pd.DataFrame()
    return pd.read_csv(csv_path)


def guess_extension_from_url(url: str) -> str:
    path = urlparse(url).path.lower()
    for ext in [".jpg", ".jpeg", ".png", ".webp"]:
        if path.endswith(ext):
            return ext
    return ".jpg"


def guess_content_type(ext: str) -> str:
    ext = ext.lower()
    if ext in [".jpg", ".jpeg"]:
        return "image/jpeg"
    if ext == ".png":
        return "image/png"
    if ext == ".webp":
        return "image/webp"
    return "application/octet-stream"


def get_s3_client():
    endpoint_url = os.getenv("S3_ENDPOINT_URL", "https://storage.yandexcloud.net")
    access_key = os.getenv("S3_ACCESS_KEY")
    secret_key = os.getenv("S3_SECRET_KEY")
    region = os.getenv("S3_REGION", "ru-central1")

    if not access_key or not secret_key:
        raise ValueError("Не заданы S3_ACCESS_KEY / S3_SECRET_KEY")

    return boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region,
    )


def get_bucket_name() -> str:
    bucket_name = os.getenv("S3_BUCKET_NAME")
    if not bucket_name:
        raise ValueError("Не задана переменная S3_BUCKET_NAME")
    return bucket_name


def test_s3_connection() -> bool:
    s3_client = get_s3_client()
    bucket_name = get_bucket_name()

    try:
        s3_client.head_bucket(Bucket=bucket_name)
        print(f"[+] Бакет доступен: {bucket_name}\n")
        return True
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "Unknown")
        print(f"[!] Нет доступа к бакету '{bucket_name}': head_bucket -> {code}\n")
        return False


def download_image_bytes(url: str, timeout: int = 30) -> Optional[bytes]:
    try:
        response = requests.get(url, headers=HEADERS, timeout=timeout)
        response.raise_for_status()
        return response.content
    except Exception as e:
        print(f"    [!] Ошибка скачивания {url}: {e}")
        return None


def object_exists(s3_client, bucket_name: str, object_key: str) -> bool:
    try:
        s3_client.head_object(Bucket=bucket_name, Key=object_key)
        return True
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code in ("404", "NoSuchKey", "NotFound"):
            return False
        if code in ("403", "AccessDenied"):
            # Если нет прав на head_object, считаем что объекта нет
            # и пробуем загрузить
            return False
        raise


def upload_bytes_to_s3(
    s3_client,
    bucket_name: str,
    object_key: str,
    content: bytes,
    content_type: str,
) -> str:
    s3_client.upload_fileobj(
        Fileobj=BytesIO(content),
        Bucket=bucket_name,
        Key=object_key,
        ExtraArgs={"ContentType": content_type},
    )
    return f"s3://{bucket_name}/{object_key}"


def upload_images_for_listing(
    s3_client,
    bucket_name: str,
    listing_id: str,
    urls: List[str],
    max_images: Optional[int] = None,
) -> List[str]:
    s3_uris = []
    selected_urls = urls[:max_images] if max_images is not None else urls

    for idx, url in enumerate(selected_urls, start=1):
        ext = guess_extension_from_url(url)
        content_type = guess_content_type(ext)
        object_key = f"move_ru/images/{listing_id}/{idx:02d}{ext}"

        try:
            if object_exists(s3_client, bucket_name, object_key):
                s3_uri = f"s3://{bucket_name}/{object_key}"
                s3_uris.append(s3_uri)
                print(f"    [=] already exists -> {s3_uri}")
                continue
        except Exception as e:
            print(f"    [!] Ошибка проверки объекта {object_key}: {e}")

        image_bytes = download_image_bytes(url)
        if image_bytes is None:
            continue

        try:
            s3_uri = upload_bytes_to_s3(
                s3_client=s3_client,
                bucket_name=bucket_name,
                object_key=object_key,
                content=image_bytes,
                content_type=content_type,
            )
            s3_uris.append(s3_uri)
            print(f"    [+] uploaded -> {s3_uri}")
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "Unknown")
            print(f"    [!] Ошибка загрузки в S3 {object_key}: {error_code}")
        except Exception as e:
            print(f"    [!] Ошибка загрузки в S3 {object_key}: {e}")

    return s3_uris


def merge_with_existing_output(raw_df: pd.DataFrame, existing_df: pd.DataFrame) -> pd.DataFrame:
    work_df = raw_df.copy()
    work_df["listing_id"] = work_df["listing_id"].astype(str)

    if existing_df.empty:
        work_df["image_s3_uris"] = [[] for _ in range(len(work_df))]
        work_df["cover_image_s3_uri"] = [None for _ in range(len(work_df))]
        return work_df

    existing_df = existing_df.copy()
    existing_df["listing_id"] = existing_df["listing_id"].astype(str)

    keep_cols = ["listing_id"]
    if "image_s3_uris" in existing_df.columns:
        keep_cols.append("image_s3_uris")
    if "cover_image_s3_uri" in existing_df.columns:
        keep_cols.append("cover_image_s3_uri")

    existing_subset = existing_df[keep_cols].drop_duplicates(subset=["listing_id"], keep="last")

    merged = work_df.merge(existing_subset, on="listing_id", how="left")

    if "image_s3_uris" not in merged.columns:
        merged["image_s3_uris"] = [[] for _ in range(len(merged))]
    else:
        merged["image_s3_uris"] = merged["image_s3_uris"].apply(parse_s3_uris)

    if "cover_image_s3_uri" not in merged.columns:
        merged["cover_image_s3_uri"] = [None for _ in range(len(merged))]

    return merged


def save_dataset(df: pd.DataFrame, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)

    save_df = df.copy()
    save_df["image_s3_uris"] = save_df["image_s3_uris"].apply(
        lambda x: str(x) if isinstance(x, list) else str(parse_s3_uris(x))
    )

    save_df.to_csv(output_path, index=False)
    print(f"\n[+] CSV сохранён: {output_path}")


def process_dataset(
    df: pd.DataFrame,
    max_listings: Optional[int] = None,
    max_images_per_listing: Optional[int] = 3,
) -> pd.DataFrame:
    s3_client = get_s3_client()
    bucket_name = get_bucket_name()

    existing_output_df = load_existing_output(OUTPUT_CSV_PATH)
    work_df = merge_with_existing_output(df, existing_output_df)

    if max_listings is not None:
        work_df = work_df.head(max_listings).copy()

    print(f"[*] Начинаю загрузку изображений в S3")
    print(f"[+] Бакет: {bucket_name}")
    print(f"[+] Объявлений в обработке: {len(work_df)}\n")

    skipped_ready = 0
    processed_now = 0

    for idx in work_df.index:
        row = work_df.loc[idx]
        listing_id = str(row["listing_id"]).strip()

        existing_cover = row.get("cover_image_s3_uri")
        existing_uris = row.get("image_s3_uris", [])
        parsed_existing_uris = parse_s3_uris(existing_uris)

        if pd.notna(existing_cover) and str(existing_cover).strip():
            print(f"[{idx}] listing_id={listing_id} -> [=] уже обработано, пропуск")
            skipped_ready += 1
            continue

        urls = validate_image_urls(parse_image_urls(row["image_urls"]))
        print(f"[{idx}] listing_id={listing_id} -> найдено {len(urls)} изображений")

        if not urls:
            work_df.at[idx, "image_s3_uris"] = parsed_existing_uris
            work_df.at[idx, "cover_image_s3_uri"] = None
            save_dataset(work_df, OUTPUT_CSV_PATH)
            continue

        s3_uris = upload_images_for_listing(
            s3_client=s3_client,
            bucket_name=bucket_name,
            listing_id=listing_id,
            urls=urls,
            max_images=max_images_per_listing,
        )

        final_uris = s3_uris if s3_uris else parsed_existing_uris

        work_df.at[idx, "image_s3_uris"] = final_uris
        work_df.at[idx, "cover_image_s3_uri"] = final_uris[0] if final_uris else None

        processed_now += 1

        # Сохраняем прогресс после каждого объявления
        save_dataset(work_df, OUTPUT_CSV_PATH)

    print("\n[+] Загрузка изображений завершена")
    print(f"[+] Обработано новых объявлений в этом запуске: {processed_now}")
    print(f"[=] Уже были обработаны ранее: {skipped_ready}")

    return work_df


def main():
    print(f"[*] Чтение CSV: {RAW_CSV_PATH}")
    df = load_raw_dataset(RAW_CSV_PATH)

    print(f"[+] Загружено строк: {len(df)}")
    print(f"[+] Колонки: {list(df.columns)}\n")

    ok = test_s3_connection()
    if not ok:
        print("[!] S3 недоступен. Останавливаю скрипт.\n")
        return

    result_df = process_dataset(
        df=df,
        max_listings=None,
        max_images_per_listing=None,
    )

    save_dataset(result_df, OUTPUT_CSV_PATH)


if __name__ == "__main__":
    main()