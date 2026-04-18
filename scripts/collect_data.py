import csv
import time
from pathlib import Path
import sys

import pandas as pd
import requests

sys.path.append(str(Path(__file__).resolve().parents[1]))

from src.scrapers.move_scraper import MoveRuScraper


OUTPUT_DIR = Path("data/raw")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

OUTPUT_CSV = OUTPUT_DIR / "move_ru_listings.csv"

FIELDNAMES = [
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


def get_first_not_none(item: dict, *keys):
    for key in keys:
        if key in item and item[key] is not None:
            return item[key]
    return None


def extract_listing_id_from_url(url: str) -> str | None:
    """
    Пытаемся достать listing_id из URL, если он есть в конце.
    Пример:
    https://move.ru/objects/moskva_bestujevyh_21_9284835489/ -> 9284835489
    """
    if not url:
        return None

    clean_url = url.rstrip("/")
    last_part = clean_url.split("_")[-1]

    if last_part.isdigit():
        return last_part

    return None


def collect_listing_urls(scraper: MoveRuScraper, max_listings: int = 100):
    urls = []
    seen = set()
    page = 1

    while len(urls) < max_listings:
        page_url = f"{scraper.base_url}/kvartiry/?page={page}"
        print(f"\n[*] Сбор ссылок. Страница {page}: {page_url}")

        try:
            response = requests.get(page_url, headers=scraper.headers, timeout=20)
            response.raise_for_status()
        except Exception as e:
            print(f"[!] Ошибка загрузки страницы {page}: {e}")
            break

        page_urls = scraper.parse_listing_urls(response.text)
        print(f"[+] На странице найдено ссылок: {len(page_urls)}")

        if not page_urls:
            print("[!] На странице не найдено ссылок, останавливаем сбор")
            break

        new_count = 0
        for url in page_urls:
            if url not in seen:
                seen.add(url)
                urls.append(url)
                new_count += 1

                if len(urls) >= max_listings:
                    break

        print(f"[+] Новых ссылок добавлено: {new_count} | Всего собрано: {len(urls)}")

        if new_count == 0:
            print("[!] Новых ссылок больше нет, останавливаемся")
            break

        page += 1
        time.sleep(1)

    return urls[:max_listings]


def normalize_item_for_csv(item: dict) -> dict:
    area_total = get_first_not_none(item, "area_total", "area")
    area_kitchen = get_first_not_none(item, "area_kitchen", "kitchen_area")
    area_living = get_first_not_none(item, "area_living", "living_area")
    metro_time_min = get_first_not_none(item, "metro_time_min", "metro_time")

    title = get_first_not_none(item, "title_clean", "title_raw", "title")
    description = get_first_not_none(item, "description_clean", "description_raw", "description")

    normalized = {
        "url": item.get("url"),
        "listing_id": item.get("listing_id"),
        "title": title,
        "deal_type": item.get("deal_type"),
        "object_type": item.get("object_type"),
        "price": item.get("price"),
        "rooms": item.get("rooms"),
        "area_total": area_total,
        "area_kitchen": area_kitchen,
        "area_living": area_living,
        "price_per_m2": item.get("price_per_m2"),
        "address": item.get("address"),
        "okrug": item.get("okrug"),
        "district": item.get("district"),
        "metro": item.get("metro"),
        "metro_time_min": metro_time_min,
        "floor": item.get("floor"),
        "total_floors": item.get("total_floors"),
        "house_type": item.get("house_type"),
        "year_built": item.get("year_built"),
        "completion_year": item.get("completion_year"),
        "seller_type": item.get("seller_type"),
        "photo_count": item.get("photo_count"),
        "image_urls": item.get("image_urls"),
        "is_studio": item.get("is_studio"),
        "has_balcony": item.get("has_balcony"),
        "has_elevator": item.get("has_elevator"),
        "has_parking": item.get("has_parking"),
        "has_renovation": item.get("has_renovation"),
        "description": description,
        "parse_errors": item.get("parse_errors"),
    }

    return normalized


def ensure_csv_exists(output_path: Path) -> None:
    if output_path.exists():
        return

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()

    print(f"[+] Создан новый CSV: {output_path}")


def append_row_to_csv(row: dict, output_path: Path) -> None:
    with open(output_path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writerow(row)


def load_existing_ids(csv_path: Path) -> set[str]:
    if not csv_path.exists():
        return set()

    try:
        df = pd.read_csv(csv_path)
    except Exception as e:
        print(f"[!] Не удалось прочитать существующий CSV: {e}")
        return set()

    if "listing_id" not in df.columns:
        return set()

    ids = (
        df["listing_id"]
        .dropna()
        .astype(str)
        .str.strip()
    )

    return set(ids)


def main():
    scraper = MoveRuScraper()
    max_listings = 25000

    print(f"[*] Сбор {max_listings} объявлений с Move.ru")

    ensure_csv_exists(OUTPUT_CSV)
    existing_ids = load_existing_ids(OUTPUT_CSV)

    print(f"[+] Уже сохранено объявлений: {len(existing_ids)}")

    urls = collect_listing_urls(scraper, max_listings=max_listings)

    if not urls:
        print("[!] Не удалось собрать ссылки")
        return

    print(f"\n[+] Ссылки собраны. Начинаю парсинг карточек: {len(urls)} шт.\n")

    parsed_now = 0
    skipped_existing = 0
    skipped_validation = 0
    parsed_errors = 0

    for i, url in enumerate(urls, start=1):
        preview_listing_id = extract_listing_id_from_url(url)

        if preview_listing_id and preview_listing_id in existing_ids:
            print(f"  [{i}/{len(urls)}] [=] skip existing listing_id={preview_listing_id}")
            skipped_existing += 1
            continue

        print(f"  [{i}/{len(urls)}] {url}")

        try:
            item = scraper.parse_item_details(url)
        except Exception as e:
            print(f"    [!] Ошибка парсинга: {e}")
            parsed_errors += 1
            continue

        if not item:
            print("    [!] Пустой результат парсинга")
            parsed_errors += 1
            continue

        normalized = normalize_item_for_csv(item)
        listing_id = normalized.get("listing_id")

        if listing_id is not None:
            listing_id = str(listing_id).strip()
            normalized["listing_id"] = listing_id

        if not listing_id:
            print("    [!] Пропуск: не удалось определить listing_id")
            parsed_errors += 1
            continue

        if listing_id in existing_ids:
            print(f"    [=] Уже есть в CSV, пропускаю listing_id={listing_id}")
            skipped_existing += 1
            continue

        if normalized["price"] is None or normalized["area_total"] is None:
            print(
                f"    [-] Пропуск по минимальной валидации: "
                f"price={normalized['price']} | area_total={normalized['area_total']}"
            )
            skipped_validation += 1
            continue

        print(
            f"    [+] price={normalized['price']} | "
            f"rooms={normalized['rooms']} | "
            f"area_total={normalized['area_total']} | "
            f"price_per_m2={normalized['price_per_m2']} | "
            f"floor={normalized['floor']}/{normalized['total_floors']} | "
            f"metro={normalized['metro']} ({normalized['metro_time_min']}) | "
            f"photos={normalized['photo_count']}"
        )

        append_row_to_csv(normalized, OUTPUT_CSV)
        existing_ids.add(listing_id)
        parsed_now += 1

        print(f"    [+] Сохранено в CSV: listing_id={listing_id}")

    total_saved = len(load_existing_ids(OUTPUT_CSV))

    print("\n[+] Парсинг завершён")
    print(f"[+] Новых объявлений сохранено в этом запуске: {parsed_now}")
    print(f"[=] Пропущено, уже были в CSV: {skipped_existing}")
    print(f"[-] Пропущено по валидации: {skipped_validation}")
    print(f"[!] Ошибок парсинга: {parsed_errors}")
    print(f"[+] Всего объявлений в CSV: {total_saved}")
    print(f"[+] Итоговый CSV: {OUTPUT_CSV}")


if __name__ == "__main__":
    main()