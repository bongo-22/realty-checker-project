import os
import sys

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.services.price_checker import PriceChecker


NUM_FEATURES = [
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

CAT_FEATURES = [
    "okrug",
    "district",
    "metro",
    "house_type",
    "seller_type",
]


def input_float(prompt: str, default=None):
    value = input(prompt).strip()

    if value == "" and default is not None:
        return default

    return float(value.replace(",", "."))


def input_str(prompt: str, default="unknown"):
    value = input(prompt).strip()

    if value == "":
        return default

    return value


def collect_listing_from_user() -> dict:
    print()
    print("REALTY CHECKER — ВВОД ДАННЫХ ОБЪЯВЛЕНИЯ")
    print("Если не знаете значение — нажмите Enter.")
    print()

    price = input_float("Цена квартиры, руб: ")
    area_total = input_float("Общая площадь, м²: ")
    rooms = input_float("Количество комнат: ")

    area_kitchen = input_float("Площадь кухни, м² [неизвестно]: ", default=None)
    area_living = input_float("Жилая площадь, м² [неизвестно]: ", default=None)
    metro_time_min = input_float("Время до метро, мин [неизвестно]: ", default=None)

    floor = input_float("Этаж [неизвестно]: ", default=None)
    total_floors = input_float("Всего этажей в доме [неизвестно]: ", default=None)

    okrug = input_str("Округ [unknown]: ")
    district = input_str("Район [unknown]: ")
    metro = input_str("Метро [unknown]: ")

    listing = {
        "price": price,
        "rooms": rooms,
        "area_total": area_total,
        "area_kitchen": area_kitchen,
        "area_living": area_living,
        "metro_time_min": metro_time_min,
        "floor": floor,
        "total_floors": total_floors,

        # значения по умолчанию
        "year_built": None,
        "completion_year": None,
        "photo_count": 0,
        "is_studio": 1 if rooms == 0 else 0,
        "has_balcony": 0,
        "has_elevator": 0,
        "has_parking": 0,
        "has_renovation": 0,

        "okrug": okrug,
        "district": district,
        "metro": metro,
        "house_type": "unknown",
        "seller_type": "unknown",
    }

    return listing


def print_report(result: dict) -> None:
    print()
    print("REALTY CHECKER — ОТЧЕТ ПО ОБЪЯВЛЕНИЮ")
    print()

    print(f"Цена квартиры: {result['actual_price']:,.0f} руб.".replace(",", " "))
    print(f"Площадь: {result['area_total']} м²")

    print()
    print("Оценка цены:")
    print(f"- Реальная цена за м²: {result['actual_price_per_m2']:,.0f} руб.".replace(",", " "))
    print(f"- Оценка модели за м²: {result['predicted_price_per_m2']:,.0f} руб.".replace(",", " "))
    print(f"- Отклонение: {result['diff_percent']:.2f}%")

    print()
    print("Вердикт:")

    if result["verdict"] == "overpriced":
        print("Объявление выглядит переоцененным.")
    elif result["verdict"] == "underpriced":
        print("Объявление выглядит недооцененным.")
    else:
        print("Цена близка к рыночной.")

    print()
    print("Комментарий:")
    print(result["comment"])

    print("=" * 60)
    print()


def main() -> None:
    checker = PriceChecker(
        model_path="models/catboost_optuna.cbm",
        num_features=NUM_FEATURES,
        cat_features=CAT_FEATURES,
        threshold_percent=10.0,
    )

    listing = collect_listing_from_user()
    result = checker.check_price(listing)

    print_report(result)


if __name__ == "__main__":
    main()