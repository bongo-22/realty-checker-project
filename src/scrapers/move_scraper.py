import re
import time
from datetime import datetime
from typing import Optional, Tuple

import requests
from bs4 import BeautifulSoup


class MoveRuScraper:
    def __init__(self):
        self.base_url = "https://move.ru"
        self.source = "move_ru"
        self.headers = {
            "User-Agent": "Mozilla/5.0",
            "Accept-Language": "ru-RU,ru;q=0.9",
        }

    # UTILITIES

    def _clean_text(self, text: str) -> str:
        if not text:
            return ""
        text = text.replace("\xa0", " ")
        text = re.sub(r"\s+", " ", text)
        return text.strip()

    def _safe_int(self, value):
        if value is None:
            return None
        if isinstance(value, int):
            return value

        value = str(value).replace("\xa0", " ")
        value = re.sub(r"[^\d]", "", value)
        if not value:
            return None

        try:
            return int(value)
        except ValueError:
            return None

    def _safe_float(self, value):
        if value is None:
            return None
        if isinstance(value, (int, float)):
            return float(value)

        value = str(value).replace("\xa0", " ").replace(",", ".")
        value = re.sub(r"[^0-9.]", "", value)
        if not value:
            return None

        try:
            return float(value)
        except ValueError:
            return None

    def _normalize_text_for_rag(self, text: str) -> str:
        if not text:
            return ""

        text = text.lower()
        text = text.replace("\xa0", " ")
        text = text.replace("«", '"').replace("»", '"')
        text = text.replace("ё", "е")

        text = re.sub(r"\bкв\.?\s*м\.?\b", " м2 ", text)
        text = re.sub(r"\bм²\b", " м2 ", text)
        text = re.sub(r"\bметров\b", " м ", text)

        text = re.sub(r"[^\w\s.,:;!?/\-()%\"]", " ", text)
        text = re.sub(r"\s+", " ", text)

        return text.strip()

    def get_soup(self, url: str):
        try:
            time.sleep(1)
            response = requests.get(url, headers=self.headers, timeout=20)
            if response.status_code == 200:
                return BeautifulSoup(response.text, "lxml")
            print(f"[!] HTTP {response.status_code}: {url}")
        except Exception as e:
            print(f"[!] Ошибка запроса {url}: {e}")
        return None

    # URLS OF LISTINGS

    def parse_listing_urls(self, html: str):
        soup = BeautifulSoup(html, "lxml")
        links = set()

        for a in soup.find_all("a", href=True):
            href = a["href"]

            if "/objects/" not in href:
                continue

            if href.startswith("/"):
                href = self.base_url + href

            if href.startswith("https://move.ru/objects/"):
                links.add(href)

        return sorted(links)

    # PHOTO PARSING

    def _normalize_photo_url(self, url: str) -> str:
        if not url:
            return ""

        url = url.strip().replace("&amp;", "&")

        if url.startswith("//"):
            url = "https:" + url

        return url

    def _photo_dedupe_key(self, url: str) -> str:
        if not url:
            return ""

        url = self._normalize_photo_url(url)
        url = url.split("?", 1)[0]
        url = re.sub(r"@[\d]+x(?=\.)", "", url, flags=re.IGNORECASE)
        return url.lower()

    def _is_real_photo(self, url: str) -> bool:
        if not url:
            return False

        url = self._normalize_photo_url(url)
        url_lower = url.lower()

        if not url_lower.startswith("http"):
            return False

        if not any(ext in url_lower for ext in [".jpg", ".jpeg", ".png", ".webp"]):
            return False

        bad_parts = [
            "logo",
            "icon",
            "avatar",
            "svg",
            "favicon",
            "pixel",
            "tracker",
            "banner",
            "placeholder",
            "blank",
            "static-maps.yandex",
            "yandex.ru/map",
            "/map?",
            "tgb-item",
            "analytics.move.ru/go",
            "advert",
            "reklama",
        ]
        if any(part in url_lower for part in bad_parts):
            return False

        good_parts = [
            "/images/items/",
            "static-i",
            "move.ru/optimize/",
            "i15.move.ru/",
            "i16.move.ru/",
            "i17.move.ru/",
        ]
        return any(part in url_lower for part in good_parts)

    def _add_photo(self, photo_url: str, photos: list, seen: set, listing_id: str = None):
        photo_url = self._normalize_photo_url(photo_url)
        if not self._is_real_photo(photo_url):
            return

        # если в URL картинки есть listing_id — проверяем, что это текущий объект
        if listing_id and "/images/items/" in photo_url:
            if f"/{listing_id}/" not in photo_url:
                return

        key = self._photo_dedupe_key(photo_url)
        if not key or key in seen:
            return

        seen.add(key)
        photos.append(photo_url)

    def _extract_photos_from_gallery(self, soup: BeautifulSoup, listing_id: str = None):
        photos = []
        seen = set()

        selectors = [
            "div.card-objects-gallery img",
            ".card-objects-gallery img",
            "[class*='card-objects-gallery'] img",
        ]

        for selector in selectors:
            nodes = soup.select(selector)
            if not nodes:
                continue

            for img in nodes:
                for attr in ["srcset", "data-srcset", "src", "data-src"]:
                    value = (img.get(attr) or "").strip()
                    if not value:
                        continue

                    if "srcset" in attr:
                        for part in value.split(","):
                            raw_url = part.strip().split(" ")[0].strip()
                            self._add_photo(raw_url, photos, seen, listing_id)
                    else:
                        self._add_photo(value, photos, seen, listing_id)

            if photos:
                return photos

        return photos

    def _extract_all_photos(self, html: str, listing_id: str = None):
        photos = []
        seen = set()

        patterns = [
            r'https://static-i\d+\.move\.ru/images/items/[^"\']+\.(?:jpg|jpeg|png|webp)(?:\?[^"\']*)?',
            r'https://i\d+\.move\.ru/optimize/[^"\', ]+\.(?:jpg|jpeg|png|webp)(?:\?[^"\', ]*)?',
            r'//static-i\d+\.move\.ru/images/items/[^"\']+\.(?:jpg|jpeg|png|webp)(?:\?[^"\']*)?',
            r'//i\d+\.move\.ru/optimize/[^"\', ]+\.(?:jpg|jpeg|png|webp)(?:\?[^"\', ]*)?',
        ]

        for pattern in patterns:
            found = re.findall(pattern, html, re.IGNORECASE)
            for photo_url in found:
                self._add_photo(photo_url, photos, seen, listing_id)

        return photos


    # MAIN BLOCKS

    def _extract_main_text(self, soup: BeautifulSoup) -> str:
        candidates = []

        for tag in soup.find_all(["main", "article", "section", "div"]):
            text = self._clean_text(tag.get_text(" ", strip=True))
            if len(text) < 500:
                continue

            score = 0
            keywords = [
                "Описание",
                "Подробные характеристики",
                "Цена",
                "Количество комнат",
                "Общая площадь",
                "Жилая площадь",
                "Площадь кухни",
                "Этаж",
                "Тип жилья",
                "Тип объекта",
            ]

            for kw in keywords:
                if kw.lower() in text.lower():
                    score += 1

            if score >= 4:
                candidates.append((score, len(text), text))

        if candidates:
            candidates.sort(key=lambda x: (x[0], x[1]), reverse=True)
            return candidates[0][2]

        return self._clean_text(soup.get_text(" ", strip=True))

    def _extract_description_from_soup(self, soup: BeautifulSoup) -> str:
        selectors = [
            "div.card-objects-description__description-text",
            "div.card-objects-description__description-text_clamped",
            "div.card-objects-description__viewport",
        ]

        for selector in selectors:
            node = soup.select_one(selector)
            if node:
                text = self._clean_text(node.get_text(" ", strip=True))
                if len(text) > 50:
                    return text[:6000]

        return ""

    def _extract_description(self, main_text: str) -> str:
        match = re.search(
            r"Описание\s+(.*?)\s+Подробные характеристики",
            main_text,
            re.IGNORECASE | re.DOTALL
        )
        if match:
            return self._clean_text(match.group(1))[:6000]

        match = re.search(
            r"Описание\s+(.*)",
            main_text,
            re.IGNORECASE | re.DOTALL
        )
        if match:
            desc = self._clean_text(match.group(1))
            desc = re.split(
                r"Подробные характеристики|Похожие объявления|Контакты|Расположение",
                desc,
                maxsplit=1
            )[0]
            return self._clean_text(desc)[:6000]

        return ""

    def _extract_spec_cards(self, soup: BeautifulSoup) -> dict:
        specs = {}

        for card in soup.select("div.card-specifications__cards-card"):
            title_node = card.select_one("div.card-specifications__card-title")
            desc_node = card.select_one("div.card-specifications__card-description")

            if not title_node or not desc_node:
                continue

            key = self._clean_text(desc_node.get_text(" ", strip=True)).lower()
            value = self._clean_text(title_node.get_text(" ", strip=True))

            if key and value:
                specs[key] = value

        return specs

    def _extract_json_ld_text(self, soup: BeautifulSoup) -> str:
        chunks = []

        for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
            text = script.string or script.get_text(" ", strip=True)
            text = self._clean_text(text)
            if text:
                chunks.append(text)

        return " ".join(chunks)

    def _extract_script_text(self, soup: BeautifulSoup) -> str:
        chunks = []

        for script in soup.find_all("script"):
            text = script.string or script.get_text(" ", strip=True)
            text = self._clean_text(text)
            if not text:
                continue

            if any(marker in text for marker in [
                "window.__INITIAL_STATE__",
                "__NUXT__",
                "address",
                "metro",
                "year",
                "coordinates",
                "description",
            ]):
                chunks.append(text)

        return " ".join(chunks)[:15000]

    # FIELD PARSERS

    def _parse_listing_id(self, url: str):
        match = re.search(r"_(\d+)/?$", url)
        return match.group(1) if match else None

    def _parse_title(self, soup: BeautifulSoup):
        h1 = soup.find("h1")
        return self._clean_text(h1.get_text()) if h1 else None

    def _parse_title_clean(self, title: str) -> str:
        if not title:
            return ""
        t = self._clean_text(title)
        t = (
            t.replace("Продается", "")
             .replace("Продажа", "")
             .replace("Продаем", "")
             .replace("Продаю", "")
             .replace("Продам", "")
        )
        return self._clean_text(t).strip(",")

    def _parse_price(self, text: str):
        patterns = [
            r"Цена\s+([\d\s]+)\s*₽",
            r"([\d\s]+)\s*₽",
            r"цена\s+([\d\s]+)\s*руб",
            r"([\d\s]+)\s*руб",
        ]
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                return self._safe_int(match.group(1))
        return None

    def _parse_rooms(self, title: str, text: str, specs: dict = None):
        if specs:
            v = specs.get("комнатность")
            if v:
                m = re.search(r"(\d+)", v)
                if m:
                    return self._safe_int(m.group(1))

        source = f"{title or ''} {text or ''}"

        match = re.search(r"(\d+)[-\s]*(?:комн|комнат)", source, re.IGNORECASE)
        if match:
            return self._safe_int(match.group(1))

        if "студия" in source.lower():
            return 0

        return None

    def _parse_area(self, title: str, text: str, specs: dict = None):
        if specs:
            v = specs.get("общая")
            if v:
                m = re.search(r"(\d+[\.,]?\d*)", v)
                if m:
                    return self._safe_float(m.group(1))

        if title:
            match = re.search(r"(\d+[\.,]?\d*)\s*м²", title, re.IGNORECASE)
            if match:
                return self._safe_float(match.group(1))

        patterns = [
            r"Общая площадь\s+(\d+[\.,]?\d*)\s*м",
            r"общая\s+(\d+[\.,]?\d*)\s*м",
            r"площадью\s+(\d+[\.,]?\d*)\s*м",
        ]
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                return self._safe_float(match.group(1))
        return None

    def _parse_kitchen_area(self, text: str, specs: dict = None):
        if specs:
            v = specs.get("кухня")
            if v:
                m = re.search(r"(\d+[\.,]?\d*)", v)
                if m:
                    return self._safe_float(m.group(1))

        patterns = [
            r"Площадь кухни\s+(\d+[\.,]?\d*)\s*м",
            r"кухня\s+(\d+[\.,]?\d*)\s*м",
            r"кухни\s+(\d+[\.,]?\d*)\s*м",
        ]
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                return self._safe_float(match.group(1))
        return None

    def _parse_living_area(self, text: str, specs: dict = None):
        if specs:
            v = specs.get("жилая")
            if v:
                m = re.search(r"(\d+[\.,]?\d*)", v)
                if m:
                    return self._safe_float(m.group(1))

        patterns = [
            r"Жилая площадь\s+(\d+[\.,]?\d*)\s*м",
            r"жилая\s+(\d+[\.,]?\d*)\s*м",
        ]
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                return self._safe_float(match.group(1))
        return None

    def _parse_floor_info(self, text: str, specs: dict = None):
        if specs:
            v = specs.get("этаж")
            if v:
                m = re.search(r"(\d+)\s+из\s+(\d+)", v, re.IGNORECASE)
                if m:
                    return self._safe_int(m.group(1)), self._safe_int(m.group(2))

        patterns = [
            r"(\d+)\s+из\s+(\d+)\s+этаж",
            r"Этаж\s+(\d+)/(\d+)",
            r"(\d+)/(\d+)\s+эт",
        ]
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                return self._safe_int(match.group(1)), self._safe_int(match.group(2))
        return None, None

    def _parse_house_type(self, text: str):
        house_types = [
            "монолитно-кирпичный",
            "монолитный",
            "панельный",
            "кирпичный",
            "блочный",
            "сталинский",
        ]

        text_lower = text.lower()
        for house_type in house_types:
            if house_type in text_lower:
                return house_type
        return None

    def _parse_year_fields(self, text: str, specs: dict = None):
        year_built = None
        completion_year = None

        if specs:
            v = specs.get("год постройки")
            if v:
                year = self._safe_int(v)
                if year is not None and 1800 <= year <= 2035:
                    year_built = year

        if specs:
            v = specs.get("срок сдачи")
            if v:
                m = re.search(r"(20\d{2})", v)
                if m:
                    year = int(m.group(1))
                    if 2000 <= year <= 2035:
                        completion_year = year

        if year_built is None:
            patterns = [
                r"(\d{4})\s*г\.\s*год постройки",
                r"(\d{4})\s*года постройки",
                r"год постройки\s+(\d{4})",
                r"дом\s+(\d{4})\s+года\s+постройки",
            ]
            for pattern in patterns:
                match = re.search(pattern, text, re.IGNORECASE)
                if match:
                    year = self._safe_int(match.group(1))
                    if year is not None and 1800 <= year <= 2035:
                        year_built = year
                        break

        if completion_year is None:
            strict_patterns = [
                r"срок сдачи[^0-9]{0,20}(20\d{2})",
                r"сдача[^0-9]{0,20}(20\d{2})",
                r"ввод в эксплуатацию[^0-9]{0,20}(20\d{2})",
                r"\b[1-4]\s*кв\.?\s*(20\d{2})",
                r"ключ[^\d]{0,20}(20\d{2})",
                r"передач[^\d]{0,20}(20\d{2})",
            ]
            for pattern in strict_patterns:
                match = re.search(pattern, text, re.IGNORECASE)
                if match:
                    year = self._safe_int(match.group(1))
                    if year is not None and 2000 <= year <= 2035:
                        completion_year = year
                        break

        if year_built is not None and completion_year is not None:
            if year_built >= completion_year:
                completion_year = None

        return year_built, completion_year

    # ADDRESS PARSING

    def _normalize_address(self, address: str):
        if not address:
            return None

        address = self._clean_text(address)

        address = re.sub(r"^г\.\s*москва,?\s*", "Москва, ", address, flags=re.IGNORECASE)
        address = re.sub(r"^москва,?\s*", "Москва, ", address, flags=re.IGNORECASE)

        address = re.sub(r"\b(\d+)a\b", r"\1а", address, flags=re.IGNORECASE)
        address = re.sub(r"\b(\d+)b\b", r"\1б", address, flags=re.IGNORECASE)
        address = re.sub(r"\b(\d+)k(\d+)\b", r"\1к\2", address, flags=re.IGNORECASE)
        address = re.sub(r"\b(\d+)s(\d+)\b", r"\1с\2", address, flags=re.IGNORECASE)

        address = re.sub(r"\bпер\.\b", "переулок", address, flags=re.IGNORECASE)
        address = re.sub(r"\bпер\b", "переулок", address, flags=re.IGNORECASE)
        address = re.sub(r"\bпр-д\b", "проезд", address, flags=re.IGNORECASE)
        address = re.sub(r"\bул\.\b", "улица", address, flags=re.IGNORECASE)
        address = re.sub(r"\bул\b", "улица", address, flags=re.IGNORECASE)
        address = re.sub(r"\bпр-т\b", "проспект", address, flags=re.IGNORECASE)
        address = re.sub(r"\bпр-кт\b", "проспект", address, flags=re.IGNORECASE)
        address = re.sub(r"\bнаб\.\b", "набережная", address, flags=re.IGNORECASE)
        address = re.sub(r"\bб-р\b", "бульвар", address, flags=re.IGNORECASE)
        address = re.sub(r"\bш\.\b", "шоссе", address, flags=re.IGNORECASE)

        address = re.sub(r"\b1-м\b", "1-й", address, flags=re.IGNORECASE)
        address = re.sub(r"\b2-м\b", "2-й", address, flags=re.IGNORECASE)
        address = re.sub(r"\b3-м\b", "3-й", address, flags=re.IGNORECASE)

        address = re.sub(r"\bд\s+(\d)", r"д. \1", address, flags=re.IGNORECASE)
        address = re.sub(r"\bд\.\s*(\d+[а-яa-z0-9/\-]*)", r"д. \1", address, flags=re.IGNORECASE)
        address = re.sub(r"\bк\.\s*(\d+[а-яa-z0-9\-]*)", r"к. \1", address, flags=re.IGNORECASE)
        address = re.sub(r"\bс\.\s*(\d+[а-яa-z0-9\-]*)", r"с. \1", address, flags=re.IGNORECASE)
        address = re.sub(r"\bкорп\.\s*(\d+[а-яa-z0-9\-]*)", r"к. \1", address, flags=re.IGNORECASE)
        address = re.sub(r"\bкорпус\s*(\d+[а-яa-z0-9\-]*)", r"к. \1", address, flags=re.IGNORECASE)
        address = re.sub(r"\bстр\.\s*(\d+[а-яa-z0-9\-]*)", r"стр. \1", address, flags=re.IGNORECASE)

        address = re.sub(r"\s+,", ",", address)
        address = re.sub(r",\s*,", ", ", address)
        address = re.sub(r"\s+", " ", address).strip(" ,.-:")

        if address and not address.lower().startswith("москва"):
            address = f"Москва, {address}"

        return address or None

    def _is_bad_short_address(self, address: str) -> bool:
        if not address:
            return True

        address = self._clean_text(address)

        bad_patterns = [
            r"Москва,\s*$",
            r"Москва,\s*:\s*г$",
            r"Москва,\s*\d+[а-яА-Яa-zA-Z0-9\-]*$",
            r"Москва,\s*[1-9]$",
            r"Москва,\s*без улицы$",
            r"Москва,\s*канал им\.? москвы$",
            r"Москва,\s*[А-Яа-яA-Za-z]{1,3}$",
        ]

        bad_substrings = [
            "отлично подходит",
            "камерный бц",
            "бизнес-класс",
            "премиальная жизнь",
            "жилой комплекс",
            "идеальный вариант",
            "оставьте заявку",
            "альфа дом",
            "недвижимость с умом",
        ]

        if len(address) < 10:
            return True

        if any(x in address.lower() for x in bad_substrings):
            return True

        return any(re.fullmatch(p, address, flags=re.IGNORECASE) for p in bad_patterns)

    def _split_address(self, address: str) -> Tuple[Optional[str], Optional[str]]:
        if not address:
            return None, None

        a = address.replace("Москва,", "").strip()
        a = self._clean_text(a)

        m = re.search(
            r"(.+?),\s*(?:д\.\s*)?([0-9а-яa-z/\-]+(?:\s*к\.\s*[0-9а-яa-z/\-]+)?(?:\s*стр\.\s*[0-9а-яa-z/\-]+)?)$",
            a,
            re.IGNORECASE
        )
        if m:
            street = self._clean_text(m.group(1))
            house = self._clean_text(m.group(2))
            return street, house

        return a, None

    def _looks_like_street(self, text: str) -> bool:
        if not text:
            return False

        text = text.lower().strip()

        street_markers = [
            "улица", "ул ", "ул.",
            "проспект", "пр-т", "пр-кт",
            "переулок", "пер ",
            "проезд",
            "шоссе",
            "бульвар", "б-р",
            "набережная", "наб.",
            "площадь",
            "аллея",
            "тупик",
        ]

        if any(marker in text for marker in street_markers):
            return True

        if re.search(r"[А-Яа-яA-Za-z0-9\-\s]+,\s*д\.\s*\d+", text, re.IGNORECASE):
            return True

        return False

    def _parse_address_from_url(self, url: str):
        m = re.search(r"/objects/([^/]+?)(?:_\d+)?/?$", url)
        if not m:
            return None

        slug = m.group(1).lower()
        slug = re.sub(r"_\d+$", "", slug)

        parts = slug.split("_")
        if not parts:
            return None

        bad_parts = {
            "m", "metro", "vystavochnaya", "park", "pobedy",
            "kievskaya", "otradnoe", "aeroport", "voljskaya"
        }

        replacements = {
            "moskva": "Москва",
            "kommunarka": "Коммунарка",
            "zelenograd": "Зеленоград",
            "ulitsa": "улица",
            "ulica": "улица",
            "prospekt": "проспект",
            "proezd": "проезд",
            "pereulok": "переулок",
            "shosse": "шоссе",
            "bulvar": "бульвар",
            "naberejnaya": "набережная",
            "ploschad": "площадь",
            "d": "д.",
            "k": "к.",
            "str": "стр.",
        }

        translit_map = {
            "trujenikov": "Тружеников",
            "rijskiy": "Рижский",
            "valdayskiy": "Валдайский",
            "bolshaya": "Большая",
            "marfinskaya": "Марфинская",
            "nijnie": "Нижние",
            "mnevniki": "Мнёвники",
            "senejskaya": "Сенежская",
            "svobody": "Свободы",
            "tihvinskaya": "Тихвинская",
            "vereskovaya": "Вересковая",
            "yasenevaya": "Ясеневая",
            "akademicheskaya": "Академическая",
            "krupskoy": "Крупской",
            "krylatskaya": "Крылатская",
            "lyublinskaya": "Люблинская",
            "mironovskaya": "Мироновская",
            "chobotovskaya": "Чоботовская",
            "ilimskaya": "Илимская",
            "sadovaya": "Садовая",
            "radialnaya": "Радиальная",
            "spasopeskovskiy": "Спасопесковский",
            "shmitovskiy": "Шмитовский",
            "setunskiy": "Сетуньский",
            "otkrytoe": "Открытое",
            "leningradskiy": "Ленинградский",
            "kutuzovskiy": "Кутузовский",
            "severnyy": "Северный",
            "bestujevyh": "Бестужевых",
            "oktyabrya": "Октября",
        }

        decoded = []
        for part in parts:
            if part in bad_parts:
                continue
            decoded.append(replacements.get(part, translit_map.get(part, part)))

        text = " ".join(decoded)

        text = re.sub(r"\b1-y\b", "1-й", text)
        text = re.sub(r"\b2-y\b", "2-й", text)
        text = re.sub(r"\b3-y\b", "3-й", text)
        text = re.sub(r"\b6-ya\b", "6-я", text)

        text = self._normalize_address(text)

        if not text or self._is_bad_short_address(text):
            return None

        if not any(marker in text.lower() for marker in [
            "улица", "проспект", "переулок", "проезд", "шоссе",
            "бульвар", "набережная", "площадь", "аллея"
        ]):
            return None

        return text

    def _parse_address_from_description(self, description: str):
        if not description:
            return None

        text = self._clean_text(description)

        patterns = [
            r"(?:по адресу|адрес)\s*:?\s*(г\.\s*москва,\s*[^\.]+?\d+[а-яА-Я0-9/\-]*)",
            r"(?:по адресу|адрес)\s*:?\s*(москва,\s*[^\.]+?\d+[а-яА-Я0-9/\-]*)",
            r"(г\.\s*москва,\s*(?:улица|ул\.?|проспект|пр-кт|пр-т|переулок|пер\.?|проезд|шоссе|бульвар|б-р|набережная|наб\.?)\s+[^\.]+?\d+[а-яА-Я0-9/\-]*)",
            r"(москва,\s*(?:улица|ул\.?|проспект|пр-кт|пр-т|переулок|пер\.?|проезд|шоссе|бульвар|б-р|набережная|наб\.?)\s+[^\.]+?\d+[а-яА-Я0-9/\-]*)",
            r"(?:расположен|расположена|расположенный|расположенного)\s+.*?(?:в|по адресу)\s*(москва,\s*[^\.]+?\d+[а-яА-Я0-9/\-]*)",
        ]

        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if not match:
                continue

            address = self._clean_text(match.group(1))
            address = re.sub(r"^г\.\s*", "", address, flags=re.IGNORECASE)
            address = re.sub(r",?\s*цена\s+[\d\s]+\s*руб.*$", "", address, flags=re.IGNORECASE)
            address = address.strip(" ,")

            address = self._normalize_address(address)

            if not address or self._is_bad_short_address(address):
                continue

            if not self._looks_like_street(address):
                continue

            return address

        return None

    def _parse_address_from_title_meta(self, soup: BeautifulSoup, title: str = ""):
        candidates = []

        if title:
            candidates.append(title)

        if soup.title and soup.title.string:
            candidates.append(self._clean_text(soup.title.string))

        for tag in soup.find_all("meta"):
            if tag.get("property") == "og:title":
                content = self._clean_text(tag.get("content", ""))
                if content:
                    candidates.append(content)

        cleaned_candidates = []
        for text in candidates:
            text = re.sub(
                r"^\d+-комнатная квартира,\s*[\d\.,]+\s*м²,\s*купить за\s*[\d\s]+\s*руб,?\s*",
                "",
                text,
                flags=re.IGNORECASE
            )
            text = re.sub(r"\|\s*Move\.Ru.*$", "", text, flags=re.IGNORECASE).strip(" ,")
            text = self._normalize_address(text)

            if not text or self._is_bad_short_address(text):
                continue

            if not self._looks_like_street(text):
                continue

            cleaned_candidates.append(text)

        if cleaned_candidates:
            cleaned_candidates = sorted(set(cleaned_candidates), key=len, reverse=True)
            return cleaned_candidates[0]

        return None

    def _parse_location_block(self, soup: BeautifulSoup):
        result = {
            "address": None,
            "okrug": None,
            "district": None,
        }

        links = soup.select("a.card-objects-location__address-link")
        text_node = soup.select_one("span.card-objects-location__address-text")

        if not links and not text_node:
            return result

        street = None
        house = self._clean_text(text_node.get_text(" ", strip=True)) if text_node else None

        okrug_list = {
            "ЦАО", "САО", "СВАО", "ВАО", "ЮВАО",
            "ЮАО", "ЮЗАО", "ЗАО", "СЗАО", "ЗелАО",
            "НМАО", "ТАО", "ТиНАО"
        }

        for node in links:
            text = self._clean_text(node.get_text(" ", strip=True))
            if not text:
                continue

            low = text.lower()

            if text in okrug_list:
                result["okrug"] = text
                continue

            if low.startswith("район "):
                district = self._clean_text(text[6:])
                if district and len(district) >= 3:
                    result["district"] = district
                continue

            if any(low.startswith(prefix) for prefix in [
                "ул ", "улица ", "пр-кт ", "проспект ",
                "пер ", "переулок ", "проезд ", "шоссе ",
                "б-р ", "бульвар ", "наб ", "набережная "
            ]):
                street = text
                continue

        if street and house:
            result["address"] = self._normalize_address(f"Москва, {street}, {house}")
        elif street:
            result["address"] = self._normalize_address(f"Москва, {street}")
        elif house and re.search(r"\d", house):
            result["address"] = self._normalize_address(f"Москва, {house}")

        if result["address"] and self._is_bad_short_address(result["address"]):
            result["address"] = None

        return result

    def _parse_address(self, soup: BeautifulSoup, main_text: str, title: str = "", description: str = "", url: str = ""):
        loc = self._parse_location_block(soup)
        if loc.get("address") and not self._is_bad_short_address(loc["address"]):
            return loc["address"]

        address = self._parse_address_from_description(description)
        if address and not self._is_bad_short_address(address):
            return address

        address = self._parse_address_from_description(main_text)
        if address and not self._is_bad_short_address(address):
            return address

        address = self._parse_address_from_title_meta(soup, title)
        if address and not self._is_bad_short_address(address):
            return address

        address = self._parse_address_from_url(url)
        if address and not self._is_bad_short_address(address):
            return address

        return None

    # OTHER FIELDS

    def _parse_okrug(self, text: str):
        match = re.search(
            r"\b(ЦАО|САО|СВАО|ВАО|ЮВАО|ЮАО|ЮЗАО|ЗАО|СЗАО|ЗелАО|НМАО|ТАО|ТиНАО)\b",
            text
        )
        return match.group(1) if match else None

    def _parse_district(self, text: str):
        if not text:
            return None

        text = self._clean_text(text)

        bad_exact = {
            "ЦАО", "САО", "СВАО", "ВАО", "ЮВАО", "ЮАО",
            "ЮЗАО", "ЗАО", "СЗАО", "ЗелАО", "НМАО", "ТАО", "ТиНАО",
        }

        bad_contains = [
            "отлично подходит",
            "камерный",
            "бц",
            "бизнес",
            "класс",
            "инфраструктура",
            "школ",
            "детск",
            "продается",
            "жилой комплекс",
            "оставьте заявку",
            "видовые квартиры",
            "премиальная жизнь",
            "район с развитой инфраструктурой",
        ]

        patterns = [
            r"\bрайон\s+([А-ЯЁ][а-яё]+(?:[-\s][А-ЯЁа-яё]+){0,2})\b",
            r"\b([А-ЯЁ][а-яё]+(?:[-\s][А-ЯЁа-яё]+){0,2})\s+район\b",
        ]

        for pattern in patterns:
            for match in re.finditer(pattern, text, re.IGNORECASE):
                district = self._clean_text(match.group(1))
                district = re.sub(r"^(район)\s+", "", district, flags=re.IGNORECASE)

                if not district:
                    continue
                if district in bad_exact:
                    continue
                if len(district) < 3 or len(district) > 30:
                    continue
                if any(x in district.lower() for x in bad_contains):
                    continue
                if re.search(r"\d", district):
                    continue

                words = district.split()
                if len(words) > 3:
                    continue

                return district

        return None

    def _parse_metro(self, text: str):
        if not text:
            return None, None

        text = self._clean_text(text)

        patterns = [
            r"до метро\s+[«\"]?([А-ЯЁа-яёA-Za-z0-9\-\s]+?)[»\"]?\s+(\d+)\s*мин",
            r"до ст\.?м\.?\s*[«\"]?([А-ЯЁа-яёA-Za-z0-9\-\s]+?)[»\"]?\s+(\d+)\s*мин",
            r"ст\.?м\.?\s*[«\"]?([А-ЯЁа-яёA-Za-z0-9\-\s]+?)[»\"]?\s+(\d+)\s*мин",
            r"метро\s+[«\"]?([А-ЯЁа-яёA-Za-z0-9\-\s]+?)[»\"]?\s+(\d+)\s*мин",
            r"(\d+)\s*мин(?:ут)?\s*пешком\s*до метро\s+[«\"]?([А-ЯЁа-яёA-Za-z0-9\-\s]+?)[»\"]?(?:[.,]|$)",
            r"(\d+)\s*мин(?:ут)?\s*на транспорте\s*до метро\s+[«\"]?([А-ЯЁа-яёA-Za-z0-9\-\s]+?)[»\"]?(?:[.,]|$)",
        ]

        for i, pattern in enumerate(patterns):
            match = re.search(pattern, text, re.IGNORECASE)
            if not match:
                continue

            if i in [0, 1, 2, 3]:
                metro = self._clean_text(match.group(1))
                metro_time = self._safe_int(match.group(2))
            else:
                metro_time = self._safe_int(match.group(1))
                metro = self._clean_text(match.group(2))

            metro = metro.strip(" ,.-–—:;")
            metro = re.sub(r"\s+", " ", metro)

            if len(metro) < 3:
                continue
            if len(metro.split()) > 4:
                continue

            bad_words = [
                "коммерческая", "офисы", "склады", "новостройки", "квартиры",
                "каталог", "жк", "подбор", "бизнес-класс", "элитные"
            ]
            if any(word in metro.lower() for word in bad_words):
                continue

            return metro, metro_time

        return None, None

    def _parse_metro_from_soup(self, soup: BeautifulSoup):
        station_blocks = soup.select("li.card-objects-near-stations__station")
        if not station_blocks:
            station_blocks = soup.select("div.card-objects-near-stations__station")

        for block in station_blocks:
            station_name = None
            station_time = None

            name_node = (
                block.select_one("a.card-objects-near-stations__station-link")
                or block.select_one("span.card-objects-near-stations__station-link")
                or block.select_one(".card-objects-near-stations__station-name")
            )

            time_node = (
                block.select_one(".card-objects-near-stations__station-duration")
                or block.select_one(".card-objects-near-stations__station-time")
            )

            if name_node:
                station_name = self._clean_text(name_node.get_text(" ", strip=True))

            if time_node:
                m = re.search(r"(\d+)", time_node.get_text(" ", strip=True))
                if m:
                    station_time = self._safe_int(m.group(1))

            if station_name and len(station_name) >= 3:
                return station_name, station_time

        return None, None

    def _parse_seller_type(self, description: str):
        text_lower = description.lower()

        if "собственник" in text_lower:
            return "owner"
        if "агент" in text_lower or "агентство" in text_lower or "риелтор" in text_lower:
            return "agent"
        if "застройщик" in text_lower:
            return "developer"

        return None

    def _parse_listing_subtype(self, description: str, title: str):
        text = f"{title or ''} {description or ''}".lower()

        if "апартам" in text:
            return "apartments"
        if "пентхаус" in text:
            return "penthouse"
        if "евродвушк" in text or "евротрешк" in text:
            return "euro_format"
        return "flat"

    # FEATURES

    def _extract_is_studio(self, title: str, description: str):
        text = f"{title or ''} {description or ''}".lower()
        return 1 if "студия" in text else 0

    def _extract_has_balcony(self, description: str):
        text = description.lower()
        return 1 if any(word in text for word in ["балкон", "лоджия"]) else 0

    def _extract_has_elevator(self, description: str):
        return 1 if "лифт" in description.lower() else 0

    def _extract_has_parking(self, description: str):
        text = description.lower()
        return 1 if any(word in text for word in ["парковка", "паркинг", "машино-мест"]) else 0

    def _extract_has_renovation(self, text: str):
        text = text.lower()

        if any(word in text for word in [
            "евроремонт",
            "дизайнерский ремонт",
            "косметический ремонт",
            "с ремонтом",
            "после ремонта",
            "качественный ремонт",
        ]):
            return 1

        if any(word in text for word in [
            "без ремонта",
            "без отделки",
            "черновая отделка",
            "white box",
            "whitebox",
            "под отделку",
            "голый бетон",
            "требует ремонта",
        ]):
            return 0

        return None

    def _build_features_text(
        self,
        rooms,
        area_total,
        floor,
        total_floors,
        house_type,
        metro,
        metro_time_min,
        district,
        okrug,
        has_renovation,
        seller_type
    ) -> str:
        parts = []

        if rooms is not None:
            if rooms == 0:
                parts.append("студия")
            else:
                parts.append(f"{rooms}-комнатная квартира")

        if area_total is not None:
            parts.append(f"{area_total} м2")

        if floor is not None and total_floors is not None:
            parts.append(f"{floor}/{total_floors} этаж")

        if house_type:
            parts.append(house_type)

        if district:
            parts.append(f"район {district}")

        if okrug:
            parts.append(okrug)

        if metro:
            if metro_time_min is not None:
                parts.append(f"метро {metro}, {metro_time_min} мин")
            else:
                parts.append(f"метро {metro}")

        if has_renovation == 1:
            parts.append("с ремонтом")
        elif has_renovation == 0:
            parts.append("без ремонта")

        if seller_type:
            parts.append(f"продавец {seller_type}")

        return ", ".join(parts)

    # VALIDATION

    def _validate_item(self, item: dict):
        errors = []

        if item.get("price") is None or item.get("price") <= 0:
            errors.append("bad_price")

        if item.get("area_total") is None or item.get("area_total") <= 0:
            errors.append("bad_area")

        if item.get("price") and item.get("area_total"):
            calc_ppm2 = round(item["price"] / item["area_total"], 2)
            if item.get("price_per_m2") is not None and abs(calc_ppm2 - item["price_per_m2"]) > 5:
                errors.append("price_per_m2_mismatch")

        if item.get("floor") and item.get("total_floors"):
            if item["floor"] > item["total_floors"]:
                errors.append("floor_gt_total_floors")

        if item.get("year_built") and not (1800 <= item["year_built"] <= 2035):
            errors.append("invalid_year_built")

        if item.get("address") and len(item["address"]) < 5:
            errors.append("address_too_short")

        if not item.get("address"):
            errors.append("no_address")

        if item.get("rooms") is not None and item["rooms"] > 20:
            errors.append("suspicious_rooms")

        if item.get("district") and len(item["district"]) > 40:
            errors.append("bad_district")

        if item.get("address") and self._is_bad_short_address(item["address"]):
            errors.append("bad_address")

        return errors

    def _quality_score(self, item: dict) -> int:
        score = 100

        important_fields = [
            "price", "area_total", "address", "rooms", "metro",
            "floor", "total_floors", "district", "description_clean"
        ]

        for field in important_fields:
            if not item.get(field):
                score -= 8

        if item.get("photo_count", 0) == 0:
            score -= 10
        elif item.get("photo_count", 0) < 3:
            score -= 5

        score -= min(len(item.get("parse_errors", [])) * 10, 40)

        return max(0, score)

    # MAIN PARSER

    def parse_item_details(self, url: str):
        soup = self.get_soup(url)
        if not soup:
            return None

        html = str(soup)
        main_text = self._extract_main_text(soup)
        title_raw = self._parse_title(soup)
        title_clean = self._parse_title_clean(title_raw)

        spec_cards = self._extract_spec_cards(soup)

        description_raw = self._extract_description_from_soup(soup)
        if not description_raw:
            description_raw = self._extract_description(main_text)

        description_clean = self._normalize_text_for_rag(description_raw)

        json_ld_text = self._extract_json_ld_text(soup)
        script_text = self._extract_script_text(soup)

        listing_id = self._parse_listing_id(url)
        price = self._parse_price(main_text)
        rooms = self._parse_rooms(title_raw, main_text, spec_cards)
        area_total = self._parse_area(title_raw, main_text, spec_cards)
        area_kitchen = self._parse_kitchen_area(main_text, spec_cards)
        area_living = self._parse_living_area(main_text, spec_cards)
        floor, total_floors = self._parse_floor_info(main_text, spec_cards)

        full_text_for_features = " ".join([
            main_text or "",
            description_raw or "",
            " ".join(spec_cards.values()) if spec_cards else "",
            json_ld_text or "",
            script_text or "",
        ])

        year_source_text = " ".join([
            description_raw or "",
            " ".join(spec_cards.values()) if spec_cards else "",
            json_ld_text or "",
        ])

        house_type = self._parse_house_type(full_text_for_features)
        year_built, completion_year = self._parse_year_fields(year_source_text, spec_cards)

        address = self._parse_address(
            soup,
            main_text,
            title=title_raw,
            description=description_raw,
            url=url,
        )

        street, house_number = self._split_address(address)

        location_data = self._parse_location_block(soup)
        district = location_data.get("district") or self._parse_district(full_text_for_features)
        okrug = location_data.get("okrug") or self._parse_okrug(full_text_for_features)

        metro, metro_time_min = self._parse_metro_from_soup(soup)
        if metro is None:
            metro, metro_time_min = self._parse_metro(description_raw)
        if metro is None:
            metro, metro_time_min = self._parse_metro(main_text)

        seller_type = self._parse_seller_type(description_raw)
        listing_subtype = self._parse_listing_subtype(description_raw, title_raw)

        # PHOTO PARSING

        gallery_photos = self._extract_photos_from_gallery(soup, listing_id=listing_id)
        regex_photos = self._extract_all_photos(html, listing_id=listing_id)

        photos = []
        seen = set()

        for img_url in gallery_photos + regex_photos:
            if not self._is_real_photo(img_url):
                continue

            key = self._photo_dedupe_key(img_url)
            if not key or key in seen:
                continue

            seen.add(key)
            photos.append(self._normalize_photo_url(img_url))

        if not photos:
            og = soup.find("meta", property="og:image")
            if og and og.get("content"):
                og_url = self._normalize_photo_url(og["content"])
                if self._is_real_photo(og_url):
                    photos = [og_url]

        photo_count = len(photos)
        cover_image_url = photos[0] if photos else None

        price_per_m2 = None
        if price is not None and area_total not in (None, 0):
            price_per_m2 = round(price / area_total, 2)

        renovation_source = " ".join([title_raw or "", description_raw or "", main_text or ""])

        has_renovation = self._extract_has_renovation(renovation_source)
        has_balcony = self._extract_has_balcony(description_raw)
        has_elevator = self._extract_has_elevator(description_raw)
        has_parking = self._extract_has_parking(description_raw)
        is_studio = self._extract_is_studio(title_raw, description_raw)

        is_new_building = None
        if completion_year is not None:
            is_new_building = 1
        elif year_built is not None and year_built >= 2020:
            is_new_building = 1
        elif year_built is not None:
            is_new_building = 0

        features_text = self._build_features_text(
            rooms=rooms,
            area_total=area_total,
            floor=floor,
            total_floors=total_floors,
            house_type=house_type,
            metro=metro,
            metro_time_min=metro_time_min,
            district=district,
            okrug=okrug,
            has_renovation=has_renovation,
            seller_type=seller_type,
        )

        item = {
            "url": url,
            "source": self.source,
            "listing_id": listing_id,
            "parsed_at": datetime.utcnow().isoformat(),

            "deal_type": "sale",
            "object_type": "apartment",
            "is_apartment": 1,
            "listing_subtype": listing_subtype,

            "currency": "RUB",
            "price": price,
            "price_per_m2": price_per_m2,

            "rooms": rooms,
            "is_studio": is_studio,
            "area_total": area_total,
            "area_kitchen": area_kitchen,
            "area_living": area_living,
            "floor": floor,
            "total_floors": total_floors,

            "address_raw": address,
            "address": address,
            "street": street,
            "house_number": house_number,
            "okrug": okrug,
            "district": district,

            "metro": metro,
            "metro_time_min": metro_time_min,

            "house_type": house_type,
            "year_built": year_built,
            "completion_year": completion_year,
            "is_new_building": is_new_building,

            "seller_type": seller_type,

            "has_balcony": has_balcony,
            "has_elevator": has_elevator,
            "has_parking": has_parking,
            "has_renovation": has_renovation,

            "title_raw": title_raw,
            "title_clean": title_clean,
            "description_raw": description_raw,
            "description_clean": description_clean,
            "features_text": features_text,

            "photo_count": photo_count,
            "cover_image_url": cover_image_url,
            "image_urls": photos,
        }

        parse_errors = self._validate_item(item)
        item["parse_errors"] = parse_errors
        item["quality_score"] = self._quality_score(item)
        item["is_valid_listing"] = 1 if len(parse_errors) == 0 else 0

        return item