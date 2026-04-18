import pandera.pandas as pa
from pandera import Check


LISTINGS_SCHEMA = pa.DataFrameSchema(
    {
        "url": pa.Column(str, nullable=False),
        "listing_id": pa.Column(str, nullable=False),

        "title": pa.Column(str, nullable=True),
        "deal_type": pa.Column(str, nullable=False, checks=Check.isin(["sale"])),
        "object_type": pa.Column(str, nullable=False, checks=Check.isin(["apartment"])),

        "price": pa.Column(float, nullable=False, checks=Check.in_range(500_000, 500_000_000)),
        "rooms": pa.Column(float, nullable=True, checks=Check.in_range(0, 10)),
        "area_total": pa.Column(float, nullable=False, checks=Check.in_range(10, 500)),
        "area_kitchen": pa.Column(float, nullable=True, checks=Check.in_range(2, 100)),
        "area_living": pa.Column(float, nullable=True, checks=Check.in_range(5, 400)),
        "price_per_m2": pa.Column(float, nullable=True, checks=Check.in_range(30_000, 10_000_000)),

        "address": pa.Column(str, nullable=True),
        "okrug": pa.Column(str, nullable=True),
        "district": pa.Column(str, nullable=True),
        "metro": pa.Column(str, nullable=True),
        "metro_time_min": pa.Column(float, nullable=True, checks=Check.in_range(0, 300)),

        "floor": pa.Column(float, nullable=True, checks=Check.in_range(1, 150)),
        "total_floors": pa.Column(float, nullable=True, checks=Check.in_range(1, 200)),

        "house_type": pa.Column(str, nullable=True),

        "year_built": pa.Column(float, nullable=True, checks=Check.in_range(1800, 2035)),
        "completion_year": pa.Column(float, nullable=True, checks=Check.in_range(2000, 2035)),

        "seller_type": pa.Column(str, nullable=True),

        "photo_count": pa.Column(float, nullable=True, checks=Check.in_range(0, 500)),
        "image_urls": pa.Column(str, nullable=True),

        "is_studio": pa.Column(float, nullable=True, checks=Check.isin([0, 1])),
        "has_balcony": pa.Column(float, nullable=True, checks=Check.isin([0, 1])),
        "has_elevator": pa.Column(float, nullable=True, checks=Check.isin([0, 1])),
        "has_parking": pa.Column(float, nullable=True, checks=Check.isin([0, 1])),
        "has_renovation": pa.Column(float, nullable=True, checks=Check.isin([0, 1])),

        "description": pa.Column(str, nullable=True),
        "parse_errors": pa.Column(str, nullable=True),
    },
    checks=[
        Check(
            lambda df: (df["floor"].isna()) | (df["total_floors"].isna()) | (df["floor"] <= df["total_floors"]),
            error="floor must be <= total_floors",
        ),
        Check(
            lambda df: (df["area_living"].isna()) | (df["area_total"].isna()) | (df["area_living"] <= df["area_total"]),
            error="area_living must be <= area_total",
        ),
        Check(
            lambda df: (df["area_kitchen"].isna()) | (df["area_total"].isna()) | (df["area_kitchen"] <= df["area_total"]),
            error="area_kitchen must be <= area_total",
        ),
    ],
    strict=True,
    coerce=True,
)