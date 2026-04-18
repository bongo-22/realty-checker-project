import pandas as pd

df = pd.read_csv("data/intermediate/move_ru_listings_with_s3.csv")

total = len(df)
with_cover = df["cover_image_s3_uri"].notna().sum()
without_cover = df["cover_image_s3_uri"].isna().sum()

print("Всего:", total)
print("С картинками:", with_cover)
print("Без картинок:", without_cover)
print("Доля с картинками:", round(with_cover / total * 100, 2), "%")