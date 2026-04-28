import pandas as pd

from sentence_transformers import SentenceTransformer


DATA_PATH = "data/processed/move_ru_clean.parquet"
OUTPUT_PATH = "data/processed/move_ru_with_embeddings.parquet"


def main():
    print("[*] Читаю данные")
    data = pd.read_parquet(DATA_PATH)

    print("[*] Загружаю модель эмбеддингов")
    model = SentenceTransformer("sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2")

    texts = data["description"].fillna("").astype(str).tolist()

    print("[*] Строю эмбеддинги")
    embeddings = model.encode(
        texts,
        batch_size=64,
        show_progress_bar=True,
        normalize_embeddings=True,
    )

    data["description_embedding"] = embeddings.tolist()

    print("[*] Сохраняю датасет с эмбеддингами")
    data.to_parquet(OUTPUT_PATH, index=False)

    print("[+] Готово:", OUTPUT_PATH)


if __name__ == "__main__":
    main()