import heapq as h
import json
from math import log2

import pandas as pd

from utils_and_classes import SentencePreprocessing, DATA_FILE_PATH, INDEX_FILE_PATH


def load_data() -> pd.DataFrame:
    return pd.read_csv(DATA_FILE_PATH, sep="\t")


def load_and_retype_data() -> pd.DataFrame:
    data = load_data()
    retype_dataframe(data)
    return data


def retype_dataframe(df: pd.DataFrame) -> None:
    df["price"] = df["price"].apply(lambda x: x.replace(".", "").replace(",", ".") if pd.notna(x) else x)
    df["price"] = df["price"].astype(float)
    df["stars"] = df["stars"].apply(lambda x: x.replace(",", ".") if pd.notna(x) else x)
    df["stars"] = df["stars"].astype(float)
    df["num_reviews"] = df["num_reviews"].astype(str)
    df["num_reviews"] = df["num_reviews"].apply(lambda x: x.replace(".", "") if x != "nan" else "-1")
    df["num_reviews"] = df["num_reviews"].astype(int)
    df["num_reviews"] = df["num_reviews"].apply(lambda x: pd.NA if x == -1 else x)
    df["prime"] = df["prime"].astype(bool)


def topN(df: pd.DataFrame, column: str, n: int, higher: bool = True) -> pd.DataFrame:
    return df.sort_values(by=column, ascending=not higher).head(n)


def build_heap_cos_similarity(query: list[str], inverted_index: dict[str, list[tuple[int, int]]],
                              documents_normas: list[float]) -> list[tuple[float, int]]:
    cos_similarity_heap = []
    h.heapify(cos_similarity_heap)
    documents_similarities = {}
    for word in query:
        documents_containing_word = inverted_index.get(word, [])
        for document in documents_containing_word:
            documents_similarities[document[0]] = documents_similarities.get(document[0], 0) + document[1] / \
                                                  documents_normas[document[0]]
    for document in documents_similarities.items():
        h.heappush(cos_similarity_heap, (-document[1], document[0]))
    return cos_similarity_heap


def get_query_result(df: pd.DataFrame, heap: list[tuple[float, int]], n: int = -1) -> pd.DataFrame:
    result_of_query = []
    num_results = len(heap) if n == -1 or n > len(heap) else n
    for _ in range(num_results):
        result_of_query.append(h.heappop(heap)[1])
    return df.loc[result_of_query]


def price_range_for_categories(description_and_price: pd.DataFrame, categories: list[str]) -> pd.DataFrame:
    categories_price_range = {category: [-1, -1] for category in categories}

    for index, row in description_and_price.iterrows():
        description = row["description"]
        price = row["price"]
        for category in categories:
            if category in description:
                categories_price_range[category][0] = price if not pd.isna(price) and (
                        categories_price_range[category][0] == -1 or categories_price_range[category][
                    0] > price) else categories_price_range[category][0]
                categories_price_range[category][1] = max(categories_price_range[category][1], price)
                break
    categories_price_range_clean = {
        "categories": [category for category in categories if categories_price_range[category][0] != -1],
        "min price": [categories_price_range[category][0] for category in categories if
                      categories_price_range[category][0] != -1],
        "max price": [categories_price_range[category][1] for category in categories if
                      categories_price_range[category][0] != -1]
    }
    return pd.DataFrame(categories_price_range_clean)


def retrieve_index(index_path: str) -> tuple[dict[str, list[tuple[int, int]]], list[float]]:
    with open(index_path, "r") as index_file:
        data = json.load(index_file)
    return data["inverted_index"], data["documents_TFIDF"]


def compute_weighted_ratings(df: pd.DataFrame) -> None:
    weighted_ratings = [row["stars"] *
                        log2(row["num_reviews"])
                        if pd.notna(row["stars"]) and pd.notna(row["num_reviews"])
                        else row["stars"] for index, row in df.iterrows()]
    df["weighted_ratings"] = pd.Series(weighted_ratings)


def analyze_primeness(df: pd.DataFrame) -> pd.DataFrame:
    prices = df["price"].dropna()
    stars = df["stars"].dropna()
    avg_price = round(prices.mean(), 2)
    avg_stars = round(stars.mean(), 2)
    prime_products = df[df["prime"]]
    prime_prices = prime_products["price"].dropna()
    prime_stars = prime_products["stars"].dropna()
    avg_price_prime = round(prime_prices.mean(), 2)
    avg_stars_prime = round(prime_stars.mean(), 2)
    return pd.DataFrame(
        {"average price": [avg_price], "average price prime products": [avg_price_prime], "average stars": [avg_stars],
         "average stars prime products": [avg_stars_prime]})


class QueryProcessor():

    def __init__(self, index_file_path: str, stopwords_file_path: str, special_characters_file_path: str):
        self.inverted_index, self.documents_normas = retrieve_index(index_file_path)
        self.preprocessor = SentencePreprocessing(stopwords_file_path, special_characters_file_path)

    def query_process(self, query: str) -> list[tuple[float, int]]:
        query_tokenized = self.preprocessor.preprocess(query.lower())
        similarity_heap = build_heap_cos_similarity(query_tokenized, self.inverted_index, self.documents_normas)
        return similarity_heap


def test():
    pd = load_data()
    inv_indx, norm_indx = retrieve_index(INDEX_FILE_PATH)
    with open("test_file.txt", "w") as f:
        for key in inv_indx.keys():
            f.write(str(key) + "\n")


if __name__ == "__main__":
    test()
