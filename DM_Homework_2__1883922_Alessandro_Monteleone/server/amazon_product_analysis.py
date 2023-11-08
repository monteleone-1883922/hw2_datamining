import json

import pandas as pd
from typing import *
from utils_and_classes import SentencePreprocessing, STOPWORDS_FILE_PATH, SPECIAL_CHARACTERS_FILE_PATH, INDEX_FILE_PATH, \
    CATEGORIES
import heapq as h


def load_data() -> pd.DataFrame:
    return pd.read_csv("../data/amazon_products_gpu.tsv", sep="\t")


def top10(df: pd.DataFrame, column: str, numeric: bool = True, higher: bool = True) -> pd.DataFrame:
    if numeric:
        df[column] = df[column].apply(lambda x: x.replace(".", "").replace(",", ".") if pd.notna(x) else x)
        df[column] = df[column].astype(float)
    return df.sort_values(by=column, ascending=not higher).head(10)


def build_heap_cos_similarity(query, inverted_index, documents_normas):
    cos_similarity_heap = []
    h.heapify(cos_similarity_heap)
    documents_similarities = {}
    for word in query:
        documents_containing_word = inverted_index[word]
        for document in documents_containing_word:
            documents_similarities[document[0]] = documents_similarities.get(document[0], 0) + document[1] / \
                                                  documents_normas[document[0]][1]
    for document in documents_similarities:
        h.heappush(cos_similarity_heap, documents_similarities[document])
    return cos_similarity_heap


def price_range_for_categories(description_and_price, categories):
    categories_price_range = {category: [-1, -1] for category in categories}
    description_and_price["price"] = description_and_price["price"].apply(
        lambda x: x.replace(".", "").replace(",", ".") if pd.notna(x) else x)
    description_and_price["price"] = description_and_price["price"].astype(float)
    for index, row in description_and_price.iterrows():
        description = row["description"]
        price = float(row["price"])
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


def retrieve_index(index_path):
    with open(index_path, "r") as index_file:
        data = json.load(index_file)
    return data["inverted_index"], data["documents_TFIDF"]


def compute_weigthed_ratings(pd):
    pass

class QueryProcessor():

    def __init__(self, inverted_index, documents_normas):
        self.inverted_index = inverted_index
        self.documents_normas = documents_normas

    def query_process(self, query: str, preprocessor: SentencePreprocessing = None):
        if preprocessor is None:
            preprocessor = SentencePreprocessing(STOPWORDS_FILE_PATH, SPECIAL_CHARACTERS_FILE_PATH)
        query_tokenized = preprocessor.remove_stopwords(query)
        similarity_heap = build_heap_cos_similarity(query_tokenized, self.inverted_index, self.documents_normas)


def test():
    pd = load_data()
    inv_indx, norm_indx = retrieve_index(INDEX_FILE_PATH)
    with open("test_file.txt", "w") as f:
        for key in inv_indx.keys():
            f.write(str(key) + "\n")


if __name__ == "__main__":
    test()
