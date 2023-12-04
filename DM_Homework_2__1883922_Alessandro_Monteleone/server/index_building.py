import json
from math import log2, sqrt
import pandas as pd
from amazon_product_analysis import load_data
from utils_and_classes import SentencePreprocessing, STOPWORDS_FILE_PATH, SPECIAL_CHARACTERS_FILE_PATH, INDEX_FILE_PATH


def build_index(corpus: list[str], preprocessor: SentencePreprocessing = None) -> tuple[
        dict[str, list[tuple[int, int]]], list[dict[str, int]]]:
    inverted_index = {}
    normal_index = []
    if preprocessor is None:
        preprocessor = SentencePreprocessing(STOPWORDS_FILE_PATH, SPECIAL_CHARACTERS_FILE_PATH)
    for idx, el in enumerate(corpus):
        words_in_el = preprocessor.preprocess(el)
        counted_words = {}
        for word in words_in_el:
            counted_words[word] = counted_words.get(word, 0) + 1
        normal_index.append(counted_words)
        for word in counted_words.keys():
            inverted_index[word] = inverted_index.get(word, []) + [(idx, counted_words[
                word])]  # bi.insort_left(inverted_index.get(word,[]),(idx,counted_words[word]), key=lambda x : x[0])
    return inverted_index, normal_index


def compute_documents_norma(inverted_index: dict[str, list[tuple[int, int]]], normal_index: list[dict[str, int]]) -> \
        list[float]:
    documents_TFIDF = []
    for i in range(len(normal_index)):
        tot = 0
        for word in normal_index[i].keys():
            tot += compute_TFIDF(inverted_index, normal_index, i, word) ** 2
        documents_TFIDF.append(sqrt(tot))
    return documents_TFIDF


def compute_TFIDF(inverted_index: dict[str, list[tuple[int, int]]], normal_index: list[dict[str, int]], document: int,
                  word: str) -> float:
    idf = log2(len(normal_index) / len(inverted_index[word]))
    tf = normal_index[document][word]
    return tf * idf


def store_indexes(inverted_index: dict[str, list[tuple[int, int]]], documents_TFIDF: list[float],
                  index_file_path: str) -> None:
    indexes = {"inverted_index": inverted_index, "documents_TFIDF": documents_TFIDF}

    with open(index_file_path, "w") as index_file:
        json.dump(indexes, index_file, indent=4)


def extract_descriptions_as_list(df: pd.DataFrame) -> list[str]:
    return df["description"].tolist()


def build_amazon_products_description_index() -> None:
    df = load_data()
    corpus = extract_descriptions_as_list(df)
    inverted_index, normal_index = build_index(corpus)
    print(list(inverted_index.keys()))
    normal_index = compute_documents_norma(inverted_index, normal_index)
    store_indexes(inverted_index, normal_index, INDEX_FILE_PATH)


def main():
    build_amazon_products_description_index()


if __name__ == "__main__":
    main()
