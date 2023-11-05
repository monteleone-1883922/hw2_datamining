import json
from nltk.tokenize import word_tokenize
import bisect as bi
from math import log2,log10,sqrt
from amazon_product_analysis import load_data
import nltk
from typing import *
import pandas as pd

STOPWORDS_FILE_PATH = "data/stopwords_list_it.json"
SPECIAL_CHARACTERS_FILE_PATH = "data/special_characters.json"
INDEX_FILE_PATH = "data/indexes.json"

class SentencePreprocessing():


    def __init__(self, stopwords_file_path : str, special_characters_file_path : str):
        #nltk.download('punkt')
        with open(stopwords_file_path, 'r') as stopwords_file:
            data = json.load(stopwords_file)
        self.stopwords = set(data["words"])
        with open(special_characters_file_path, 'r') as special_characters_file:
            data = json.load(special_characters_file)
        self.special_characters = set(data["special_characters"])


    def remove_stopwords(self,words : list[str]) -> list[str]:
        result = []
        for word in words:
            if word.lower() not in self.stopwords and word not in self.special_characters:
                result.append(word.lower())
        return result
    
    def remove_special_characters(self,words : list[str]) -> list[str]:
        result = []
        for word in words:
            if word not in self.special_characters:
                result.append(word.lower())
        return result
    
    def preprocess(self,sentence : str, remove_stopwords : bool = True) -> list[str]:
        tokenized = word_tokenize(sentence)
        return self.remove_stopwords(tokenized) if remove_stopwords else self.remove_special_characters(tokenized)
    

    
def build_index(corpus : list[str], preprocessor : SentencePreprocessing = None) -> tuple[dict[str,list[tuple[int,int]]],list[dict[str,int]]]:
    inverted_index = {}
    normal_index = []
    if preprocessor is None:
        preprocessor = SentencePreprocessing(STOPWORDS_FILE_PATH,SPECIAL_CHARACTERS_FILE_PATH)
    for idx,el in enumerate(corpus):
        words_in_el = preprocessor.preprocess(el)
        counted_words = {}
        for word in words_in_el:
            counted_words[word] = counted_words.get(word,0) + 1
        normal_index.append(counted_words)
        for word in counted_words.keys():
            inverted_index[word] = inverted_index.get(word,[]) + [(idx,counted_words[word])]       #bi.insort_left(inverted_index.get(word,[]),(idx,counted_words[word]), key=lambda x : x[0])
    return inverted_index, normal_index



def compute_documents_norma(inverted_index : dict[str,list[tuple[int,int]]],normal_index : list[dict[str,int]]) -> list[dict[str,tuple[int,float]]]:
    documents_TFIDF = []
    for i in range(len(normal_index)):
        tot = 0
        for word in normal_index[i].keys():
            tot += compute_TFIDF(inverted_index,normal_index,i,word)**2
        documents_TFIDF.append((normal_index[i][word],sqrt(tot)))
    return documents_TFIDF


def compute_TFIDF(inverted_index : dict[str,list[tuple[int,int]]],normal_index : list[dict[str,int]],document : int,word : str) -> float:
    
    idf = log2(len(normal_index)/len(inverted_index[word]))
    tf = normal_index[document][word]
    return tf * idf


def store_indexes(inverted_index : dict[str,list[tuple[int,int]]], documents_TFIDF : list[dict[str,tuple[int,float]]], index_file_path : str) -> None:
    indexes = {"inverted_index": inverted_index, "documents_TFIDF" : documents_TFIDF}
    
    with open(index_file_path, "w") as index_file:
        json.dump(indexes, index_file, indent=4)


def extract_descriptions_as_list(df : pd.DataFrame) -> list[str]:
    return df["description"].tolist()



def build_amazon_products_description_index() -> None:
    df = load_data()
    corpus = extract_descriptions_as_list(df)
    inverted_index, normal_index = build_index(corpus)
    print(list(inverted_index.keys()))
    normal_index = compute_documents_norma(inverted_index, normal_index)
    store_indexes(inverted_index,normal_index,INDEX_FILE_PATH)


def main():
    build_amazon_products_description_index()

if __name__ == "__main__":
    main()
        
        






