
import pandas as pd
from typing import *
from utils_and_classes import SentencePreprocessing,STOPWORDS_FILE_PATH,SPECIAL_CHARACTERS_FILE_PATH
import heapq as h

def load_data() -> pd.DataFrame:
    return pd.read_csv("data/amazon_products_gpu.tsv",sep="\t")

def top10(df,column, numeric = True, higher = True) -> pd.DataFrame:
    if numeric:
        df[column] = df[column].str.replace(".","").str.replace(",",".").astype(float)
    return df.sort_values(by=column, ascending=not higher).head(10)


def build_heap_cos_similarity(query,inverted_index,documents_normas):
    cos_similarity_heap = []
    h.heapify(cos_similarity_heap)
    documents_similarities = {}
    for word in query:
        documents_containing_word = inverted_index[word]
        for document in documents_containing_word:
            documents_similarities[document[0]] = documents_similarities.get(document[0],0) + document[1] / documents_normas[document[0]][1]
    for document in documents_similarities:
        h.heappush(cos_similarity_heap,documents_similarities[document])
    return cos_similarity_heap


class QueryProcessor():

    def __init__(self,inverted_index,documents_normas):
        self.inverted_index = inverted_index
        self.documents_normas = documents_normas
        

    def query_process(self, query : str, preprocessor : SentencePreprocessing = None):
        if preprocessor is None:
            preprocessor = SentencePreprocessing(STOPWORDS_FILE_PATH,SPECIAL_CHARACTERS_FILE_PATH)
        query_tokenized = preprocessor.remove_stopwords(query)
        similarity_heap = build_heap_cos_similarity(query_tokenized,self.inverted_index, self.documents_normas)





def test():
    pass





if __name__ == "__main__":
    test()