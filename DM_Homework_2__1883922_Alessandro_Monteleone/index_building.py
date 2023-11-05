import json
from nltk.tokenize import word_tokenize
import bisect as bi
from math import log2,log10,sqrt
from amazon_product_analysis import load_data

STOPWORDS_FILE_PATH = "/data/stopwords_list.json"
INDEX_FILE_PATH = "data/indexes.json"

class SentencePreprocessing():


    def __init__(self, stopwords_file_path):
        with open(stopwords_file_path, 'r') as stopwords_file:
            data = json.load(stopwords_file)
            self.stopwords = set(data["words"])

    def remove_stopwords(self,words):
        result = []
        for word in words:
            if word not in self.stopwords:
                result.append(word)
        return result
    
    def preprocess(self,sentence):
        tokenized = word_tokenize(sentence)
        return self.remove_stopwords(tokenized)
    

    
def build_index(corpus, preprocessor = None):
    inverted_index = {}
    normal_index = []
    if preprocessor is None:
        preprocessor = SentencePreprocessing(STOPWORDS_FILE_PATH)
    for idx,el in enumerate(corpus):
        words_in_el = preprocessor.preprocess(el)
        counted_words = {}
        for word in words_in_el:
            counted_words[word] = counted_words.get(word,0) + 1
        normal_index.append(counted_words)
        for word in counted_words.keys():
            inverted_index[word] = inverted_index.get(word,[]).append((idx,counted_words[word]))       #bi.insort_left(inverted_index.get(word,[]),(idx,counted_words[word]), key=lambda x : x[0])
    return inverted_index, normal_index



def compute_documents_norma(inverted_index,normal_index):
    documents_TFIDF = []
    for i in range(len(normal_index)):
        tot = 0
        for word in normal_index[i].keys():
            tot += compute_TFIDF(inverted_index,normal_index,i,word)**2
        documents_TFIDF.append((normal_index[i],sqrt(tot)))
    return documents_TFIDF


def compute_TFIDF(inverted_index,normal_index,document,word):
    
    idf = log2(len(normal_index)/len(inverted_index[word]))
    tf = normal_index[document]
    return tf * idf


def store_indexes(inverted_index, documents_TFIDF, index_file_path):
    indexes = {"inverted_index": inverted_index, "documents_TFIDF" : documents_TFIDF}
    with open(index_file_path, "w") as index_file:
        json.dump(indexes, index_file)


def extract_descriptions_as_list(df):
    return df["descriptions"].tolist()



def build_amazon_products_description_index():
    df = load_data()
    corpus = extract_descriptions_as_list(df)
    inverted_index, normal_index = build_index(corpus)
    normal_index = compute_documents_norma(inverted_index, normal_index)
    store_indexes(inverted_index,normal_index,INDEX_FILE_PATH)


def main():
    build_amazon_products_description_index()

if __name__ == "__main__":
    main()
        
        






