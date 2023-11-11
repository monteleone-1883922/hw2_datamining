import sys
import hashlib
from typing import Callable
import time
import datetime

from amazon_product_analysis import load_data


class Shingling():

    def __init__(self, hash_function, k: int):
        self.hash = hash_function
        self.k = k

    def get_shingling(self, document):
        set_of_shinglings = []

        for i in range(len(document) - self.k + 1):
            word = document[i: i + self.k]
            set_of_shinglings.append(self.hash(word))
        return set_of_shinglings


class MinwiseHashing():

    def __init__(self, hash_functions, t: int):
        self.hashes = hash_functions
        self.t = t

    def get_minwise_hashing(self, elements_set):
        set_signature = []
        for i in range(self.t):
            min_hash = None
            for el in elements_set:
                min_hash = min(min_hash, self.apply_hash(el, i)) if not min_hash is None else self.apply_hash(el, i)
            set_signature.append(min_hash)
        return set_signature

    def apply_hash(self, el, i):
        hash = self.hashes[i % len(self.hashes)]
        return hash(el)


class LSH():

    def __init__(self, hash_functions, b: int, r: int):
        self.b = b
        self.r = r
        self.hashes = hash_functions

    def compute_approximation_threshold(self):
        return (1 / self.b) ** (1 / self.r)

    def get_LSH(self, elements):
        if len(elements[0]) != self.b * self.r:
            print("the signature of the elements must be ", self.b * self.r, " but it is ", len(elements[0]),
                  sys.stderr)
            exit(1)

        near_elements = set()
        for i in range(self.b):
            buckets = {}
            for j in range(len(elements)):
                fragment = elements[j][i * self.r:i * self.r + self.r]
                bucket = self.apply_hash(fragment, i)
                buckets[bucket] = buckets.get(bucket, []) + [j]
                for k in range(len(buckets[bucket]) - 1):
                    near_elements.add((buckets[bucket][k], j) if buckets[bucket][k] < j else (j, buckets[bucket][k]))
        return near_elements

    def apply_hash(self, el, i: int):
        hash = self.hashes[i % len(self.hashes)]
        return hash(el)


class CompareWithLSH():

    def __init__(self, shingling: Shingling, minwise_hashing: MinwiseHashing, lsh: LSH):
        self.shingling = shingling
        self.minwise_hashing = minwise_hashing
        self.lsh = lsh
        self.execution_times = []

    def compute_nearest_documents(self, documents):
        start_time = int(time.time() * 1000)
        signatures = []
        for document in documents:
            document_shingling = self.shingling.get_shingling(document)
            signatures.append(self.minwise_hashing.get_minwise_hashing(document_shingling))
        result = self.lsh.get_LSH(signatures)
        self.execution_times.append(int(time.time() * 1000) - start_time)
        return result

    def get_last_execution_time(self):
        return self.execution_times[-1]


class CompareWithJaccardSimilarity():

    def __init__(self, shingling: Shingling):
        self.shingling = shingling
        self.execution_times = []

    def compute_nearest_documents(self, documents, threshold: float):
        start_time = int(time.time() * 1000)
        near_documents = set()
        shinglings = []
        for document in documents:
            shinglings.append(set(self.shingling.get_shingling(document)))
        for i in range(len(shinglings) - 1):
            for j in range(i + 1, len(shinglings)):
                jaccard_similarity = (len(shinglings[i].intersection(shinglings[j])) /
                                      len(shinglings[i].union(shinglings[j])))
                if jaccard_similarity >= threshold:
                    near_documents.add((i, j))
        self.execution_times.append(int(time.time() * 1000) - start_time)
        return near_documents

    def get_last_execution_time(self):
        return self.execution_times[-1]


def compare_nearest_documents(df, jaccard_similarity: CompareWithJaccardSimilarity, lsh: CompareWithLSH):
    documents = df["description"]
    print("your threshold for lsh is ", lsh.lsh.compute_approximation_threshold())
    lsh_nearest = lsh.compute_nearest_documents(documents)
    jaccard_nearest = jaccard_similarity.compute_nearest_documents(documents)
    return lsh_nearest, jaccard_nearest




def hashFamily(i):
    resultSize = 8
    # how many bytes we want back
    maxLen = 20
    # how long can our i be (in decimal)
    salt = str(i).zfill(maxLen)[-maxLen:]
    salt = salt.encode('utf-8')

    def hashMember(x):
        if type(x) is str:
            x = x.encode('utf-8')
        elif type(x) is list:
            x = b"".join(x)
        return hashlib.sha1(x + salt).digest()[-resultSize:]

    return hashMember


class Report():

    def __init__(self):
        self.lsh_result = None
        self.jaccard_result = None
        self.lsh_num_duplicates = None
        self.jaccard_num_duplicates = None
        self.size_intersection_results = None
        self.lsh_last_execution_time = None
        self.jaccard_last_execution_time = None

    def build_report(self, df):
        documents = df["description"].tolist()
        shingling_hash = hashFamily(1)
        minwise_hashes = [hashFamily(i) for i in range(2, 72)]
        lsh_hashes = [hashFamily(72)]
        shingling = Shingling(shingling_hash, 10)
        minwise_hashing = MinwiseHashing(minwise_hashes, 70)
        lsh = LSH(lsh_hashes, 7, 10)
        lsh_comparator = CompareWithLSH(shingling, minwise_hashing, lsh)
        jaccard_comparator = CompareWithJaccardSimilarity(shingling)
        lsh_result = lsh_comparator.compute_nearest_documents(documents)
        jaccard_result = jaccard_comparator.compute_nearest_documents(documents,0.8)
        self.lsh_result = lsh_result
        self.jaccard_result = jaccard_result
        self.lsh_num_duplicates = len(lsh_result)
        self.jaccard_num_duplicates = len(jaccard_result)
        self.size_intersection_results = len(lsh_result.intersection(jaccard_result))
        self.lsh_last_execution_time = lsh_comparator.get_last_execution_time()
        self.jaccard_last_execution_time = jaccard_comparator.get_last_execution_time()


def test():
    df = load_data()
    report = Report()
    report.build_report(df)
    with open("report.txt", "w") as f:
        f.write("lsh_result: " + str(report.lsh_result) + "\n")
        f.write("jaccard_result: " + str(report.jaccard_result) + "\n")
        f.write("lsh_num_duplicates: " + str(report.lsh_num_duplicates) + "\n")
        f.write("jaccard_num_duplicates: " + str(report.jaccard_num_duplicates) + "\n")
        f.write("size_intersection_results: " + str(report.size_intersection_results) + "\n")
        f.write("lsh_last_execution_time: " + str(report.lsh_last_execution_time) + "\n")
        f.write("jaccard_last_execution_time: " + str(report.jaccard_last_execution_time) + "\n")


if __name__ == "__main__":
    test()
