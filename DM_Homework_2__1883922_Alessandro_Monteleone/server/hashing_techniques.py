import json
import sys
from utils_for_hashing import hashFamily,NUM_PARTITIONS,PATH_REPORT,PATH_REPORT_SPARK

import time
import pandas as pd
import pyspark as pk
from hashing_techniques_with_spark import CompareWithLSHSpark,CompareWithJaccardSimilaritySpark
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



def load_report(file):
    with open(file,"r") as report_file:
        return json.load(report_file)

class Report():

    def __init__(self):
        self.lsh_result = None
        self.jaccard_result = None
        self.lsh_num_duplicates = None
        self.jaccard_num_duplicates = None
        self.size_intersection_results = None
        self.lsh_last_execution_time = None
        self.jaccard_last_execution_time = None

    def save_results(self,lsh_result, jaccard_result, lsh_exec_time, jaccard_exec_time):
        self.lsh_result = lsh_result
        self.jaccard_result = jaccard_result
        self.lsh_num_duplicates = len(lsh_result)
        self.jaccard_num_duplicates = len(jaccard_result)
        self.size_intersection_results = len(lsh_result.intersection(jaccard_result))
        self.lsh_last_execution_time = lsh_exec_time
        self.jaccard_last_execution_time = jaccard_exec_time

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
        self.save_results(lsh_result,jaccard_result,lsh_comparator.get_last_execution_time(),jaccard_comparator.get_last_execution_time() )

    def build_report_spark(self,df):
        documents = df["description"]
        shingling_hash = hashFamily(1)
        minwise_hashes = [hashFamily(i) for i in range(2, 72)]
        lsh_hashes = [hashFamily(72)]
        documents = pd.DataFrame(documents)
        sc = pk.SparkContext("local[*]")
        documents_rdd = sc.parallelize(documents.to_records(index=False), NUM_PARTITIONS)
        lsh_comparator = CompareWithLSHSpark(10,7,10,shingling_hash,minwise_hashes,lsh_hashes)
        jaccard_comparator = CompareWithJaccardSimilaritySpark(10,0.8,shingling_hash)
        lsh_result = lsh_comparator.compute_nearest_documents(documents_rdd)
        jaccard_result = jaccard_comparator.compute_nearest_documents(documents_rdd)
        self.save_results(lsh_result,jaccard_result,lsh_comparator.get_last_execution_time(),jaccard_comparator.get_last_execution_time() )
        sc.stop()

    def store_report(self,file):
        with open(file, "w") as report_file:
            report = {
                "lsh_result": list(self.lsh_result),
                "jaccard_result": list(self.jaccard_result),
                "lsh_num_duplicates": self.lsh_num_duplicates,
                "jaccard_num_duplicates": self.jaccard_num_duplicates,
                "size_intersection_results": self.size_intersection_results,
                "lsh_last_execution_time": self.lsh_last_execution_time,
                "jaccard_last_execution_time": self.jaccard_last_execution_time
            }
            json.dump(report, report_file, indent=4)


def main():
    df = load_data()
    report = Report()
    report.build_report(df)
    print("end report1")
    report_spark = Report()
    report_spark.build_report_spark(df)
    print("end report2")
    report.store_report(PATH_REPORT)
    report_spark.store_report(PATH_REPORT_SPARK)



if __name__ == "__main__":
    main()
