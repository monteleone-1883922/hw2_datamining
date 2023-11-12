
from utils_for_hashing import *
import time



class CompareWithLSHSpark():

    def __init__(self,k,b,r,shingling_hash,minwise_hashes,lsh_hashes):
        self.k = k
        self.b = b
        self.r = r
        self.execution_times = []
        self.shingling_hash = shingling_hash
        self.minwise_hashes = minwise_hashes
        self.lsh_hashes = lsh_hashes

    def get_last_execution_time(self):
        return self.execution_times[-1]

    def compute_nearest_documents(self,documents):
        start_time = int(time.time() * 1000)
        rdd = documents.zipWithIndex()

        # returns set of tuples (id,hash(shingle))
        shingles = rdd.flatMap(
            lambda line: [(line[1], self.shingling_hash(line[0][0][i: i + self.k])) for i in range(len(line[0][0]) - self.k + 1)])
        # minwise hashing
        # returns set of tuples (id, num hash), hashed element
        minwise_hashing1 = shingles.flatMap(
            lambda line: [((line[0], i), apply_hash(self.minwise_hashes, line[1], i)) for i in range(self.b*self.r)])

        # returns a set of tuples (id, num hash), min between all hashed elements with hash num hash of document id
        minwise_hashing2 = minwise_hashing1.reduceByKey(lambda hash1, hash2: min(hash1, hash2))

        # lsh
        # returns a set of tuples (id, num band), array with element in position (ex num hash) containing the element
        lsh1 = minwise_hashing2.map(
            lambda el: ((el[0][0], el[0][1] // self.r), get_array_with_element(self.r, el[0][1] % self.r, el[1])))

        # returns a set of tuples (id, num band), full array with ordered signature of document id
        lsh2 = lsh1.reduceByKey(lambda el1, el2: merge_arrays(el1, el2))

        # returns a set of tuples (num band, hash(element)), id
        lsh3 = lsh2.map(lambda el: ((el[0][1], apply_hash(self.lsh_hashes, el[1], el[0][1])), el[0][0]))

        # returns a set of tuples (num band, hash), set of pairs
        lsh4 = lsh3.reduceByKey(lambda el1, el2: merge_sets(el1, el2))

        lsh5 = lsh4.filter(lambda el: type(el[1]) is set)

        result = lsh5.reduce(lambda el1, el2: union_sets(el1, el2))

        self.execution_times.append(int(time.time() * 1000) - start_time)
        return result


class CompareWithJaccardSimilaritySpark():

    def __init__(self,k,threshold, hash):
        self.k = k
        self.threshold = threshold
        self.num_records = 0
        self.execution_times = []
        self.hash = hash

    def get_last_execution_time(self):
        return self.execution_times[-1]

    def compute_nearest_documents(self, documents):
        num_documents = documents.count()
        start_time = int(time.time() * 1000)
        rdd = documents.zipWithIndex()

        # returns set of tuples (id,hash(shingle))
        shingles = rdd.flatMap(
            lambda line: [(line[1], self.hash(line[0][0][i: i + self.k])) for i in
                          range(len(line[0][0]) - self.k + 1)])

        # jaccard
        jaccard1 = shingles.reduceByKey(lambda s1, s2: union_shingles(s1, s2))

        jaccard2 = jaccard1.flatMap(lambda el: [(pair, el[1]) for pair in comparison_pairs(num_documents, el[0])])

        jaccard3 = jaccard2.reduceByKey(lambda set1, set2: compute_jaccard(set1, set2, 0.8))

        jaccard4 = jaccard3.filter(lambda x: x[1])

        result_jaccard = jaccard4.reduce(lambda el1, el2: union_pairs(el1, el2))

        self.execution_times.append(int(time.time() * 1000) - start_time)
        return result_jaccard

