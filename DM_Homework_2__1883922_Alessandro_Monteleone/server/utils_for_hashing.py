import hashlib

NUM_PARTITIONS = 4
PATH_REPORT = "data/report.json"
PATH_REPORT_SPARK = "data/report_spark.json"

def apply_hash(hashes, el, i):
    hash = hashes[i % len(hashes)]
    return hash(el)


def get_array_with_element(len_array, el_pos, el):
    array = [None for _ in range(len_array)]
    array[el_pos] = el
    return array


def merge_arrays(arr1, arr2):
    new_arr = []
    for i in range(len(arr1)):
        if arr1[i] is None and arr2[i] is None:
            new_arr.append(None)
        else:
            new_arr.append(arr1[i] if arr1[i] is not None else arr2[i])
    return new_arr


def merge_sets(el1, el2):
    if type(el1) is set and type(el2) is set:
        result_union = el1.union(el2)
        for x in el1:
            for y in el2:
                result_union.add((x[0], y[0]) if x[0] < y[0] else (y[0], x[0]))
                result_union.add((x[1], y[0]) if x[1] < y[0] else (y[0], x[1]))
                result_union.add((x[0], y[1]) if x[0] < y[1] else (y[1], x[0]))
                result_union.add((x[1], y[1]) if x[1] < y[1] else (y[1], x[1]))

        return result_union
    if type(el1) is set:
        tmp = set()
        for x in el1:
            tmp.add((el2, x[0]) if el2 < x[0] else (x[0], el2))
            tmp.add((el2, x[1]) if el2 < x[1] else (x[1], el2))
        return el1.union(tmp)
    if type(el2) is set:
        tmp = set()
        for x in el2:
            tmp.add((el1, x[0]) if el1 < x[0] else (x[0], el1))
            tmp.add((el1, x[1]) if el1 < x[1] else (x[1], el1))
        return el2.union(tmp)
    return {(el1, el2)} if el1 < el2 else {(el2, el1)}


def union_sets(el1, el2):
    if type(el1) is set and type(el2) is set:
        return el1.union(el2)
    if type(el1) is set:
        return el1.union(el2[1])
    if type(el2) is set:
        return el2.union(el1[1])
    return el1[1].union(el2[1])


def union_shingles(s1, s2):
    if type(s1) is set and type(s2) is set:
        return s1.union(s2)
    if type(s1) is set:
        return s1.union({s2})
    if type(s2) is set:
        return s2.union({s1})
    return {s1, s2}


def union_pairs(p1, p2):
    if type(p1) is set and type(p2) is set:
        return p1.union(p2)
    if type(p1) is set:
        return p1.union({p2[0]})
    if type(p2) is set:
        return p2.union({p1[0]})
    return {p1[0], p2[0]}


def comparison_pairs(n, id):
    result_comparison = []
    for i in range(n):
        if i != id:
            result_comparison.append((i, id) if i < id else (id, i))
    return result_comparison


def compute_jaccard(s1, s2, threshold):
    jaccard_similarity = (len(s1.intersection(s2)) /
                          len(s1.union(s2)))
    if jaccard_similarity >= threshold:
        return True
    return False



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
