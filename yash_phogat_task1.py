from pyspark import SparkContext, SparkConf
import itertools
import random
import time
import sys

start = time.time()
conf = SparkConf().set('spark.executor.memory', '4g').set("spark.driver.memory", "4g").setMaster("local[*]").setAppName("assignment3_task1")
# sc = SparkContext('local[*]', 'assignment3_task1_v1')
sc = SparkContext(conf=conf)
Yelp_RDD = sc.textFile(sys.argv[1])

rdd1_original = Yelp_RDD.map(lambda x: x.split(",")).filter(lambda a: str(a[0]) != 'user_id' and str(a[1]) != 'business_id')\
    .map(lambda x: (x[0], x[1]))

rdd1_unique_users = rdd1_original.map(lambda a: a[0]).distinct()
rdd1_unique_business = rdd1_original.map(lambda a: (a[1])).distinct()

#The [(User, Business), 1] RDD for all users that have rated any business


# print(" Data Length: ", len(rdd1_original.collect()))
# print("Unique Users: ", len(rdd1_unique_users.collect()))
#
# print("Unique Business: ", len(rdd1_unique_business.collect()))


m = len(rdd1_unique_users.collect())

dict_user = {}
j = 1
for i in rdd1_unique_users.collect():
        if i not in dict_user.keys():
            dict_user[i] = j
            j += 1

# print("Dictionary", dict_user.items())

rdd1_zero_one_matrix = rdd1_original.map(lambda k: ((k[1], dict_user[k[0]]), 1)).map(lambda b: (b[0][0], [b[0][1]])).reduceByKey(lambda a,b: a+b)
# print("Zero One Matrix: ", rdd1_zero_one_matrix.take(5))
zero_one_dict = rdd1_zero_one_matrix.collectAsMap()
# print(zero_one_dict.items())

def hash_func (row, hash_parameters):
    a = hash_parameters[0]
    b = hash_parameters[1]
    # print(row)

    return min([(a * x + b) % m for x in row[1]])



hash_para = [[random.randint(1,12000), random.randint(1,12000)],[random.randint(1,12000), random.randint(1,12000)],[random.randint(1,12000), random.randint(1,12000)],[random.randint(1,12000), random.randint(1,12000)],[random.randint(1,12000), random.randint(1,12000)],[random.randint(1,12000), random.randint(1,12000)],[random.randint(1, 12000), random.randint(1, 12000)], [random.randint(1, 12000), random.randint(1, 12000)],[random.randint(1, 12000), random.randint(1, 12000)],[random.randint(1,12000), random.randint(1,12000)],[random.randint(1,12000), random.randint(1,12000)],[random.randint(1,12000), random.randint(1,12000)],[random.randint(1,12000), random.randint(1,12000)],[random.randint(1,12000), random.randint(1,12000)],[random.randint(1,12000), random.randint(1,12000)],[random.randint(1, 12000), random.randint(1, 12000)], [random.randint(1, 12000), random.randint(1, 12000)],[random.randint(1, 12000), random.randint(1, 12000)],[random.randint(1, 12000), random.randint(1, 12000)],[random.randint(1, 12000), random.randint(1, 12000)],[random.randint(1,12000), random.randint(1,12000)],[random.randint(1,12000), random.randint(1,12000)],[random.randint(1,12000), random.randint(1,12000)],[random.randint(1,12000), random.randint(1,12000)],[random.randint(1,12000), random.randint(1,12000)],[random.randint(1,12000), random.randint(1,12000)],[random.randint(1, 12000), random.randint(1, 12000)], [random.randint(1, 12000), random.randint(1, 12000)],[random.randint(1, 12000), random.randint(1, 12000)],[random.randint(1,12000), random.randint(1,12000)],[random.randint(1,12000), random.randint(1,12000)],[random.randint(1,12000), random.randint(1,12000)],[random.randint(1,12000), random.randint(1,12000)],[random.randint(1,12000), random.randint(1,12000)],[random.randint(1,12000), random.randint(1,12000)],[random.randint(1, 12000), random.randint(1, 12000)], [random.randint(1, 12000), random.randint(1, 12000)],[random.randint(1, 12000), random.randint(1, 12000)],[random.randint(1, 12000), random.randint(1, 12000)],[random.randint(1, 12000), random.randint(1, 12000)],[random.randint(1,12000), random.randint(1,12000)],[random.randint(1,12000), random.randint(1,12000)],[random.randint(1,12000), random.randint(1,12000)],[random.randint(1,12000), random.randint(1,12000)],[random.randint(1,12000), random.randint(1,12000)],[random.randint(1,12000), random.randint(1,12000)],[random.randint(1, 12000), random.randint(1, 12000)], [random.randint(1, 12000), random.randint(1, 12000)],[random.randint(1, 12000), random.randint(1, 12000)],[random.randint(1,12000), random.randint(1,12000)],[random.randint(1,12000), random.randint(1,12000)],[random.randint(1,12000), random.randint(1,12000)],[random.randint(1,12000), random.randint(1,12000)],[random.randint(1,12000), random.randint(1,12000)],[random.randint(1,12000), random.randint(1,12000)],[random.randint(1, 12000), random.randint(1, 12000)], [random.randint(1, 12000), random.randint(1, 12000)],[random.randint(1, 12000), random.randint(1, 12000)],[random.randint(1, 12000), random.randint(1, 12000)],[random.randint(1, 12000), random.randint(1, 12000)],[random.randint(1,12000), random.randint(1,12000)],[random.randint(1,12000), random.randint(1,12000)],[random.randint(1,12000), random.randint(1,12000)],[random.randint(1,12000), random.randint(1,12000)],[random.randint(1,12000), random.randint(1,12000)],[random.randint(1,12000), random.randint(1,12000)],[random.randint(1, 12000), random.randint(1, 12000)], [random.randint(1, 12000), random.randint(1, 12000)],[random.randint(1, 12000), random.randint(1, 12000)],[random.randint(1,12000), random.randint(1,12000)],[random.randint(1,12000), random.randint(1,12000)],[random.randint(1,12000), random.randint(1,12000)],[random.randint(1,12000), random.randint(1,12000)],[random.randint(1,12000), random.randint(1,12000)],[random.randint(1,12000), random.randint(1,12000)],[random.randint(1, 12000), random.randint(1, 12000)], [random.randint(1, 12000), random.randint(1, 12000)],[random.randint(1, 12000), random.randint(1, 12000)],[random.randint(1, 12000), random.randint(1, 12000)],[random.randint(1, 12000), random.randint(1, 12000)],[random.randint(1,12000), random.randint(1,12000)],[random.randint(1,12000), random.randint(1,12000)],[random.randint(1,12000), random.randint(1,12000)],[random.randint(1,12000), random.randint(1,12000)],[random.randint(1,12000), random.randint(1,12000)],[random.randint(1,12000), random.randint(1,12000)],[random.randint(1, 12000), random.randint(1, 12000)], [random.randint(1, 12000), random.randint(1, 12000)],[random.randint(1, 12000), random.randint(1, 12000)],[random.randint(1,12000), random.randint(1,12000)],[random.randint(1,12000), random.randint(1,12000)],[random.randint(1,12000), random.randint(1,12000)],[random.randint(1,12000), random.randint(1,12000)],[random.randint(1,12000), random.randint(1,12000)],[random.randint(1,12000), random.randint(1,12000)],[random.randint(1, 12000), random.randint(1, 12000)], [random.randint(1, 12000), random.randint(1, 12000)],[random.randint(1, 12000), random.randint(1, 12000)],[random.randint(1, 12000), random.randint(1, 12000)],[random.randint(1, 12000), random.randint(1, 12000)]]
writestream = open(sys.argv[2], "w")
# writestream.write(str(hash_para) + ":")

signature_rdd = rdd1_zero_one_matrix.map(lambda mat: (mat[0], [hash_func(mat, hash_val) for hash_val in hash_para]))
# print("Hash Results: ", (signature_rdd.collect()))


n_hash_functions = len(hash_para)
band = 50
rows_per_band = int(n_hash_functions/band)

def divide_bands (sig_mat):
    sol = []
    # print(sig_mat)
    for i in range(0, len(sig_mat[1]), rows_per_band):
        sol.append(((int(i/rows_per_band)+1, tuple(sig_mat[1][i:i + rows_per_band])), [sig_mat[0]]))
    return sol


candidate_rdd = signature_rdd.map(lambda j: divide_bands(j)).flatMap(lambda f:f).\
                reduceByKey(lambda x,y: x+y).filter(lambda d: len(d[1])>1).map(lambda h: h[1])
# candidate_rdd_unique = list(set(candidate_rdd.collect()))

# print("Candidate Sets:", candidate_rdd.collect())

def similar_candidate_pairs(para):
    val_list = sorted(list(para))
    candidates_list = itertools.combinations(val_list,2)
    return list(set(candidates_list))




def jaccard_similarity (candidate_pairs):
    result = set()

    for i in candidate_pairs:
        # print(i+1)
        ele1 = i[0]
        ele2 = i[1]
        list_ele1 = zero_one_dict[ele1]
        list_ele2 = zero_one_dict[ele2]
        # print(i, "\nKey 1:", ele1, ", Value1 ", list_ele1, ", Key 2: ", ele2, ", Value2: ", list_ele2)
        jacc_sim_val = len(set(list_ele1).intersection(set(list_ele2)))/len(set(list_ele1).union(set(list_ele2)))
        result.add(((ele1, ele2), jacc_sim_val))
    return result

get_pairs_rdd = candidate_rdd.map(lambda val: similar_candidate_pairs(val)).map(lambda y: jaccard_similarity(y)).flatMap(lambda d: d).filter(lambda t: t[1]>=0.5).sortBy(lambda  v: v[0][1]).sortBy(lambda v: v[0][0])

# print(get_pairs_rdd.collect())
# print(zero_one_dict['HlYzk84INQaNslFT-lyBFw'])
# print(zero_one_dict['7dtzpuYYpZeg-kJj3yTcUQ'])


pred = get_pairs_rdd.collectAsMap()

# ***************************************************************************************************
# # Code for Precision and Recall check
#
#
# Yelp_ground_truth_rdd = sc.textFile("file:///Users/yashphogat/Documents/USC/INF553_Data_Mining/Assignment_3/data/pure_jaccard_similarity.csv")
#
# ground_truth_rdd = Yelp_ground_truth_rdd.map(lambda x: x.split(",")).filter(lambda a: str(a[0]) != 'business_id_1' and str(a[1]) != 'business_id_2').map(lambda x: ((x[0], x[1]), x[2]))
#
# # print(ground_truth_rdd.collect())
#
# pred = get_pairs_rdd.collectAsMap()
# # print(len(pred))
# actual = ground_truth_rdd.collectAsMap()
# # print(len(actual))
#
# true_positive = 0
# false_positive = 0
# false_negative = 0
# for i in pred.keys():
#     if i in actual.keys() and float(actual[i])==float(pred[i]):
#         # print(i)
#         true_positive+= 1
#     else:
#         false_positive+= 1
#
# for j in actual:
#     if j not in pred:
#         false_negative+=1
#
# precision = true_positive/(true_positive + false_positive)
# recall = true_positive/(true_positive + false_negative)
#
# print("Precision: ", precision)
# print("Recall: ", recall)
#
# *****************************************************************************************************************************************

output = "business_id_1, business_id_2, similarity\n"
for k,v in pred.items():
    output += str(k).replace(" ","").replace("'", "")[1:-1] + "," + str(v) + "\n"
writestream.write(str(output))

# writestream.write( "\nPrecision: " + str(precision) + "\nRecall: "+ str(recall)+ "\n\n")
writestream.close()

print("Duration: ", (time.time()-start))