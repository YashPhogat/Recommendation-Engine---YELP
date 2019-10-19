from pyspark import SparkContext, SparkConf
import math
import time
from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel, Rating
import numpy
import sys

start = time.time()

# conf = SparkConf().set('spark.executor.memory', '4g').set("spark.driver.memory", "4g").setMaster("local[*]").setAppName("assignment3_task2_userbased")
sc = SparkContext("local[*]", "assignment3_task2_userbased")

# sc = SparkContext(conf=conf)

Yelp_RDD_train = sc.textFile(sys.argv[1])
Yelp_RDD_active = sc.textFile(sys.argv[2])

case = int(sys.argv[3])
writefile = open(sys.argv[4], "w")

user_averages_dict = {}


rdd1_original = Yelp_RDD_train.map(lambda x: x.split(",")).filter(lambda a: str(a[0]) != 'user_id' and str(a[1]) != 'business_id')
    # .map(lambda x: (x[0], [(x[1], x[2])]))
rdd_active_users = Yelp_RDD_active.map(lambda x: x.split(",")).filter(lambda a: str(a[0]) != 'user_id' and str(a[1]) != 'business_id')
    # .map(lambda x: (x[0], x[1]))
# rdd1_original_bc = sc.broadcast(rdd1_original)
# rdd_active_users_bc = sc.broadcast(rdd_active_users)

#*******************************************************************************
# Load and parse the data
if case ==1:


    user_dict = {}
    business_dict = {}
    dict_to_user = {}
    dict_to_business = {}

    train_users = rdd1_original.map(lambda tru: tru[0]).collect()
    test_users = rdd_active_users.map(lambda teu: teu[0]).collect()
    all_users = set(train_users).union(set(test_users))

    usrcnt = 1
    for i in all_users:
        if i not in user_dict.keys():
            user_dict[i] = usrcnt
            dict_to_user[usrcnt] = i
            usrcnt += 1

    train_business = rdd1_original.map(lambda trb : trb[1]).collect()
    test_business = rdd_active_users.map(lambda teb : teb[1]).collect()
    all_business = set(train_business).union(set(test_business))

    buscnt = 1
    for j in all_business:
        if j not in business_dict.keys():
            business_dict[j] = buscnt
            dict_to_business[buscnt] = j
            buscnt += 1

    rdd_train_case1 = rdd1_original.map(lambda x : Rating(int(user_dict[x[0]]), int(business_dict[x[1]]), float(x[2])))
    # Build the recommendation model using Alternating Least Squares
    rank = 5
    numIterations = 10
    tune = 0.1
    model = ALS.train(rdd_train_case1, rank, numIterations, tune)

    # Evaluate the model on training data
    recommend_rating = rdd_active_users.map(lambda x : Rating(int(user_dict[x[0]]), int(business_dict[x[1]]), float(x[2])))
    rdd_active_case1 = recommend_rating.map(lambda x: (x[0], x[1]))
    recommend = model.predictAll(rdd_active_case1).map(lambda v: ((v[0], v[1]), v[2]))
    cold_start_rdd = recommend_rating.map(lambda x: ((x[0], x[1]), x[2])).subtractByKey(recommend)
    recommend = recommend.union(cold_start_rdd.map(lambda f: (f[0], 3.0)))
    # compare_rating = recommend_rating.map(lambda x: ((x[0], x[1]), x[2])).sortBy(lambda f: f[0][0]).join(recommend)

    def round_val(para):
        if para > 5.0:
            return 5.0
        elif para <= 1.0:
            return 1.0
        else:
            return para

    return_recommend = recommend.map(lambda x: ((dict_to_user[x[0][0]], dict_to_business[x[0][1]]), x[1])).map(lambda j: (j[0], round_val(j[1])))
    compare_rating = rdd_active_users.map(lambda x: ((x[0], x[1]), float(x[2]))).join(return_recommend)
    output_rdd = compare_rating.map(lambda x: ((x[0][0], x[0][1]), x[1][1])).collectAsMap()
    active_rdd = rdd_active_users.map(lambda d: ((d[0], d[1]), float(d[2]))).collectAsMap()

    # print((compare_rating.take(10)))
    MSE = compare_rating.map(lambda r: (r[1][0] - r[1][1])**2).mean()

    print("Root Mean Squared Error = " + str(math.sqrt(MSE)))

    output = "user_id, business_id, prediction\n"
    for k, v in active_rdd.items():
        output += str(k).replace(" ", "").replace("'", "")[1 :-1] + "," + str(output_rdd[k]) + "\n"
    writefile.write(str(output))



# *******************************************************************************

elif case == 2:

    rdd_train_user_dict = rdd1_original.map(lambda x: (x[0], [(x[1], float(x[2]))])).reduceByKey(lambda x,y: x+y).mapValues(lambda x: dict(x)).collectAsMap()
    rdd_train_business = rdd1_original.map(lambda x: (x[1], [(x[0], float(x[2]))])).reduceByKey(lambda x,y: x+y).mapValues(lambda x: dict(x)).collectAsMap()

    for user in rdd_train_user_dict.keys():
        user_averages_dict[user] = sum(dict(rdd_train_user_dict.get(user)).values())/len(dict(rdd_train_user_dict.get(user)).keys())

    def pearson (para):
        global user_averages_dict
        if para[0] in rdd_train_user_dict.keys():
            active_user_businesses = rdd_train_user_dict.get(para[0])
         # active_user_averages = sum(dict(active_user_businesses).values())/len(dict(active_user_businesses).keys())
            active_user_business_list = []
            business_in_concern_user_ratings = []
            active_user_business_list = list(dict(active_user_businesses).keys())
            # list(active_user_business_list).remove(para[1])
            # print(active_user_business_list)
            # user_averages_dict[para[0]] = active_user_averages
            # print("Dict: ", user_averages_dict.items())

            if(rdd_train_business.get(para[1]) == None):
                return [(para[0], para[1]), user_averages_dict[para[0]]]
            else:
                user_list_rated_active_business = list(rdd_train_business[para[1]])
                # print("====>", user_list_rated_active_business)
                if user_list_rated_active_business != []:
                    for j in range(len(user_list_rated_active_business)):#Users to check for rating if they have rated the prediction business
                        corated_item_user1 = 0
                        corated_item_user2 = 0
                        list_rating1 = []
                        list_rating2 = []
                        length_of_corated_items = 0

                        for i in range(len(active_user_business_list)): #Business's rated by active user
                            # print(active_user_business_list[i], "===>", rdd_train_user_dict[user_list_rated_active_business[j]])
                            if(rdd_train_user_dict[user_list_rated_active_business[j]].get(active_user_business_list[i])):

                                list_rating1.append(rdd_train_user_dict[para[0]].get(active_user_business_list[i]))
                                # print("======>", list_rating1)
                                list_rating2.append(rdd_train_user_dict[user_list_rated_active_business[j]].get(active_user_business_list[i]))

                                corated_item_user1 = sum(list_rating1)
                                corated_item_user2 = sum(list_rating2)

                                # corated_item_user1 += rdd_train_user_dict[para[0]].get(active_user_businesses[i])
                                # corated_item_user2 += rdd_train_user_dict[user_list_rated_active_business[j]].get(active_user_businesses[i])

                                length_of_corated_items = len(list_rating1)

                        if length_of_corated_items!=0:

                            avg_user1 = corated_item_user1/length_of_corated_items
                            avg_user2 = corated_item_user2/length_of_corated_items

                            normalized_rating1 = [x - avg_user1 for x in list_rating1]
                            normalized_rating2 = [x - avg_user2 for x in list_rating2]

                            denom1 = math.sqrt(sum(vec**2 for vec in normalized_rating1))
                            denom2 = math.sqrt(sum(vec**2 for vec in normalized_rating2))
                            numerator = sum(normalized_rating1[k] * normalized_rating2[k] for k in range(len(list_rating1)))

                            if (denom1*denom2 == 0):
                                weight = 0
                            else:
                                weight = numerator/(denom1*denom2)



                            business_in_concern_user_ratings.append((weight, weight * (rdd_train_user_dict[user_list_rated_active_business[j]].get(para[1]) - user_averages_dict[user_list_rated_active_business[j]])))
                        # print(para[1])

                    numerator_pred = sum(business_in_concern_user_ratings[i][1] for i in range(len(business_in_concern_user_ratings)))
                    denominator_pred = sum(abs(business_in_concern_user_ratings[i][0]) for i in range(len(business_in_concern_user_ratings)))
                    # predicted_rating = user_averages_dict[para[0]] + (numerator_pred / denominator_pred)
                    if numerator_pred == 0 or denominator_pred == 0:
                        return[(para[0], para[1]), user_averages_dict[para[0]]]
                    else:
                        predicted_rating = user_averages_dict[para[0]] + (numerator_pred/denominator_pred)
                        if predicted_rating > 5.0:
                            predicted_rating = user_averages_dict[para[0]]
                        elif predicted_rating < 1.0:
                            predicted_rating = user_averages_dict[para[0]]

                        return [(para[0], para[1]), predicted_rating]

                else:
                    return [(para[0], para[1]), 3.0]
        else:
            return [(para[0], para[1]), 3.0]



    recommend = rdd_active_users.map(lambda a: pearson(a)).collectAsMap()

    recommend_rating = rdd_active_users.map(lambda a: ((a[0],a[1]), float(a[2]))).collectAsMap()
    # print(recommend.keys())
    # print(recommend_rating.keys())
    output = "user_id, business_id, prediction\n"
    for k, v in recommend.items():
        output += str(k).replace(" ","").replace("'", "")[1:-1] + "," + str(v) + "\n"
    writefile.write(str(output))

    diff = 0.0
    for i in recommend.keys():
        if i in recommend_rating.keys():
            # print(i, "Pred: ", recommend[i]," Actual: ", recommend_rating[i], "Difference: ", recommend[i] - recommend_rating[i])
            diff += (recommend[i] - recommend_rating[i])**2
#
    RMSE = math.sqrt(diff/len(recommend.keys()))
#
    print("RMSE: ", RMSE)
    writefile.close()



    # print(len(rdd1_original.collect()))
    #
    # print(len(rdd_active_users.collect()))

# ***********************************************************************************************
# Item Based Collaborative Filtering

elif case == 3:


    rdd_train_user_dict = rdd1_original.map(lambda x : (x[0], [(x[1], float(x[2]))])).reduceByKey(lambda x, y : x + y).mapValues(lambda x : dict(x)).collectAsMap()
    rdd_train_business = rdd1_original.map(lambda x : (x[1], [(x[0], float(x[2]))])).reduceByKey(lambda x, y : x + y).mapValues(lambda x : dict(x)).collectAsMap()


        # business_averages_para = sum(dict(rdd_train_business.get(para[1])).values()) / len(dict(rdd_train_business.get(para[1])).keys())

    # for user in rdd_train_user_dict.keys():
    #     user_averages_dict[user] = sum(dict(rdd_train_user_dict.get(user)).values())/len(dict(rdd_train_user_dict.get(user)).keys())


    def pearson(para):

        if para[1] in rdd_train_business.keys() :
            business_averages_para = sum((dict(rdd_train_business.get(para[1])).values())) / len((dict(rdd_train_business.get(para[1])).keys()))


            active_user_business_list = []
            business_in_concern_ratings = []
            active_user_business_list = list(rdd_train_user_dict.get(para[0]).keys())

            # print(active_user_business_list)

            # print("Dict: ", user_averages_dict.items())

            if (rdd_train_business.get(para[1]) == None) :
                return [(para[0], para[1]), business_averages_para]
            else:
                user_list_rated_active_business = list(rdd_train_business[para[1]])
                # print("====>", user_list_rated_active_business)

                for j in range(len(active_user_business_list)):  # Users to check for rating if they have rated the prediction business
                    corated_item1 = 0
                    corated_item2 = 0
                    numerator = 0
                    list_rating1 = []
                    list_rating2 = []
                    length_of_corated_items = 0

                    for i in range(len(user_list_rated_active_business)) :  # Business's rated by active user
                        # print(active_user_business_list[i], "===>", rdd_train_user_dict[user_list_rated_active_business[j]])
                        if (rdd_train_business[active_user_business_list[j]].get(user_list_rated_active_business[i])) :
                            list_rating1.append(rdd_train_user_dict[user_list_rated_active_business[i]].get(para[1]))
                            # print("======>", list_rating1)
                            list_rating2.append(rdd_train_user_dict[user_list_rated_active_business[i]].get(active_user_business_list[j]))


                            length_of_corated_items = len(list_rating1)
                            # print(length_of_corated_items)
                    if length_of_corated_items >= 30:

                        for u in range(length_of_corated_items):
                            corated_item1 += list_rating1[u]
                            corated_item2 += list_rating2[u]

                        avg_item1 = corated_item1 / length_of_corated_items
                        avg_item2 = corated_item2 / length_of_corated_items

                        normalized_rating1 = [x - avg_item1 for x in list_rating1]
                        normalized_rating2 = [x - avg_item2 for x in list_rating2]

                        denom1 = math.sqrt(sum(vec ** 2 for vec in normalized_rating1))
                        denom2 = math.sqrt(sum(vec ** 2 for vec in normalized_rating2))
                        for k in range(len(list_rating1)):
                            numerator += (normalized_rating1[k] * normalized_rating2[k])
                        denom = denom1*denom2
                        if (denom == 0) :
                            weight = 0
                        else:
                            weight = numerator / (denom)

                        # if para[0] in rdd_train_user_dict.keys() and active_user_business_list[j] in dict(rdd_train_user_dict[para[0]]).keys():
                        business_in_concern_ratings.append((weight, weight * (rdd_train_user_dict[para[0]].get(active_user_business_list[j]))))
                numerator_pred = 0
                denominator_pred = 0# print(para[1])
                for x in range(len(business_in_concern_ratings)):
                    numerator_pred += (business_in_concern_ratings[x][1])
                    denominator_pred += abs(business_in_concern_ratings[x][0])
                # predicted_rating = user_averages_dict[para[0]] + (numerator_pred / denominator_pred)
                if numerator_pred == 0 or denominator_pred == 0:
                    return [(para[0], para[1]), business_averages_para]
                else:
                    predicted_rating = (numerator_pred / denominator_pred)
                    if predicted_rating > 5.0:
                        predicted_rating = business_averages_para
                    elif predicted_rating < 1.0:
                        predicted_rating = business_averages_para

                    return [(para[0], para[1]), predicted_rating]

        else:
            return [(para[0], para[1]), 3.0]



    recommend = rdd_active_users.map(lambda a : pearson(a)).collectAsMap()

    recommend_rating = rdd_active_users.map(lambda a : ((a[0], a[1]), float(a[2]))).collectAsMap()
    # print(recommend.keys())
    # print(recommend_rating.keys())
    output = "user_id, business_id, prediction\n"
    for k, v in recommend.items() :
        output += str(k).replace(" ", "").replace("'", "")[1 :-1] + "," + str(v) + "\n"
    writefile.write(str(output))

    diff = 0.0
    for i in recommend.keys():
        if i in recommend_rating.keys():
            # print(len(recommend.keys()))
            diff += (recommend[i] - recommend_rating[i]) ** 2
    #
    RMSE = math.sqrt(diff / len(recommend.keys()))
    #
    print("RMSE: ", RMSE)
    writefile.close()



else:
    print("Invalid Case Value!!!!!")

print("Duration: ", time.time() - start)
sc.stop()