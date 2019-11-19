from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from afinn import Afinn
from pyspark.sql.functions import avg
from pyspark.sql import SQLContext


from pyspark.mllib.recommendation import ALS
from pyspark.mllib.recommendation import MatrixFactorizationModel
import math, time, json
import redis



import os
import json
import pandas as pd

def test(rdd):

    if len(rdd.collect()) == 0:

        pass

    else:
        new_user_ratings = []
        msgValue = rdd.map(lambda x: json.loads(x[1])).collect()
        print(msgValue)
        print(type(msgValue[0]))
        new_user_ID = msgValue[0]['user_id']
        song_id = msgValue[0]['shoes_id']
        rating = msgValue[0]['rating']

        print(new_user_ID, song_id, rating)
        data = (new_user_ID, song_id, rating)

        new_user_ratings.append(data)



        print(new_user_ratings)

        new_user_ratings_RDD = sc.parallelize(new_user_ratings)


        print('New user ratings: %s' % new_user_ratings_RDD.take(3))


        # merge new data into old data

        complete_data_with_new_ratings_RDD = complete_ratings_data.union(new_user_ratings_RDD)

        # train model again with new data

        from time import time

        t0 = time()

        new_ratings_model = ALS.train(complete_data_with_new_ratings_RDD, best_rank, seed=seed, iterations=iterations, lambda_=regularization_parameter)

        tt = time() - t0

        print("New model trained in %s seconds" % round(tt,3))


        #print(new_user_ratings)
        #print(type(new_user_ratings))
        #print(map(lambda x: x[1], new_user_ratings))
        try:
            new_user_ratings_ids = map(lambda x: x[1], new_user_ratings) # get just music IDs
        except:
            print(new_user_ratings,new_user_ratings_ids)

        # keep just those not on the ID list

        new_user_unrated_music_RDD = (complete_music_data.filter(lambda x: x[0] not in new_user_ratings_ids).map(lambda x: (new_user_ID, x[0])))

        # Use the input RDD, new_user_unrated_music_RDD, with new_ratings_model.predictAll() to predict new ratings for the musics

        new_user_recommendations_RDD = new_ratings_model.predictAll(new_user_unrated_music_RDD)


        # get every predicct result for new user

        new_user_recommendations_rating_RDD = new_user_recommendations_RDD.map(lambda x: (x.product, x.rating))

        # merge data with music info

        new_user_recommendations_rating_title_and_count_RDD = new_user_recommendations_rating_RDD.join(complete_music_titles).join(music_rating_counts_RDD)

        new_user_recommendations_rating_title_and_count_RDD.take(3)

        # transfer data format

        new_user_recommendations_rating_title_and_count_RDD = new_user_recommendations_rating_title_and_count_RDD.map(lambda r: (r[1][0][1], r[1][0][0], r[1][1]))

        # sort data by rating score and list first 25 data

        top_musics = new_user_recommendations_rating_title_and_count_RDD.filter(lambda r: r[2]>=25).takeOrdered(25, key=lambda x: -x[1])

        # print('TOP recommended musics (with more than 25 reviews):\n%s' % '\n'.join(map(str, top_musics)))

        # result_r = r.hset('shoes', new_user_ID, str(top_musics))


        # j = {'user': new_user_ID, 'music': top_musics}
        # result_m = shoe_recommend.insert_one(j)

        new_user_ratings = []

        return sc.parallelize(top_musics)
        # print(result_r, new_user_ratings)


def redis_conn(rdd):



    r = redis.StrictRedis(
        host='35.194.196.242',
        port="6379",
        password='123456',
        ssl=False, charset="utf-8", decode_responses=True)


    if len(rdd.collect()) == 0:

        pass

    else:
        print(str(rdd.take(25)))
        
        r.hset('shoes', "610", str(rdd.take(25))) 
        print("test123")


if __name__ == "__main__":


    sc = SparkContext()
    ssc = StreamingContext(sc, 1)
    sqlContext = SQLContext(sc)

    # A sentiment analysis model
    model_path = "/home/cloudera/recommend/shoe_lens_als"

    same_model = MatrixFactorizationModel.load(sc, model_path)

    # load data

    complete_ratings_raw_data = sc.textFile("/home/cloudera/recommend/ratings.csv")

    # set column names

    complete_ratings_raw_data_header = complete_ratings_raw_data.take(1)[0]



    complete_ratings_data = complete_ratings_raw_data.filter(lambda line: line!=complete_ratings_raw_data_header).map(lambda line: line.split(",")).map(lambda tokens: (int(tokens[0]),int(tokens[1]),float(tokens[2]))).cache()

    print("There are %s recommendations in the complete dataset" % (complete_ratings_data.count()))



    seed = 5

    iterations = 10

    regularization_parameter = 0.1

    ranks = [4, 8, 12]

    errors = [0, 0, 0]

    err = 0

    tolerance = 0.02

    min_error = float('inf')

    best_rank = 4


    best_iteration = -1



    # load music neta data

    complete_music_raw_data = sc.textFile("/home/cloudera/recommend/shoes_20191107.csv")

    complete_music_raw_data_header = complete_music_raw_data.take(1)[0]

    complete_music_data = complete_music_raw_data.filter(lambda line: line!=complete_music_raw_data_header).map(lambda line: line.split(",")).map(lambda tokens: (int(tokens[0]),tokens[1],tokens[2])).cache()

    complete_music_titles = complete_music_data.map(lambda x: (int(x[0]),x[1], x[2]))

    print("There are %s shoes in the complete dataset" % (complete_music_titles.count()))




    def get_counts_and_averages(ID_and_ratings_tuple):

        nratings = len(ID_and_ratings_tuple[1])

        return ID_and_ratings_tuple[0], (nratings, float(sum(x for x in ID_and_ratings_tuple[1]))/nratings)


    # group by every mucis couts data by music id

    music_ID_with_ratings_RDD = (complete_ratings_data.map(lambda x: (x[1], x[2])).groupByKey())

    # get average and counts for each music

    music_ID_with_avg_ratings_RDD = music_ID_with_ratings_RDD.map(get_counts_and_averages)

    # get counts for each music

    music_rating_counts_RDD = music_ID_with_avg_ratings_RDD.map(lambda x: (x[0], x[1][0]))


    raw_stream = KafkaUtils.createStream(ssc, "zookeeper:2181", "consumer-group", {"shoes_rating": 1}) \
                           .window(1, 1)


    data = raw_stream.transform(test)
    data.foreachRDD(redis_conn)

    data.pprint(20)

    # Start it
    ssc.start()
    ssc.awaitTermination()

