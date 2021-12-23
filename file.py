import sys
from pyspark.sql import SparkSession

spark = SparkSession\
        .builder\
        .appName("Rating")\
        .getOrCreate()

movie_file = spark.sparkContext.textFile("movies.csv")
rating_file = spark.sparkContext.textFile("ratings.csv")

movie_content = movie_file.filter(lambda x: x.split(",")[0] != "movieId")\
        .map(lambda x: (int(x.split(",")[0]), x.split(",")[len(x.split(",")) - 1].split("|")[0]))

rating_content = rating_file.filter(lambda x: x.split(",")[1] != "movieId")\
        .map(lambda x: (int(x.split(",")[1]), float(x.split(",")[2])))\
        .groupByKey()\
        .map(lambda x: (x[0], list(x[1])))\
        .map(lambda x: (x[0], sum(x[1]) / len(x[1])))

joined = rating_content\
        .join(movie_content)\
        .map(lambda x: (x[1][1], x[1][0]))\
        .groupByKey()\
        .map(lambda x: (x[0], list(x[1])))\
        .map(lambda x: (x[0], sum(x[1]) / len(x[1])))\

joined.saveAsTextFile("results")

spark.stop()
