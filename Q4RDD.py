from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

import csv
from operator import itemgetter

def taxhnomhsh(g):
    return g.sort((x[0][1][1]))    
    max(g, key=itemgetter([0][0][1]))



movies_genres = \
    spark.sparkContext.textFile("hdfs://master:9000/Ergasia/movie_genres.csv"). \
    filter(lambda x: (x.split(",")[1] == "Comedy")) .\
    map(lambda x: (x.split(",")[0], x.split(",")[1])) 

movies = \
    spark.sparkContext.textFile("hdfs://master:9000/Ergasia/movies.csv") .\
    filter(lambda x:(x.split(",")[3] >= "1995")) .\
    map(lambda x: (x.split(",")[0],(x.split(",")[1],x.split(",")[3], x.split(",")[7])))

g = movies.join(movies_genres) .\
    map(lambda x : (x[0],x[1])) .\
    groupBy( lambda x: (x[1][1], x[1][0]))

# g_sort = g.sortBy(lambda x: (x[0][1][1]))

g.mapValues(taxhnomhsh).collect()
    

