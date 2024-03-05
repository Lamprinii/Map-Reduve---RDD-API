from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()
    
from io import StringIO
import csv

lines = spark.sparkContext.textFile("hdfs://master:9000/Ergasia/movies.csv")

movies = \
    spark.sparkContext.textFile("hdfs://master:9000/Ergasia/movies.csv") .\
    filter(lambda x:(x.split(",")[3] == "1995" and x.split(",")[6] > "0")) .\
    map(lambda x: (x.split(",")[0], x.split(",")[6]))

movies_genres = \
    spark.sparkContext.textFile("hdfs://master:9000/Ergasia/movie_genres.csv"). \
    filter(lambda x: (x.split(",")[1] == "Animation")) .\
    map(lambda x: (x.split(",")[0], x.split(",")[1]))   
    
poso = movies.join(movies_genres)
x = poso.sortBy(lambda x: x[1], ascending=False) # Taxinomhsh 
print(x.take(1))

id_mv = "54648"
lines1 = spark.sparkContext.textFile("hdfs://master:9000/Ergasia/movies.csv")
mylist = lines1.filter(lambda x: (x.split(",")[0] == id_mv))
mylist1 = mylist.map(lambda x: (x.split(",")[1])) 
print("H tainia me to megalytero poso:", mylist1.collect())
