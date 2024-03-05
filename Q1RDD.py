from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()
    
#df = sc.read.csv("hdfs://master:9000/Ergasia/departmentsR.csv")
#df.show()
#df = sc.read.csv("hdfs://master:9000/Ergasia/employeesR.csv")
#df.show()
#df = sc.read.csv("hdfs://master:9000/Ergasia/movie_genres.csv")
#df.show()
#df = sc.read.csv("hdfs://master:9000/Ergasia/movies.csv")
#df.show()
#df = sc.read.csv("hdfs://master:9000/Ergasia/ratings.csv")
#df.show()

# Q1

lines = spark.sparkContext.textFile("hdfs://master:9000/Ergasia/movies.csv")
mylist = lines.filter(lambda x:(x.split(",")[3] >= "1995" and x.split(",")[5] > "0" and x.split(",")[6] >= "0" ))
mylist2 = mylist.map(lambda x: (x.split(',')[1],  int(x.split(',')[5])-int(x.split(',')[6])))
print(mylist2.collect())
