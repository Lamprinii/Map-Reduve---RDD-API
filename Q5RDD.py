from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

movies = \
    spark.sparkContext.textFile("hdfs://master:9000/Ergasia/movies.csv") .\
    filter(lambda x:(x.split(",")[3] > "0" and x.split(",")[6] > "0")) .\
    map(lambda x: (x.split(",")[1],x.split(",")[3], x.split(",")[6])))


lines = spark.sparkContext.textFile("hdfs://master:9000/Ergasia/movies.csv")
mylist = lines.filter(lambda x:(x.split(",")[3] > "0" and x.split(",")[6] > "0"))
mylist2 = mylist.map(lambda x: int(x.split(',')[6]))
print(mylist2.collect())
mylist2 = mylist.reduce( lambda x,y: int(x.split(',')[6]+y.split(',')[6]))