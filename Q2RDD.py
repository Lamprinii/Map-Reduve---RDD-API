from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()
    
from io import StringIO
import csv

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

# Q2

#lines = spark.sparkContext.textFile("hdfs://master:9000/Ergasia/movies.csv")
#mylist1 = lines.filter(lambda x: (x.split(",")[1] == "Cesare deve morire"))
#mylist3 = mylist1.map(lambda x: (x.split(",")[0])) 
#id = print(mylist3.collect()) #Emfanish id

# Posoi xrhstes vathmologhsan 

def split_complex(x):
   return list(csv.reader(StringIO(x), delimiter=','))[0] 
    

movies = \
        spark.sparkContext.textFile("hdfs://master:9000/Ergasia/movies.csv"). \
        map(split_complex)
   
ratings = \
        spark.sparkContext.textFile("hdfs://master:9000/Ergasia/ratings.csv"). \
        map(lambda x: (x.split(",")[1], x.split(",")[2]))

cesareid = \
    movies.filter(lambda x: (x[1] == "Cesare deve morire")). \
    map(lambda x: (x[0])). \
    collect()[0]

print("cesareid:", cesareid) #id
mylist8 = ratings.filter(lambda x: (x[0] == cesareid))
print(mylist8.collect()) #o enwmenos pinakas me ta id kai tis kritikes

lines = spark.sparkContext.textFile("hdfs://master:9000/Ergasia/ratings.csv")
mylist0 = lines.filter(lambda x: (x.split(",")[0] == cesareid))
mylist4 = mylist0.map(lambda x: (x.split(",")[0]))
rtg_num = print(mylist4.count()) # Posoi vathmologhsan thn sygkekrimenh tainia