from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, LongType, FloatType

if __name__ == '__main__':
    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL basic example") \
        .getOrCreate()

    movies_schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("description", StringType(), True),
        StructField("year", IntegerType(), True),
        StructField("duration", IntegerType(), True),
        StructField("cost", LongType(), True),
        StructField("revenue", LongType(), True),
        StructField("popularity", FloatType(), True),
    ])


movies = spark.read.csv("hdfs://master:9000/Ergasia/movies.csv", schema=movies_schema) #\
new_movies = spark.read.parquet("hdfs://master:9000/Ergasia/new_movies.parquet")
#new_movies.show()
# .write.save("hdfs://master:9000/Ergasia/new_movies.parquet", format="parquet")

rating_schema = StructType([
    StructField("id_user", IntegerType(), True),
    StructField("id", IntegerType(), True),
    StructField("rating", FloatType(), True),
    StructField("time", LongType(), True),
 ])

rating = spark.read.csv("hdfs://master:9000/Ergasia/ratings.csv", schema=rating_schema) #\
new_rating = spark.read.parquet("hdfs://master:9000/Ergasia/new_rating.parquet")
#new_rating.show()
# .write.save("hdfs://master:9000/Ergasia/new_rating.parquet", format="parquet")


movies_genres_schema = StructType([
   StructField("id", IntegerType(), True),
   StructField("type", StringType(), True),
])

movies_genres = spark.read.csv("hdfs://master:9000/Ergasia/movie_genres.csv", schema = movies_genres_schema) #\
new_movies_genres = spark.read.parquet("hdfs://master:9000/Ergasia/new_movies_genres.parquet")
#new_movies_genres.show()
# .write.save("hdfs://master:9000/Ergasia/new_movies_genres.parquet", format="parquet")


departments = spark.read.csv("hdfs://master:9000/Ergasia/departmentsR.csv") #\
new_departments = spark.read.parquet("hdfs://master:9000/Ergasia/new_departments.parquet")
#new_departments.show()
# .write.save("hdfs://master:9000/Ergasia/new_departments.parquet", format="parquet")

employees = spark.read.csv("hdfs://master:9000/Ergasia/employeesR.csv") #\
new_employees = spark.read.parquet("hdfs://master:9000/Ergasia/new_employees.parquet")
#new_employees.show()
# .write.save("hdfs://master:9000/Ergasia/new_employees.parquet", format="parquet")

# Q1

new_movies.filter((new_movies.year >= 1995) & (new_movies.revenue > 0) & (new_movies.cost > 0)).show()
(new_movies.select("cost").subtract(new_movies.select("revenue"))).show()

#Q2

new_movies.filter(new_movies.name == "Cesare deve morire") \
.select("id").show()

print("O arithmos gia to posoi vathmologhsan einai:", new_rating.select('rating').where(new_rating.id == 96821).count())

new_rating.agg({'rating': 'sum'}).show()

print("H mesh vathmologia einai peripou: 0.00489760363")

#Q3

movies1 = new_movies.filter((new_movies.year >= 1995) & (new_movies.revenue > 0))
movies1.show()
genres1 = new_movies_genres.filter(new_movies_genres.type == "Animation")
genres1.show()

joined_df = movies1.join(genres1, movies1.id == genres1.id)

joined_df.show()

sorted = joined_df.sort(joined_df.revenue.desc())

sorted.show(1)

#Q4

genres1 = new_movies_genres.filter(new_movies_genres.type == "Comedy")
movies1 = new_movies.filter((new_movies.year >= 1995) & (new_movies.revenue > 0))

#Q5
movies1 = new_movies.filter((new_movies.year >= 0) & (new_movies.revenue > 0))