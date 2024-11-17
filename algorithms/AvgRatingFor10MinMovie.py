from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, floor, round
import time

# Measure total script runtime
script_start = time.time()

# Initialize Spark session
spark_start = time.time()
spark = SparkSession.builder \
    .config("spark.neo4j.bolt.url", "bolt://localhost:7687") \
    .config("spark.neo4j.authentication.type", "basic") \
    .config("spark.neo4j.authentication.basic.username", "neo4j") \
    .config("spark.neo4j.authentication.basic.password", "gjy123456") \
    .config("spark.neo4j.bolt.encryption.level", "NONE") \
    .getOrCreate()
spark_end = time.time()
print(f"Spark session initialization time: {spark_end - spark_start:.2f} seconds")

# Load movie data from Neo4j
load_start = time.time()
movies_df = spark.read.format("org.neo4j.spark.DataSource") \
    .option("url", "bolt://localhost:7687") \
    .option("authentication.basic.username", "neo4j") \
    .option("authentication.basic.password", "gjy123456") \
    .option("query", """
        MATCH (movie:Movie)
        WHERE movie.runtimeMinutes IS NOT NULL AND movie.averageRating IS NOT NULL
        RETURN toInteger(movie.runtimeMinutes) AS runtimeMinutes, movie.averageRating AS averageRating
    """) \
    .load()
load_end = time.time()
print(f"Data loading time: {load_end - load_start:.2f} seconds")

# Calculate the 10-minute runtime bucket
bucket_start = time.time()
movies_df = movies_df.withColumn("runtimeBucket", floor(col("runtimeMinutes") / 10) * 10)
bucket_end = time.time()
print(f"Runtime bucket calculation time: {bucket_end - bucket_start:.2f} seconds")

# Group by the runtime bucket and calculate the average rating
grouping_start = time.time()
average_rating_df = movies_df.groupBy("runtimeBucket") \
    .agg(round(avg("averageRating"), 2).alias("averageRating"))
grouping_end = time.time()
print(f"Average rating calculation time: {grouping_end - grouping_start:.2f} seconds")

# Sort by runtimeBucket for readability
sorting_start = time.time()
average_rating_df = average_rating_df.orderBy("runtimeBucket")
sorting_end = time.time()
print(f"Sorting results time: {sorting_end - sorting_start:.2f} seconds")

# Write the results to a single CSV file
output_start = time.time()
output_file = "./data/result/AvgRatingFor10MinMovie.csv"
average_rating_df.coalesce(1).write.option("header", "true").mode("overwrite").csv(output_file)
output_end = time.time()
print(f"Saving results time: {output_end - output_start:.2f} seconds")
print(f"Average ratings for 10-minute intervals have been saved to {output_file}")

# Total runtime
script_end = time.time()
print(f"Total script runtime: {script_end - script_start:.2f} seconds")

# Stop Spark session
spark.stop()
