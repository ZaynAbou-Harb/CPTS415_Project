from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, floor, round

spark = SparkSession.builder \
    .config("spark.neo4j.bolt.url", "bolt://localhost:7687") \
    .config("spark.neo4j.authentication.type", "basic") \
    .config("spark.neo4j.authentication.basic.username", "neo4j") \
    .config("spark.neo4j.authentication.basic.password", "gjy123456") \
    .config("spark.neo4j.bolt.encryption.level", "NONE") \
    .getOrCreate()

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

# Calculate the 10-minute runtime bucket
movies_df = movies_df.withColumn("runtimeBucket", floor(col("runtimeMinutes") / 10) * 10)

# Group by the runtime bucket and calculate the average rating, rounded to 2 decimal places
average_rating_df = movies_df.groupBy("runtimeBucket") \
    .agg(round(avg("averageRating"), 2).alias("averageRating"))

# Sort by runtimeBucket for readability
average_rating_df = average_rating_df.orderBy("runtimeBucket")

# Output the results to the console
average_rating_df.show()

# Write the results to a single CSV file
output_file = "./data/result/AvgRatingFor10MinMovie.csv"
average_rating_df.coalesce(1).write.option("header", "true").mode("overwrite").csv(output_file)
print(f"Average ratings for 10-minute intervals have been saved to {output_file}")

spark.stop()
