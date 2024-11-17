from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, max, floor, explode, collect_list, round, concat_ws
import time
start_time = time.time()

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

load_start = time.time()
print("Loading data from Neo4j...")
movies_df = spark.read.format("org.neo4j.spark.DataSource") \
    .option("url", "bolt://localhost:7687") \
    .option("authentication.basic.username", "neo4j") \
    .option("authentication.basic.password", "gjy123456") \
    .option("query", """
        MATCH (movie:Movie)
        WHERE movie.runtimeMinutes IS NOT NULL AND movie.averageRating IS NOT NULL
        RETURN toInteger(movie.runtimeMinutes) AS runtimeMinutes,
               movie.averageRating AS averageRating,
               toInteger(movie.startYear) AS startYear,
               movie.genres AS genres,
               movie.primaryTitle AS primaryTitle
    """) \
    .load()
load_end = time.time()
print(f"Data loading time: {load_end - load_start:.2f} seconds")

cache_start = time.time()
movies_df.cache()
cache_end = time.time()
print(f"Data caching time: {cache_end - cache_start:.2f} seconds")

# Add decade column
decade_start = time.time()
print("Adding decade column...")
movies_df = movies_df.withColumn("decade", (floor(col("startYear") / 10) * 10).cast("int"))
decade_end = time.time()
print(f"Decade calculation time: {decade_end - decade_start:.2f} seconds")

# Explode genres into multiple rows
explode_start = time.time()
print("Exploding genres column...")
movies_df = movies_df.withColumn("genre", explode(col("genres")))
explode_end = time.time()
print(f"Genres exploding time: {explode_end - explode_start:.2f} seconds")

# Calculate average rating of genres by decade
avg_rating_start = time.time()
print("Calculating average ratings for each genre by decade...")
avg_rating_by_genre_decade = movies_df.groupBy("decade", "genre") \
    .agg(round(avg("averageRating"), 2).alias("avgRating")) \
    .orderBy("decade", "genre")
avg_rating_end = time.time()
print(f"Average rating calculation time: {avg_rating_end - avg_rating_start:.2f} seconds")


# Save the average ratings of genres to a file
save_avg_rating_start = time.time()
avg_rating_output_file = "./data/result/AvgRatingByGenreDecade.csv"
avg_rating_by_genre_decade.coalesce(1).write.option("header", "true").mode("overwrite").csv(avg_rating_output_file)
print(f"Average ratings by genre and decade saved to {avg_rating_output_file}")
save_avg_rating_end = time.time()
print(f"Saving average ratings time: {save_avg_rating_end - save_avg_rating_start:.2f} seconds")


# Calculate the top-rated genre per decade
top_genre_start = time.time()

print("Finding top-rated genre per decade...")
top_genre_per_decade = avg_rating_by_genre_decade.groupBy("decade") \
    .agg(max("avgRating").alias("maxAvgRating"))
top_genre_end = time.time()
print(f"Top genre calculation time: {top_genre_end - top_genre_start:.2f} seconds")


# Alias datasets to avoid ambiguity
join_top_genre_start = time.time()
avg_rating_alias = avg_rating_by_genre_decade.alias("avg_rating")
top_genre_alias = top_genre_per_decade.alias("top_genre")

# Join to get the top-rated genre's name per decade
top_genre_details = top_genre_alias.join(
    avg_rating_alias,
    (top_genre_alias["decade"] == avg_rating_alias["decade"]) &
    (top_genre_alias["maxAvgRating"] == avg_rating_alias["avgRating"]),
    "inner"
).select(
    col("top_genre.decade").alias("decade"),
    col("avg_rating.genre").alias("genre"),
    col("avg_rating.avgRating").alias("maxAvgRating")
)
join_top_genre_end = time.time()
print(f"Joining top genres time: {join_top_genre_end - join_top_genre_start:.2f} seconds")


# Alias datasets to avoid ambiguity in the next join
top_genre_details_alias = top_genre_details.alias("top_genres")
movies_df_alias = movies_df.alias("movies")
alias_end = time.time()


# Find the top-rated movie in each top genre per decade
find_top_movie_start = time.time()
print("Finding top movies in each top genre per decade...")
movies_in_top_genres = top_genre_details_alias.join(
    movies_df_alias,
    (col("top_genres.decade") == col("movies.decade")) &
    (col("top_genres.genre") == col("movies.genre")),
    "inner"
).groupBy("top_genres.decade", "top_genres.genre") \
    .agg(
        round(max("movies.averageRating"), 2).alias("maxAvgRating"),
        concat_ws(", ", collect_list("movies.primaryTitle")).alias("topMovies")
    ).orderBy("top_genres.decade")

find_top_movie_end = time.time()
print(f"Top movie calculation time: {find_top_movie_end - find_top_movie_start:.2f} seconds")


# Save the top genres and movies to a file
save_top_genre_start = time.time()
top_genre_output_file = "./data/result/TopGenresByDecade.csv"
movies_in_top_genres.coalesce(1).write.option("header", "true").mode("overwrite").csv(top_genre_output_file)
print(f"Top genres by decade with movies saved to {top_genre_output_file}")
save_top_genre_end = time.time()
print(f"Saving top genres time: {save_top_genre_end - save_top_genre_start:.2f} seconds")


end_time = time.time()
total_runtime = end_time - start_time
print(f"Total runtime: {total_runtime:.2f} seconds")

spark.stop()
