from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, max, floor, explode, collect_list, round, concat_ws

spark = SparkSession.builder \
    .config("spark.neo4j.bolt.url", "bolt://localhost:7687") \
    .config("spark.neo4j.authentication.type", "basic") \
    .config("spark.neo4j.authentication.basic.username", "neo4j") \
    .config("spark.neo4j.authentication.basic.password", "gjy123456") \
    .config("spark.neo4j.bolt.encryption.level", "NONE") \
    .getOrCreate()

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

movies_df.cache()

# Add decade column
print("Adding decade column...")
movies_df = movies_df.withColumn("decade", (floor(col("startYear") / 10) * 10).cast("int"))

# Explode genres into multiple rows
print("Exploding genres column...")
movies_df = movies_df.withColumn("genre", explode(col("genres")))

# Calculate average rating of genres by decade
print("Calculating average ratings for each genre by decade...")
avg_rating_by_genre_decade = movies_df.groupBy("decade", "genre") \
    .agg(round(avg("averageRating"), 2).alias("avgRating")) \
    .orderBy("decade", "genre")

# Save the average ratings of genres to a file
avg_rating_output_file = "./data/result/AvgRatingByGenreDecade.csv"
avg_rating_by_genre_decade.coalesce(1).write.option("header", "true").mode("overwrite").csv(avg_rating_output_file)
print(f"Average ratings by genre and decade saved to {avg_rating_output_file}")

# Calculate the top-rated genre per decade
print("Finding top-rated genre per decade...")
top_genre_per_decade = avg_rating_by_genre_decade.groupBy("decade") \
    .agg(max("avgRating").alias("maxAvgRating"))

# Alias datasets to avoid ambiguity
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

# Alias datasets to avoid ambiguity in the next join
top_genre_details_alias = top_genre_details.alias("top_genres")
movies_df_alias = movies_df.alias("movies")

# Find the top-rated movie in each top genre per decade
print("Finding top movies in each top genre per decade...")
movies_in_top_genres = top_genre_details_alias.join(
    movies_df_alias,
    (col("top_genres.decade") == col("movies.decade")) &
    (col("top_genres.genre") == col("movies.genre")),
    "inner"
).groupBy("top_genres.decade", "top_genres.genre") \
    .agg(
        round(max("movies.averageRating"), 2).alias("maxAvgRating"),
        concat_ws(", ", collect_list("movies.primaryTitle")).alias("topMovies")  # Convert ARRAY<STRING> to STRING
    ).orderBy("top_genres.decade")

# Save the top genres and movies to a file
top_genre_output_file = "./data/result/TopGenresByDecade.csv"
movies_in_top_genres.coalesce(1).write.option("header", "true").mode("overwrite").csv(top_genre_output_file)
print(f"Top genres by decade with movies saved to {top_genre_output_file}")

spark.stop()
