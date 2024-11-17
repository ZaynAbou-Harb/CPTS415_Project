from pyspark.sql import SparkSession
import time

url = "bolt://localhost:7687"
username = "neo4j"
password = "password"
dbname = "neo4j"

spark = (
    SparkSession.builder
    .config("spark.jars", "file:///C:/Users/eunic/spark-3.5.3-bin-hadoop3/jars/neo4j-connector-apache-spark_2.12-5.3.2_for_spark_3.jar")
    .config("spark.neo4j.url", url)
    .config("spark.neo4j.authentication.basic.username", username)
    .config("spark.neo4j.authentication.basic.password", password)
    .config("spark.neo4j.database", dbname)
    .getOrCreate()
)


query_collab_test = """
MATCH (p1:Person)-[:WORKED_ON]->(m:Movie)<-[:WORKED_ON]->(p2:Person)
WHERE p1<>p2 
AND m.startYear = 2022
AND ('actor' IN p1.primaryProfession OR 'actress' IN p1.primaryProfession)
AND ('actor' IN p2.primaryProfession OR 'actress' IN p2.primaryProfession)
WITH DISTINCT
    CASE WHEN p1.primaryName < p2.primaryName THEN p1.primaryName ELSE p2.primaryName END AS person1,
    CASE WHEN p1.primaryName < p2.primaryName THEN p2.primaryName ELSE p1.primaryName END AS person2,
    COUNT(DISTINCT m) AS collab_count,
    COLLECT(DISTINCT m.primaryTitle) AS movie_list

RETURN person1, person2, collab_count, movie_list
ORDER BY collab_count DESC
"""

df_collab = (
    spark.read.format("org.neo4j.spark.DataSource")
    .option("url", url)
    .option("authentication.basic.username", username)
    .option("authentication.basic.password", password)
    .option("database", dbname)

    # cypher query to find ppl that have coworked/shared a movie in a specific year
    .option("query", query_collab_test)
    .load()
)

# execution start time
collab_start = time.time()

df_collab.show(truncate=False) # full list display
year_collab_test = df_collab.count()
print(year_collab_test)
collab_end = time.time()

print(collab_end - collab_start) # obtain duration

