from pyspark.sql import SparkSession
from pyspark import RDD
from neo4j import GraphDatabase

spark = SparkSession.builder.appName("TopRatedActorDirector").getOrCreate()

def run_neo4j_query(query):
    uri = "bolt://localhost:7687"
    driver = GraphDatabase.driver(uri, auth=("neo4j", "neo4j"))
    with driver.session() as session:
        result = session.run(query)
        return [record.data() for record in result]

def top_rated_actor_director_avg():
    query = """
    MATCH (actor:Person)-[:WORKED_ON]->(m:Movie)<-[:WORKED_ON]-(director:Person)
    WHERE m.averageRating IS NOT NULL
    RETURN actor.primaryName AS actor_name, director.primaryName AS director_name, m.averageRating AS avg_rating
    """
    
    result = run_neo4j_query(query)
    
    if not result:
        return None
    
    rdd = spark.sparkContext.parallelize(result)
    
    mapped_rdd = rdd.map(lambda x: ((x['actor_name'], x['director_name']), x['avg_rating']))
    
    reduced_rdd = mapped_rdd.aggregateByKey((0, 0), 
                                             lambda acc, value: (acc[0] + value, acc[1] + 1),
                                             lambda acc1, acc2: (acc1[0] + acc2[0], acc1[1] + acc2[1]))
    
    avg_ratings_rdd = reduced_rdd.mapValues(lambda x: x[0] / x[1])
    
    top_actor_director = avg_ratings_rdd.max(key=lambda x: x[1])
    
    return top_actor_director

# Example Usage
top_pair = top_rated_actor_director_avg()
if top_pair:
    actor_director, avg_rating = top_pair
    actor_name, director_name = actor_director
    print(f"Top-rated actor-director pair: {actor_name} and {director_name} with an average rating of {avg_rating:.2f}")
else:
    print("No data found.")