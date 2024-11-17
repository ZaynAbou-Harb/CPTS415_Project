from neo4j import GraphDatabase
import csv

# 1. Average rating for an actor + director
def average_rating_for_actor_director(actor_name, director_name):
    query = f"""
    MATCH (actor:Person {{primaryName: '{actor_name}'}})-[:WORKED_ON]->(m:Movie)<-[:WORKED_ON]-(director:Person {{primaryName: '{director_name}'}})
    RETURN avg(m.averageRating) as avg_rating
    """
    driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "neo4j"))
    with driver.session() as session:
        output = session.run(query)
        result = output.data()
    return result[0]["avg_rating"] if result else None

# 2. Rating trends over time for actors and directors
def rating_trends_over_time(person_name):
    query = f"""
    MATCH (p:Person {{primaryName: '{person_name}'}})-[:WORKED_ON]->(m:Movie)
    RETURN m.startYear AS year, avg(m.averageRating) AS avg_rating
    ORDER BY year
    """
    driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "neo4j"))
    with driver.session() as session:
        output = session.run(query)
        result = output.data()
    return [(row["year"], row["avg_rating"]) for row in result]

# 3. Most popular genre for an actor or director
def most_popular_genre(person_name):
    query = f"""
    MATCH (p:Person {{primaryName: '{person_name}'}})-[:WORKED_ON]->(m:Movie)
    UNWIND m.genres AS genre
    RETURN genre, avg(m.averageRating) AS avg_rating
    ORDER BY avg_rating DESC
    LIMIT 1
    """
    driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "neo4j"))
    with driver.session() as session:
        output = session.run(query)
        result = output.data()
    return result[0]["genre"] if result else None

# 4. Top-rated actor + director average
def top_rated_actor_director_avg():
    query = """
    MATCH (actor:Person)-[:WORKED_ON]->(m:Movie)<-[:WORKED_ON]-(director:Person)
    WITH actor, director, avg(m.averageRating) AS avg_rating
    ORDER BY avg_rating DESC
    LIMIT 1
    RETURN actor.primaryName AS actor_name, director.primaryName AS director_name, avg_rating
    """
    driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "neo4j"))
    with driver.session() as session:
        output = session.run(query)
        result = output.data()
    return result[0] if result else None

# 5. Most popular genre for each actor or director
def most_popular_genre_for_all():
    query = """
    MATCH (p:Person)-[:WORKED_ON]->(m:Movie)
    UNWIND m.genres AS genre
    WITH p.primaryName AS person, genre, m.averageRating AS rating
    WHERE rating IS NOT NULL  // Exclude movies without ratings
    WITH person, genre, avg(rating) AS avg_rating
    ORDER BY person, avg_rating DESC
    WITH person, collect({genre: genre, avg_rating: avg_rating}) AS genre_ratings
    RETURN person, genre_ratings[0].genre AS most_popular_genre, genre_ratings[0].avg_rating AS avg_rating
    """
    driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "neo4j"))
    with driver.session() as session:
        output = session.run(query)
        result = output.data()
    
    processed_data = [
        {
            "person": record["person"], 
            "most_popular_genre": record["most_popular_genre"], 
            "avg_rating": record["avg_rating"]
        } 
        for record in result
    ]
    
    csv_file = "mostPopularGenreActorDirector.csv"
    with open(csv_file, mode="w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=["person", "most_popular_genre", "avg_rating"])
        writer.writeheader()
        writer.writerows(processed_data)
    
    print(f"Data successfully written to {csv_file}")


# Example usages of functions
if __name__ == "__main__":
    print("Average Rating for Actor + Director:", average_rating_for_actor_director("Tom Cruise", "Joseph Kosinski"))
    print("Rating Trends Over Time:", rating_trends_over_time("Tom Cruise"))
    
    name = "Tom Cruise"
    popular_genre = most_popular_genre(name)
    if popular_genre:
        print(f"The most popular genre for {name} based on average rating is {popular_genre}.")
    else:
        print(f"No genre data found for {name}.")

    
    #print("Top Rated Actor + Director Average:", top_rated_actor_director_avg()) # This command was replaced with spark version (See topRatedActorDirectorAvg.py)
    
    most_popular_genre_for_all()