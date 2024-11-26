from neo4j import GraphDatabase
import matplotlib.pyplot as plt
import csv
import sys
import os


# 1. Average rating for an actor + director
def average_rating_for_actor_director(actor_name, director_name):
    query = f"""
    MATCH (actor:Person {{primaryName: '{actor_name}'}})-[:WORKED_ON]->(m:Movie)<-[:WORKED_ON]-(director:Person {{primaryName: '{director_name}'}})
    RETURN avg(m.averageRating) as avg_rating
    """
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from algorithms.credentialHandler import getCredentials
    url, username, password = getCredentials()
    driver = GraphDatabase.driver(url, auth=(username, password))
    with driver.session() as session:
        output = session.run(query)
        result = output.data()
    return result[0]["avg_rating"] if result else None

# 2. Rating trends over time for actors and directors
def rating_trends_over_time(person_name):
    # Define the query
    query = f"""
    MATCH (p:Person {{primaryName: '{person_name}'}})-[:WORKED_ON]->(m:Movie)
    RETURN m.startYear AS year, avg(m.averageRating) AS avg_rating
    ORDER BY year
    """
    
    # Import credentials and establish a Neo4j connection
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from algorithms.credentialHandler import getCredentials
    url, username, password = getCredentials()
    driver = GraphDatabase.driver(url, auth=(username, password))
    
    # Execute the query
    with driver.session() as session:
        output = session.run(query)
        result = output.data()

    # Extract data from query result
    trends = [(row["year"], row["avg_rating"]) for row in result]
    
    # Plot the data
    if trends:  # Check if there is data to plot
        years, avg_ratings = zip(*trends)  # Unzip the data
        plt.figure(figsize=(10, 6))
        plt.plot(years, avg_ratings, marker='o', linestyle='-', color='blue')
        plt.title(f'Rating Trends Over Time for {person_name}')
        plt.xlabel('Year')
        plt.ylabel('Average Rating')
        plt.grid(True)
        plt.tight_layout()
        
        # Save the plot as a PNG file
        output_dir = os.path.join("static", "plots")
        os.makedirs(output_dir, exist_ok=True)  # Create the directory if it doesn't exist
        plot_path = os.path.join(output_dir, "rating_trends.png")
        plt.savefig(plot_path)
        plt.close()  # Close the plot to free memory
        print(f"Plot saved to {plot_path}")
    else:
        print("No data available for the plot.")
    
    return trends

# 3. Most popular genre for an actor or director
def most_popular_genre(person_name):
    query = f"""
    MATCH (p:Person {{primaryName: '{person_name}'}})-[:WORKED_ON]->(m:Movie)
    UNWIND m.genres AS genre
    RETURN genre, avg(m.averageRating) AS avg_rating
    ORDER BY avg_rating DESC
    LIMIT 1
    """
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from algorithms.credentialHandler import getCredentials
    url, username, password = getCredentials()
    driver = GraphDatabase.driver(url, auth=(username, password))
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
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from algorithms.credentialHandler import getCredentials
    url, username, password = getCredentials()
    driver = GraphDatabase.driver(url, auth=(username, password))
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
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from algorithms.credentialHandler import getCredentials
    url, username, password = getCredentials()
    driver = GraphDatabase.driver(url, auth=(username, password))   
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
