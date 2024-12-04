import pandas as pd
from neo4j import GraphDatabase
import pandas as pd
import os
from algorithms.predictor import predict_movie
from algorithms.CypherBasedFunctions import average_rating_for_actor_director, most_popular_genre, rating_trends_over_time
from algorithms.vizualizeGraph import get_edges_and_nodes, create_and_save_custom_graph
from algorithms.collab_movies import most_collab


DATA_DIR = os.path.abspath("./data/result/")

# CSV Data access model
def read_csv_from_directory(directory_path):
    # Find the correct file in the directory
    for file in os.listdir(directory_path):
        if file.startswith("part-") and file.endswith(".csv"):
            file_path = os.path.join(directory_path, file)
            return pd.read_csv(file_path)
    raise FileNotFoundError(f"No CSV file found in directory: {directory_path}")

def get_AvgRatingFor10MinMovie():
    return read_csv_from_directory("./data/result/AvgRatingFor10MinMovie.csv")

def get_AvgRatingByGenreDecade():
    return read_csv_from_directory("./data/result/AvgRatingByGenreDecade.csv")

def get_mostPopularGenreActorDirector():
    return read_csv_from_directory("./data/result/mostPopularGenreActorDirector.csv")

def get_TopGenresByDecade():
    return read_csv_from_directory("./data/result/TopGenresByDecade.csv")

def predict_score(prediction_model, movie, spark_session):
    return predict_movie(prediction_model, movie, spark_session)

def get_AvgRatingActorDirector(actor_name, director_name):
    return average_rating_for_actor_director(actor_name, director_name)

def get_most_popular_genre(person_name):
    return most_popular_genre(person_name)

def get_rating_trends(person_name):
    return rating_trends_over_time(person_name)

def get_graph(search_query, search_type, nNodes):
    graph_data = get_edges_and_nodes(search_query, search_type, nNodes)
    create_and_save_custom_graph(graph_data,search_query, search_type, nNodes, "static/plots/graph.png")
    return "static/plots/graph.png"

def get_most_popular_genre(person_name):
    return most_popular_genre(person_name)

def get_rating_trends(person_name):
    return rating_trends_over_time(person_name)

def get_graph(search_query, search_type, nNodes):
    graph_data = get_edges_and_nodes(search_query, search_type, nNodes)
    create_and_save_custom_graph(graph_data,search_query, search_type, nNodes, "static/plots/graph.png")
    return "static/plots/graph.png"

def get_MostCollabPeople(year, professions, spark):
   return most_collab(year, professions, spark)