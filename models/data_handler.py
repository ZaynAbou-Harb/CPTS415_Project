import pandas as pd
from neo4j import GraphDatabase
import pandas as pd
import os

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

def get_TopGenresByDecade():
    return read_csv_from_directory("./data/result/TopGenresByDecade.csv")
