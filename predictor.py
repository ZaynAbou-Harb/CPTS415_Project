from pyspark.sql import SparkSession
from pyspark.ml.classification import NaiveBayesModel

from pyspark.ml.linalg import SparseVector
from pyspark.sql import Row

from predictor_trainer import prepare_data, model_path

spark = SparkSession.builder.getOrCreate()

prediction_model = NaiveBayesModel.load(model_path)
print("Model loaded successfully.")

test_movie = {
    "genres": ["Action","Adventure"],
    "runtimeMinutes": 150,
    "startYear": 2024
}

#all_genres = ['Crime', 'Romance', 'Thriller', 'Adventure', 'Drama', 'War', 'Documentary', 'Family', 'Fantasy', 'Adult', 'History', 'Mystery', 'Musical', 'Animation', 'Music', 'Film-Noir', 'Horror', 'Western', 'Biography', 'Comedy', 'Action', 'Sport', 'Sci-Fi', 'News']
#movie_data = prepare_data(spark.createDataFrame([test_movie]), include_rating=False, all_genres=all_genres)
#movie_vector = movie_data.select("features").na.drop()

#print(movie_vector.take(1)[0].asDict())

data = [
    Row(features=SparseVector(26, {0: 1.0, 1: 1.0, 2: 1.0, 24: 2.0, 25: 4.0}))
]

predictions = prediction_model.transform(spark.createDataFrame(data))
predictions = predictions.select("prediction")
predictions.write.csv("predictions.csv", header=True)

#print(f"This movie's estimated rating is: {prediction}")
