from algorithms.predictor_trainer import prepare_data

def predict_movie(prediction_model, movie_dict, spark):
    #prediction_model = NaiveBayesModel.load(model_path)

    all_genres = ['Crime', 'Romance', 'Thriller', 'Adventure', 'Drama', 'War', 'Documentary', 'Family', 'Fantasy', 'Adult', 'History', 'Mystery', 'Musical', 'Animation', 'Music', 'Film-Noir', 'Horror', 'Western', 'Biography', 'Comedy', 'Action', 'Sport', 'Sci-Fi', 'News']
    movie_data = prepare_data(spark.createDataFrame([movie_dict]), include_rating=False, all_genres=all_genres)
    movie_vector = movie_data.select("features").na.drop()

    predictions = prediction_model.transform(movie_vector)
    prediction = predictions.select("prediction").first()[0]
    #prediction = 1.0

    results_output = [
        "This movie is estimated to get an average score of 0-4! Probably not the best...",
        "This movie is estimated to get an average score of 4-7! It might be worth watching.",
        "This movie is estimated to get an average score of 7-10! Sounds like a great watch!"
    ]

    return results_output[int(prediction)]
