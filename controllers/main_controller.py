from flask import Blueprint, render_template, request, current_app
from models.data_handler import get_AvgRatingFor10MinMovie, get_AvgRatingByGenreDecade, get_TopGenresByDecade, predict_score

main_controller = Blueprint('main', __name__)

@main_controller.route('/')
def index():
    return render_template('index.html')

@main_controller.route('/AvgRatingFor10MinMovie_route')
def AvgRatingFor10MinMovie():
    AvgRatingFor10MinMovie_data = get_AvgRatingFor10MinMovie()
    return render_template('AvgRatingFor10MinMovie.html', title="Average Ratings by Genre and Decade", data=AvgRatingFor10MinMovie_data)

@main_controller.route('/AvgRatingByGenreDecade_route')
def AvgRatingByGenreDecade():
    AvgRatingByGenreDecade_data = get_AvgRatingByGenreDecade()
    return render_template('AvgRatingByGenreDecade.html', title="Average Ratings of Genres by Decade", data=AvgRatingByGenreDecade_data)

@main_controller.route('/TopGenresByDecade_route')
def TopGenresByDecade():
    TopGenresByDecade_data = get_TopGenresByDecade()
    return render_template('AvgRatingByGenreDecade.html', title="Top Genres by Decade", data=TopGenresByDecade_data)

all_genres = ['Crime', 'Romance', 'Thriller', 'Adventure', 'Drama', 'War', 'Documentary', 'Family', 'Fantasy', 'Adult', 'History', 'Mystery', 'Musical', 'Animation', 'Music', 'Film-Noir', 'Horror', 'Western', 'Biography', 'Comedy', 'Action', 'Sport', 'Sci-Fi', 'News']

@main_controller.route("/predictor", methods=["GET", "POST"])
def predictor_logic():
    if request.method == "POST":
        spark = current_app.config['SPARK_SESSION']

        # Retrieve form data
        genres = request.form.getlist("genres")  # Gets list of selected genres
        runtime = request.form.get("runtime")
        release_year = request.form.get("release_year")

        movie_dict = {
            "genres": genres,
            "runtimeMinutes": runtime,
            "startYear": release_year
        }

        prediction = predict_score(movie_dict, spark)

        return render_template("Predictor.html", genres=all_genres, result=prediction)

    return render_template("Predictor.html", genres=all_genres, result=None)