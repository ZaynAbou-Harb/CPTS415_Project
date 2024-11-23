from flask import Blueprint, render_template, request, current_app
from models.data_handler import get_AvgRatingFor10MinMovie, get_AvgRatingByGenreDecade, get_TopGenresByDecade, predict_score
from algorithms.CypherBasedFunctions import average_rating_for_actor_director

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

@main_controller.route('/average_rating_for_actor_director_route', methods=['GET', 'POST'])
def AvgRatingActorDirector():
    # Get actor and director names from request arguments
    if request.method == 'POST':
        actor_name = request.form.get('actor_name')
        director_name = request.form.get('director_name')
        avg_rating = get_AvgRatingActorDirector(actor_name, director_name)
        
        # Check if the result is None
        if avg_rating is None:
            message = f"No data found for Actor: {actor_name} and Director: {director_name}."
            return render_template('AvgRatingActorDirector.html', message=message, actor_name=actor_name, director_name=director_name)
        # If data is found
        else:   
            return render_template('AvgRatingActorDirector.html', avg_rating=avg_rating, actor_name=actor_name, director_name=director_name)
    return render_template('AvgRatingActorDirector.html')

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
