from flask import Blueprint, render_template, request, current_app, jsonify
from models.data_handler import get_AvgRatingFor10MinMovie, get_AvgRatingByGenreDecade, get_TopGenresByDecade, get_mostPopularGenreActorDirector, predict_score, get_AvgRatingActorDirector, get_MostCollabPeople, get_most_popular_genre, get_rating_trends, get_graph

main_controller = Blueprint('main', __name__)

@main_controller.route('/')
def index():
    return render_template('index.html')

PAGES = [
    {"name": "Home", "url": "/"},
    {"name": "Average Ratings by 10-Minute Interval", "url": "/AvgRatingFor10MinMovie_route"},
    {"name": "Average Ratings by Genre and Decade", "url": "/AvgRatingByGenreDecade_route"},
    {"name": "Top Genres by Decade", "url": "/TopGenresByDecade_route"},
    {"name": "Average Rating for Actor and Director", "url": "/average_rating_for_actor_director_route"},
    {"name": "Predict Score for Hypothetical Movie", "url": "/predictor"},
    {"name": "Most Popular Genre For Each Actor/Actress and Director", "url": "/mostPopularGenreActorDirector_route"},
    {"name": "Individual Statistics", "url": "/individualStatistics_route"},
    {"name": "Most Collabs", "url": "/MostCollabPeople_route"},
    {"name": "Search Graph", "url": "/search_graph"}
]

@main_controller.route('/search', methods=['GET'])
def search():
    keyword = request.args.get('q', '').lower()
    results = [page for page in PAGES if keyword in page["name"].lower()]
    return jsonify(results)

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

@main_controller.route('/mostPopularGenreActorDirector_route')
def mostPopularDenreActorDirector():
    mostPopularDenreActorDirector_data = get_mostPopularGenreActorDirector()
    return render_template('mostPopularGenreActorDirector.html', title="Most Popular Genre For Each Actor/Actress and Director", data=mostPopularDenreActorDirector_data)


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

@main_controller.route('/individualStatistics_route', methods=['GET', 'POST'])
def individual_statistics():
    # Get person name from request arguments
    if request.method == 'POST':
        person_name = request.form.get('person_name')
        genre = get_most_popular_genre(person_name)
        rating_trends = get_rating_trends(person_name)

        # Check if the result is None
        if genre is None:
            message = f"No data found for {person_name}."
            return render_template('individualStatistics.html', message=message, person_name=person_name)
        # If data is found
        else:
            return render_template('individualStatistics.html', genre=genre, rating_trends=rating_trends, person_name=person_name)
    return render_template('individualStatistics.html')

all_genres = ['Crime', 'Romance', 'Thriller', 'Adventure', 'Drama', 'War', 'Documentary', 'Family', 'Fantasy', 'Adult', 'History', 'Mystery', 'Musical', 'Animation', 'Music', 'Film-Noir', 'Horror', 'Western', 'Biography', 'Comedy', 'Action', 'Sport', 'Sci-Fi', 'News']

@main_controller.route("/predictor", methods=["GET", "POST"])
def predictor_logic():
    if request.method == "POST":
        spark = current_app.config['SPARK_SESSION']

        # Retrieve form data
        genres = request.form.getlist("genres")  # Gets list of selected genres
        runtime = request.form.get("runtime")
        release_year = request.form.get("release_year")

        if not runtime:
            runtime = 120

        if not release_year:
            release_year = 2000

        movie_dict = {
            "genres": genres,
            "runtimeMinutes": runtime,
            "startYear": release_year
        }

        prediction = predict_score(movie_dict, spark)

        return render_template("Predictor.html", genres=all_genres, result=prediction)

    return render_template("Predictor.html", genres=all_genres, result=None)

all_professions = ['actor', 'actress', 'director']
@main_controller.route('/MostCollabPeople_route', methods=["GET", "POST"])
def MostCollabPeople():
    if request.method == "POST":
        spark = current_app.config['SPARK_SESSION']

        year = request.form['year']
        selected_professions = request.form.getlist('professions')
        collabs = get_MostCollabPeople(year, selected_professions, spark)

        if collabs is None:
            message = "No collaborations found please try again."
            return render_template('MostCollabPeople.html', message=message, professions=all_professions)

        else:
            return render_template('MostCollabPeople.html', results=collabs, professions=all_professions)

    return render_template('MostCollabPeople.html', title="People with the most movie collaborations", professions=all_professions)

@main_controller.route('/search_graph', methods=['GET', 'POST'])
def search_graph():
    if request.method == 'POST':
        search_query = request.form.get('search_query')
        search_type = request.form.get('search_type')
        nNodes = request.form.get('num_nodes')

        path = get_graph(search_query, search_type, int(nNodes))
        return render_template('searchGraph.html', image_path=path)
    return render_template("searchGraph.html")
