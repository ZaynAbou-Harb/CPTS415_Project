from flask import Blueprint, render_template
from models.data_handler import get_AvgRatingFor10MinMovie, get_AvgRatingByGenreDecade, get_TopGenresByDecade

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
