# Web server
import os
from flask import Flask, redirect, render_template, request, url_for, flash
from sqlalchemy.ext.automap import automap_base
from sqlalchemy import desc
from flask_sqlalchemy import SQLAlchemy

app = Flask(__name__)
app.config['SECRET_KEY'] = __name__
app.config['MOVIE_PER_PAGE'] = 10
app.config['REVIEW_PER_PAGE'] = 10

# Connect to database
host = os.getenv('MYSQL_HOST')
user = os.getenv('MYSQL_USER')
password = os.getenv('MYSQL_PASSWORD')
database = 'reviews'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['SQLALCHEMY_DATABASE_URI'] = f'mysql://{user}:{password}@{host}/{database}'
db = SQLAlchemy(app)
session = db.session

Base = automap_base()
Base.prepare(db.engine, reflect=True)
movies = Base.classes.movies
reviews = Base.classes.amazon_reviews

# Read options for drop-down lists
with db.engine.connect() as conn:
	result = conn.execute('SELECT DISTINCT year from movies ORDER BY year DESC')
	years = [row.year for row in result]

	result = conn.execute('SELECT id, name from titleTypes')
	titleTypes = {row.id: row.name for row in result}

	result = conn.execute('SELECT id, name from genres')
	genres = {f'{row.id:02d}': row.name for row in result}

options = {
	'years': years,
	'titleTypes': titleTypes,
	'genres': genres,
	'sortkeys': {'amazon_rating': 'Amazon rating', 'imdb_rating': 'IMDb rating'}
}

@app.route('/', methods=['GET', 'POST'])
def main():
	return redirect(url_for('show_movie'))

@app.route('/movie', methods=['GET', 'POST'])
def show_movie():
	if not request.args:
		return render_template('movie.html', options=options)

	# Construct query
	query = session.query(movies)
	title = request.args.get('title', None)
	if title: query = query.filter(movies.title.like(f'%{title}%'))
	year = request.args.get('year', None, type=int)
	if year: query = query.filter_by(year=year)
	genre = request.args.get('genre', None)
	if genre: query = query.filter(movies.genres.like(f'%{genre}%'))
	sortkey = request.args.get('sortkey', None)
	if not sortkey: sortkey = list(options['sortkeys'].keys())[0]
	query = query.order_by(desc(sortkey))

	# Pagination
	nitem = app.config['MOVIE_PER_PAGE']
	page = request.args.get('page', 1, type=int)
	result = query.paginate(page, nitem, False)

	args = request.args.copy()
	if result.has_prev:
		args['page'] = page - 1
		prev_url = url_for('show_movie', **args)
	else:
		prev_url = None

	if result.has_next:
		args['page'] = page + 1
		next_url = url_for('show_movie', **args)
	else:
		next_url = None

	return render_template('movie.html', options=options,
		result=result, prev_url=prev_url, next_url=next_url)

@app.route('/review', methods=['GET', 'POST'])
def show_review():
	movie_id = request.args.get('movie', None, type=int)
	if not movie_id: return redirect(url_for('show_movie'))

	# Read movie information from database
	movie = session.query(movies).get(movie_id)

	# Construct query
	query = session.query(reviews).filter_by(movie_id=movie_id)
	keyword = request.args.get('keyword', None)
	if keyword: query = query.filter(reviews.reviewText.like(f'%{keyword}%'))

	# Pagination
	nitem = app.config['REVIEW_PER_PAGE']
	page = request.args.get('page', 1, type=int)
	result = query.paginate(page, nitem, False)

	args = request.args.copy()
	if result.has_prev:
		args['page'] = page - 1
		prev_url = url_for('show_review', **args)
	else:
		prev_url = None

	if result.has_next:
		args['page'] = page + 1
		next_url = url_for('show_review', **args)
	else:
		next_url = None

	return render_template('review.html', genres=genres, movie=movie,
		result=result, prev_url=prev_url, next_url=next_url)

if __name__ == '__main__':
	app.run(host='0.0.0.0')
