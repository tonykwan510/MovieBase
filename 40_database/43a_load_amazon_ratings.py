# Compute Amazon ratings
import os
from sqlalchemy import create_engine

# Add column to database table
def add_column(conn, table, col, dtype):
	sql = f"SELECT COUNT(*) FROM information_schema.columns" + \
		f" WHERE table_name = '{table}' AND column_name = '{col}';"
	result = conn.execute(sql)
	if next(result)[0] > 0: return

	sql = f'ALTER TABLE {table} ADD COLUMN {col} {dtype}';
	conn.execute(sql)
	return

host = os.getenv('MYSQL_HOST')
user = os.getenv('MYSQL_USER')
password = os.getenv('MYSQL_PASSWORD')
database = 'reviews'

engine = create_engine(f'mysql://{user}:{password}@{host}/{database}')
with engine.connect() as conn:
	# Create columns in movies table if not exist
	add_column(conn, 'movies', 'amazon_rating', 'DECIMAL(4,2)')

	# Fill rating column of movies table
	sql = 'UPDATE movies,' + \
		' (SELECT movie_id, AVG(overall) AS rating' + \
		' FROM amazon_reviews' + \
		' GROUP BY movie_id) AS t2' + \
		' SET movies.amazon_rating = t2.rating' + \
		' WHERE movies.id = t2.movie_id;'
	conn.execute(sql)
