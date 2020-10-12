# Load IMDb ratings into database
import os
import pandas as pd
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

# Read IMDb ratings
path = '../data/imdb_ratings.csv'
names = ['imdb_id', 'rating']
imdb_df = pd.read_csv(path, header=None, names=names)

engine = create_engine(f'mysql://{user}:{password}@{host}/{database}')
with engine.connect() as conn:
	# Intermediate table
	imdb_df.to_sql('imdb_ratings', engine, if_exists='replace', index_label='id')

	# Create rating column in movies table if not exist
	add_column(conn, 'movies', 'imdb_rating', 'DECIMAL(4,2)')

	# Fill rating column
	sql = 'UPDATE movies, {t2}' + \
		' SET movies.{col} = {t2}.rating' + \
		' WHERE movies.imdb_id = {t2}.imdb_id;'
	conn.execute(sql.format(t2='imdb_ratings', col='imdb_rating'))

	# Drop intermediate table
	sql = 'DROP TABLE imdb_ratings;'
	conn.execute(sql)
