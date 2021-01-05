# Load match keys into database
import os
import numpy as np
import pandas as pd
from sqlalchemy import create_engine, SmallInteger

host = os.getenv('MYSQL_HOST')
user = os.getenv('MYSQL_USER')
password = os.getenv('MYSQL_PASSWORD')
database = 'reviews'

# Load match keys into DataFrame
path = '../data/IMDb/imdb_meta_match.tsv'
names = ['imdb_id', 'titleType', 'title', 'X1', 'isAdult', 'year', 'X2', 'runtime', 'genres']
usecols = [name for name in names if name[0] != 'X']
movies = pd.read_csv(path, sep='\t', header=0, names=names, usecols=usecols)

# Recode titeType
titleTypes = movies.titleType.unique()
titleType_dict = {val: k for k, val in enumerate(titleTypes)}
movies.titleType = movies.titleType.apply(lambda x: titleType_dict[x])

# Recode isAdult
movies.isAdult = movies.isAdult.apply(lambda x: x==1)

# Recode runtime
movies.loc[movies.runtime==r'\N', 'runtime'] = np.nan
movies.runtime = movies.runtime.astype('float')

# Recode genres
genres = sorted(set(genre for s in movies.genres.values for genre in s.split(',')))
genre_dict = {val: k for k, val in enumerate(genres)}
movies.genres = movies.genres.apply(lambda x: [genre_dict[genre] for genre in x.split(',')])
movie_genres = pd.DataFrame(movies.genres.explode()).rename(columns={'genres':'genre_id'})
movies.genres = movies.genres.apply(lambda x: ','.join(f'{code:02d}' for code in x))

# Load data to database
engine = create_engine(f'mysql://{user}:{password}@{host}/{database}')

pd.DataFrame(titleTypes, columns=['name']) \
	.to_sql('titleTypes', engine, if_exists='replace', index_label='id')

pd.DataFrame(genres, columns=['name']) \
	.to_sql('genres', engine, if_exists='replace', index_label='id')

movies.to_sql('movies', engine, if_exists='replace', index_label='id',
	dtype={'year': SmallInteger(), 'runtime': SmallInteger()})

movie_genres.to_sql('movie_genres', engine, if_exists='replace', index_label='movie_id')

# Set primary and foreign keys
with engine.connect() as conn:
	conn.execute('ALTER TABLE titleTypes ADD PRIMARY KEY (id)')
	conn.execute('ALTER TABLE genres ADD PRIMARY KEY (id)')
	conn.execute('ALTER TABLE movies ADD PRIMARY KEY (id)')
	conn.execute('ALTER TABLE movies ADD FOREIGN KEY (titleType) REFERENCES titleTypes(id)')
	conn.execute('ALTER TABLE movie_genres ADD PRIMARY KEY (movie_id, genre_id)')
	conn.execute('ALTER TABLE movie_genres ADD FOREIGN KEY (movie_id) REFERENCES movies(id)')
	conn.execute('ALTER TABLE movie_genres ADD FOREIGN KEY (genre_id) REFERENCES genres(id)')
	conn.execute('ALTER TABLE movie_genres ADD INDEX (movie_id)')
	conn.execute('ALTER TABLE movie_genres ADD INDEX (genre_id)')
