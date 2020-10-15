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
df = pd.read_csv(path, sep='\t', header=0, names=names, usecols=usecols)

# Recode titeType
titleTypes = df.titleType.unique()
titleType_dict = {val: k for k, val in enumerate(titleTypes)}
df.titleType = df.titleType.apply(lambda x: titleType_dict[x])

# Recode isAdult
df.isAdult = df.isAdult.apply(lambda x: x==1)

# Recode runtime
df.loc[df.runtime==r'\N', 'runtime'] = np.nan
df.runtime = df.runtime.astype('float')

# Recode genres
genres = sorted(list(set(genre for s in df.genres.values for genre in s.split(','))))
genre_dict = {val: k for k, val in enumerate(genres)}
df.genres = df.genres.apply(lambda x: ','.join(f'{genre_dict[genre]:02d}' for genre in x.split(',')))

# Load data to database
engine = create_engine(f'mysql://{user}:{password}@{host}/{database}')

pd.DataFrame(titleTypes, columns=['name']) \
	.to_sql('titleTypes', engine, if_exists='replace', index_label='id')

pd.DataFrame(genres, columns=['name']) \
	.to_sql('genres', engine, if_exists='replace', index_label='id')

df.to_sql('movies', engine, if_exists='replace', index_label='id',
	dtype={'year': SmallInteger(), 'runtime': SmallInteger()})

# Set primary and foreign keys
with engine.connect() as conn:
	conn.execute('ALTER TABLE titleTypes ADD PRIMARY KEY (id)')
	conn.execute('ALTER TABLE genres ADD PRIMARY KEY (id)')
	conn.execute('ALTER TABLE movies ADD PRIMARY KEY (id)')
	conn.execute('ALTER TABLE movies ADD FOREIGN KEY (titleType) REFERENCES titleTypes(id)')
