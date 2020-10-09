# Load ratings into database
import os
import pandas as pd
from sqlalchemy import create_engine

host = os.getenv('MYSQL_HOST')
user = os.getenv('MYSQL_USER')
password = os.getenv('MYSQL_PASSWORD')
database = 'reviews'

# Read Amazon ratings
path = '../data/amazon_ratings.csv'
names = ['imdb_id', 'rating']
amazon_df = pd.read_csv(path, header=None, names=names)

# Read IMDb ratings
path = '../data/imdb_ratings.csv'
names = ['imdb_id', 'rating']
imdb_df = pd.read_csv(path, header=None, names=names)

# Load data to database
engine = create_engine(f'mysql://{user}:{password}@{host}/{database}')

amazon_df.to_sql('amazon_ratings', engine, if_exists='replace', index_label='id')
imdb_df.to_sql('imdb_ratings', engine, if_exists='replace', index_label='id')
