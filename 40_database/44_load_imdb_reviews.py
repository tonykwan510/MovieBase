# Load IMDb reviews into database
import os, boto3, s3fs
import pandas as pd
from sqlalchemy import create_engine, Date, SmallInteger

def list_objects(s3, bucket, prefix, suffix):
	response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
	return [item['Key'] for item in response['Contents'] if item['Key'][-len(suffix):] == suffix]

# List review files
print('Listing review files...')
region = os.getenv('AWS_REGION')
bucket = os.getenv('AWS_BUCKET')
prefix = '20_IMDb_data/imdb_reviews_match'
suffix = '.parquet'
s3 = boto3.client('s3', region_name=region)
keys = list_objects(s3, bucket, prefix, suffix)
folders = sorted(list(set(key.split('/')[2] for key in keys)))
print(f'{len(keys)} files found in {len(folders)} folders.')

# Connect to database
host = os.getenv('MYSQL_HOST')
user = os.getenv('MYSQL_USER')
password = os.getenv('MYSQL_PASSWORD')
database = 'reviews'
engine = create_engine(f'mysql://{user}:{password}@{host}/{database}')

# Read movie IDs
with engine.connect() as conn:
	movie_df = pd.read_sql('SELECT id AS movie_id, imdb_id FROM movies', conn, index_col='imdb_id')

s3a = s3fs.S3FileSystem()
columns=['imdb_id', 'score', 'reviewID', 'title', 'userID', 'user', 'date', 'review']
for k, folder in enumerate(folders):
	# Read reviews
	print(f'Reading folder ({k+1}/{len(folders)})...')
	path = f's3a://{bucket}/{prefix}/{folder}'
	review_df = pd.read_parquet(path, columns=columns, filesystem=s3a) \
		.join(movie_df, on='imdb_id', how='inner') \
		.drop(columns=['imdb_id'])

	# Load reviews into database
	print(f'Loading {len(review_df)} records into database...')
	if_exists = 'replace' if k == 0 else 'append'
	review_df.to_sql('imdb_reviews', engine, if_exists=if_exists, index=False,
		dtype={'score': SmallInteger(), 'date': Date()})

	# Set primary and foreign keys
	if k == 0:
		with engine.connect() as conn:
			conn.execute('ALTER TABLE imdb_reviews ADD PRIMARY KEY (reviewID)')
			conn.execute('ALTER TABLE imdb_reviews ADD FOREIGN KEY (movie_id) REFERENCES movies(id)')
			conn.execute('ALTER TABLE imdb_reviews ADD INDEX (movie_id)')
