# Load Amazon reviews into database
import os, boto3, s3fs
import pandas as pd
from sqlalchemy import create_engine, Date, SmallInteger

def list_objects(s3, bucket, prefix, suffix):
	response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
	return [item['Key'] for item in response['Contents'] if item['Key'][-len(suffix):] == suffix]

# Read matched metadata
infile = '../data/amazon_imdb_match_p3.txt'
matches = []
for line in open(infile, 'r'):
	asin, imdb_ids = line.split()
	if imdb_ids.find(',') == -1:
		matches.append((asin, imdb_ids))
match_df = pd.DataFrame.from_records(matches, index='asin', columns=['asin', 'imdb_id'])

# List partitions
print('Listing partitions...')
region = os.getenv('AWS_REGION')
bucket = os.getenv('AWS_BUCKET')
prefix = '00_Amazon_data/amazon_reviews_match'
suffix = '.json.gz'
s3 = boto3.client('s3', region_name=region)
keys = list_objects(s3, bucket, prefix, suffix)
print(f'{len(keys)} partitions found.')

# Connect to database
host = os.getenv('MYSQL_HOST')
user = os.getenv('MYSQL_USER')
password = os.getenv('MYSQL_PASSWORD')
database = 'reviews'
engine = create_engine(f'mysql://{user}:{password}@{host}/{database}')

for k, key in enumerate(keys):
	# Read partition
	print(f'Reading partition ({k+1}/{len(keys)})...')
	path = f's3a://{bucket}/{key}'
	review_df = pd.read_json(path, orient='records', compression='gzip',
			convert_dates=['reviewTime'], lines=True) \
		.drop(columns=['style', 'unixReviewTime', 'vote', 'image']) \
		.join(match_df, on='asin', how='inner') \
		.drop(columns=['asin'])

	# Load partition into database
	print(f'Loading {len(review_df)} records into database...')
	if_exists = 'replace' if k == 0 else 'append'
	review_df.to_sql('amazon_reviews', engine, if_exists=if_exists, index_label='id',
		dtype={'overall': SmallInteger(), 'reviewTime': Date()})
