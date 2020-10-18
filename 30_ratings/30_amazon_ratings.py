# Compute average rating for each title
import os
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from pyspark.sql.functions import col

# Read matched metadata
infile = '../data/amazon_imdb_match_p3.txt'
matches = []
for line in open(infile, 'r'):
	asin, s = line.split()
	imdb_ids = s.split(',')
	if len(imdb_ids) == 1:
		matches.append((asin, imdb_ids[0]))

region = os.getenv('AWS_REGION')
bucket = os.getenv('AWS_BUCKET')
src = '00_Amazon_data/amazon_reviews_match'

sc = SparkContext()
sc.setLogLevel('ERROR')
sc._jsc.hadoopConfiguration().set('fs.s3a.endpoint', f's3-{region}.amazonaws.com')

# Broadcast data to workers
matches = sc.broadcast(matches).value

spark = SparkSession(sc)

# Read raw reviews into Dataframe
path = f's3a://{bucket}/{src}.parquet'
review_df = spark.read.parquet(path).select('asin', 'overall')

# Convert Product ID list to DataFrame
match_df = spark.createDataFrame(matches, ['asin', 'imdb_id'])

# Compute ratings
rating_df = review_df.join(match_df, on='asin', how='inner') \
	.groupby('imdb_id').avg() \
	.toPandas()

spark.stop()

# Output
rating_df.sort_values(by='imdb_id') \
	.to_csv('../data/amazon_ratings.csv', header=False, index=False)
