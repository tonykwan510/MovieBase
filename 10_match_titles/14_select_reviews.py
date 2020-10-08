# Filter reviews based on matched metadata
import os
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from pyspark.sql.functions import col

# Read Product IDs (asin) from matched metadata
infile = '../data/amazon_imdb_match_p3.txt'
asin_list = []
for line in open(infile, 'r'):
	asin = line.split()[0]
	asin_list.append(asin)

region = os.getenv('AWS_REGION')
bucket = os.getenv('AWS_BUCKET')
src = '00_Amazon_data/amazon_reviews_select'
desc = '00_Amazon_data/amazon_reviews_match'

sc = SparkContext()
sc.setLogLevel('ERROR')
sc._jsc.hadoopConfiguration().set('fs.s3a.endpoint', f's3-{region}.amazonaws.com')

# Broadcast data to workers
asin_list = sc.broadcast(asin_list).value

spark = SparkSession(sc)

# Read raw reviews into Dataframe
path = f's3a://{bucket}/{src}'
review_df = spark.read.json(path)

# Convert Product ID list to DataFrame
asin_df = spark.createDataFrame(asin_list, StringType()).select(col('value').alias('asin'))

# Join DataFrames
df = review_df.join(asin_df, on='asin', how='inner')

# Output to S3
path = f's3a://{bucket}/{desc}'
df.coalesce(16).write.json(path, compression='gzip')

spark.stop()
