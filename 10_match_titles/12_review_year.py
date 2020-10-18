# Extract year information from reviews
import os
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
import pyspark.sql.functions as F

# Read matched Product IDs
infile = '../data/amazon_imdb_match.txt'
asin_list = []
for line in open(infile, 'r'):
	asin = line.split()[0]
	asin_list.append(asin)

region = os.getenv('AWS_REGION')
bucket = os.getenv('AWS_BUCKET')
src = '00_Amazon_data/amazon_reviews_select'

sc = SparkContext()
sc.setLogLevel('ERROR')
sc._jsc.hadoopConfiguration().set('fs.s3a.endpoint', f's3-{region}.amazonaws.com')

# Broadcast data to workers
asin_list = sc.broadcast(asin_list).value

spark = SparkSession(sc)

# Convert Product ID list to DataFrame
asin_df = spark.createDataFrame(asin_list, StringType()).select(F.col('value').alias('asin'))

# Read reviews into Dataframe, join, and extract year
path = f's3a://{bucket}/{src}.parquet'
review_df = spark.read.parquet(path).select('asin', 'unixReviewTime') \
	.join(asin_df, on='asin', how='inner') \
	.withColumn('year', F.year(F.from_unixtime('unixReviewTime'))) \
	.drop('unixReviewTime')

# Minimum year
year_df = review_df.groupBy('asin').min('year').toPandas()

spark.stop()

outfile = '../data/Amazon/review_year.csv'
year_df.to_csv(outfile, index=False)
