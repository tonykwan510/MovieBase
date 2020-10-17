# Filter reviews based on metadata and review counts
import json, os
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from pyspark.sql.functions import broadcast, col

# Read Product IDs (asin) from metadata
infile = '../data/Amazon/amazon_meta.json'
asin_list = []
for line in open(infile, 'r'):
	item = json.loads(line.strip())
	asin_list.append(item['asin'])

region = os.getenv('AWS_REGION')
bucket = os.getenv('AWS_BUCKET')
src = '00_Amazon_data/amazon_reviews.json.gz'
desc = '00_Amazon_data/amazon_reviews_select'

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

# Keep products with at least 100 reviews
count_df = review_df.groupBy('asin').count() \
	.filter(col('count') >= 100) \
	.join(asin_df, on='asin', how='inner')

# Filter reviews
review_df = review_df.join(count_df.drop(col('count')), on='asin', how='inner')

# Output filtered reviews to S3
path = f's3a://{bucket}/{desc}.parquet'
review_df.coalesce(16).write.parquet(path)

# Output filtered product list
count_df = count_df.toPandas()
spark.stop()

outfile = '../data/Amazon/review_count.csv'
count_df.to_csv(outfile, index=False)
