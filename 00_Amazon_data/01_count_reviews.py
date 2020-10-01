# Use Spark to count number of reviews per product
import os
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc

region = os.getenv('AWS_REGION')
bucket = os.getenv('AWS_BUCKET')
key = '00_Amazon_data/amazon_reviews.json.gz'
outfile = '../data/Amazon/review_count.csv'

sc = SparkContext()
sc.setLogLevel('ERROR')
sc._jsc.hadoopConfiguration().set('fs.s3a.endpoint', f's3-{region}.amazonaws.com')
spark = SparkSession(sc)

review_df = spark.read.json(f's3a://{bucket}/{key}')
count_df = review_df.groupBy('asin').count().sort(col('count').desc()).toPandas()
count_df.to_csv(outfile, index=False)

spark.stop()
