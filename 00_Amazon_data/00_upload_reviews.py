# Upload Amazon review data to S3
import os, boto3

region = os.getenv('AWS_REGION')
bucket = os.getenv('AWS_BUCKET')

src = '../data/Amazon/Movies_and_TV.json.gz'
desc = '00_Amazon_data/amazon_reviews.json.gz'

s3 = boto3.client('s3', region_name=region)
s3.upload_file(Filename=src, Bucket=bucket, Key=desc)
