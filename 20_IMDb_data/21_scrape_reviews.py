import requests
from scrapy import Selector
import pandas as pd
import os, boto3, s3fs

def scrape_imdb_reviews(imdb_id):
	base = f'https://www.imdb.com/title/{imdb_id}/reviews/_ajax?spoiler=hide&sort=submissionDate&paginationKey='
	reviews = []
	key = ''
	while key is not None:
		url = base + key
		sel = Selector(text=requests.get(url).content)
		items = sel.xpath('//div[@class="review-container"]')
		for item in items:
			review = {
				'score': item.xpath('.//span[@class="rating-other-user-rating"]/span/text()').get(),
				'reviewID': int(item.xpath('.//a[@class="title"]/@href').get().split('/')[2][2:]),
				'title': item.xpath('.//a[@class="title"]/text()').get().strip(),
				'userID': int(item.xpath('.//span[@class="display-name-link"]//@href').get().split('/')[2][2:]),
				'user': item.xpath('.//span[@class="display-name-link"]//text()').get(),
				'date': item.xpath('.//span[@class="review-date"]/text()').get(),
				'review': item.xpath('.//div[@class="text show-more__control"]/text()').get(),
				'helpful': '/'.join(item.xpath('.//div[@class="actions text-muted"]/text()').get().split()[0:4:3])
			}
			reviews.append(review)
		key = sel.xpath('//div[@class="load-more-data"]/@data-key').get()
	return reviews

# Read matched IMDb titles
movies = []
k = 0
for line in open('../data/IMDb/imdb_meta_match.tsv', 'r'):
	k += 1
	fields = line.strip().split('\t')
	if k == 1:
		headers = fields
		continue
	movie = dict(zip(headers, fields))
	movies.append((movie['tconst'], movie['startYear']))

# S3 client
region = os.getenv('AWS_REGION')
bucket = os.getenv('AWS_BUCKET')
desc = f'20_IMDb_data/imdb_reviews_match'
s3 = boto3.client('s3', region_name=region)

s3a = s3fs.S3FileSystem()
for imdb_id, year in movies:
	# Check if file already exists
	prefix = f'{desc}/{year}/{imdb_id}'
	reponse = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
	if reponse['KeyCount'] > 0: continue

	# Scrape reviews
	reviews = scrape_imdb_reviews(imdb_id)
	if not reviews: continue
	print(f'{imdb_id}: {len(reviews)}')

	# Write to S3
	review_df = pd.DataFrame.from_records(reviews)
	review_df.score = review_df.score.astype('float64')
	review_df.date = pd.to_datetime(review_df.date, format='%d %B %Y')
	review_df['imdb_id'] = imdb_id
	path = f's3a://{bucket}/{prefix}.parquet'
	review_df.to_parquet(path, index=False, filesystem=s3a)
