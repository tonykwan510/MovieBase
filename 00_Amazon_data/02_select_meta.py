# Metadata of movies with at least 100 reviews
import gzip, json

# Product with at least 100 reviews
asins = {}
infile = '../data/Amazon/review_count.csv'
k = 0
for line in open(infile, 'r'):
	k += 1
	if k == 1: continue
	asin, cnt = line.strip().split(',')
	if int(cnt) >= 100:
		asins[asin] = int(cnt)

# Categories containing movie-related products
categories = set([
	'Movies',
	'All MGM Titles',
	'All Sony Pictures Titles',
	'Cult Movies',
	'DreamWorks',
	'All BBC Titles',
	'20th Century Fox Home Entertainment',
	'Warner Home Video',
	'Miramax Home Entertainment',
	'All HBO Titles',
	'All Disney Titles',
	'Walt Disney Studios Home Entertainment',
	'Paramount Home Entertainment',
	'All Lionsgate Titles',
	'Universal Studios Home Entertainment',
	'Lionsgate Home Entertainment',
	'MGM Home Entertainment',
])

# Fields to be kept in metadata
fields = ['asin', 'category', 'title', 'description']

infile = '../data/Amazon/meta_Movies_and_TV.json.gz'
outfile = '../data/Amazon/amazon_meta.json'
with open(outfile, 'w') as op:
	k = nitem = nreview = 0
	print('Processing metadata...', end='', flush=True)
	for line in gzip.open(infile, 'rt'):
		k += 1
		if k % 50000 == 0: print('.', end='', flush=True)
		movie = json.loads(line.strip())
		asin = movie['asin']
		if asin in asins and not set(movie['category']).isdisjoint(categories):
			nitem += 1
			nreview += asins.pop(asin)
			item = {field: movie[field] for field in fields}
			op.write('{}\n'.format(json.dumps(item)))

	print('Done')
	print('Number of products =', nitem)
	print('Number of reviews =', nreview)
