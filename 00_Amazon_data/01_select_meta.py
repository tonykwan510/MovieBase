# Filter metadata by categories
import gzip, json

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

asins = set()

infile = '../data/Amazon/meta_Movies_and_TV.json.gz'
outfile = '../data/Amazon/amazon_meta.json'
with open(outfile, 'w') as op:
	k = nitem = 0
	print('Processing metadata...', end='', flush=True)
	for line in gzip.open(infile, 'rt'):
		k += 1
		if k % 50000 == 0: print('.', end='', flush=True)
		movie = json.loads(line.strip())
		asin = movie['asin']

		# Skip duplicated products
		if asin in asins: continue

		# Filter by category
		if set(movie['category']).isdisjoint(categories): continue

		nitem += 1
		asins.add(asin)
		item = {field: movie[field] for field in fields}
		op.write('{}\n'.format(json.dumps(item)))

	print('Done')
	print('Before: Number of products =', k)
	print('After: Number of products =', nitem)
