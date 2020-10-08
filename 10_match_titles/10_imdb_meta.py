# Filter IMDb metadata
# - Keep only titles belonging to certain types
# - Discard certain single-genre titles
# - Discard titles with no year information
# - Keep titles up to 2018 (Amazon reviews are up to 2018)
import gzip

infile = '../data/IMDb/title.akas.tsv.gz'

titles = set()
k = ntitle = 0
print('Processing metadata 1...', end='', flush=True)
for line in gzip.open(infile, 'rt'):
	k += 1
	if k % 500000 == 0: print('.', end='', flush=True)

	fields = line.strip().split('\t')

	# Get headers from first line
	if k == 1:
		headers = fields
		continue

	movie = dict(zip(headers, fields))

	# Filter by region and language
	if movie['region'] == 'US' and movie['language'] in ('en', '\\N'):
		ntitle += 1
		titles.add(movie['titleId'])
print('Done')
print('Number of titles =', ntitle)

infile = '../data/IMDb/title.basics.tsv.gz'
outfile = '../data/IMDb/imdb_meta.tsv.gz'

title_types = ['movie', 'tvMovie']
skip_genres = ['\\N', 'Documentary', 'Game-Show', 'Music', 'Musical', 'News', 'Reality-TV', 'Sport', 'Talk-Show']

with gzip.open(outfile, 'wt') as op:
	k = ntitle = 0
	print('Processing metadata 2...', end='', flush=True)
	for line in gzip.open(infile, 'rt'):
		k += 1
		if k % 500000 == 0: print('.', end='', flush=True)

		fields = line.strip().split('\t')

		# Get headers from first line
		if k == 1:
			headers = fields
			op.write(line)
			continue

		movie = dict(zip(headers, fields))

		# Filter by genre
		if movie['genres'] in skip_genres or movie['startYear'] == '\\N': continue

		# Filter by title type and year
		if movie['titleType'] in title_types and int(movie['startYear']) <= 2018 and movie['tconst'] in titles:
			ntitle += 1
			op.write(line)

	print('Done')
	print('Number of titles =', ntitle)
