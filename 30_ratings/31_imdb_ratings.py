# Extract IMDb ratings of matched titles
import gzip

infile = '../data/amazon_imdb_match_p3.txt'
keys = set()
for line in open(infile, 'r'):
	items = line.split()[1].split(',')
	if len(items) == 1:
		keys.add(items[0])

infile = '../data/IMDb/title.ratings.tsv.gz'
outfile = '../data/imdb_ratings.csv'

with open(outfile, 'w') as op:
	k = ntitle = 0
	for line in gzip.open(infile, 'rt'):
		k += 1
		fields = line.strip().split('\t')

		# Get headers from first line
		if k == 1:
			headers = fields
			continue

		movie = dict(zip(headers, fields))
		if movie['tconst'] in keys:
			ntitle += 1
			tconst = movie['tconst']
			rating = movie['averageRating']
			op.write(f'{tconst},{rating}\n')

	print('Number of titles =', ntitle)
