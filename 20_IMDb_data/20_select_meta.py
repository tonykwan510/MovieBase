# Keep only matched IMDb metadata
import gzip

infile = '../data/amazon_imdb_match.csv'
keys = set()
for line in open(infile, 'r'):
	key = line.strip().split(',')[1]
	keys.add(key)

infile = '../data/IMDb/imdb_meta.tsv.gz'
outfile = '../data/IMDb/imdb_meta_match.tsv'

with open(outfile, 'w') as op:
	k = ntitle = 0
	for line in gzip.open(infile, 'rt'):
		k += 1
		fields = line.strip().split('\t')

		# Get headers from first line
		if k == 1:
			headers = fields
			op.write(line)
			continue

		movie = dict(zip(headers, fields))
		if movie['tconst'] in keys:
			ntitle += 1
			op.write(line)

	print('Number of titles =', ntitle)
