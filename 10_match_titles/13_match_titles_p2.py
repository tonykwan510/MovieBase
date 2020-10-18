# Match Amazon and IMDb titles
# Part 2: Filter matches by review year
import gzip, json, re

# Load year information of IMDb titles
def load_imdb_meta(path, key, val):
	meta = {}
	k = 0
	for line in gzip.open(path, 'rt'):
		fields = line.strip().split('\t')
		k += 1
		if k == 1:
			headers = fields
			continue
		item = dict(zip(headers, fields))
		meta[item[key]] = int(item[val])
	return meta

# Load review years
def load_review_years(path):
	years = {}
	k = 0
	for line in open(path, 'r'):
		k += 1
		if k == 1: continue
		asin, year = line.strip().split(',')
		years[asin] = int(year)
	return years

print('Loading IMDb metadata...')
imdb_meta = load_imdb_meta('../data/IMDb/imdb_meta.tsv.gz', 'tconst', 'startYear')

print('Loading review years...')
review_years = load_review_years('../data/Amazon/review_year.csv')

infile = '../data/amazon_imdb_match.txt'
outfile = '../data/amazon_imdb_match_p2.txt'

n1 = 0
n2 = 0
with open(outfile, 'w') as op:
	for line in open(infile, 'r'):
		asin, s = line.split()
		imdb_ids = s.split(',')
		year = review_years[asin]

		# Filter matches
		matches = [imdb_id for imdb_id in imdb_ids if imdb_meta[imdb_id] <= year]
		if not matches: continue

		s = ','.join(matches)
		if len(matches) == 1:
			n1 += 1
		else:
			n2 += 1
		op.write(f'{asin} {s}\n')

print('Titles with one match =', n1)
print('Titles with multiple matches =', n2)
