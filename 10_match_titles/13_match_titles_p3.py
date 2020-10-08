# Match Amazon and IMDb titles
# Part 3: Manually handle multiple matches
import gzip

imdb_movies = {}
k = 0
for line in gzip.open('../data/IMDb/imdb_meta.tsv.gz', 'rt'):
	vals = line.strip().split('\t')
	k += 1
	if k == 1:
		headers = vals
		continue
	item = dict(zip(headers, vals))
	imdb_movies[item['tconst']] = int(item['startYear'])

amazon_movies = {}
for line in open('input.txt', 'r'):
	asin, imdb_ids = line.split()
	amazon_movies[asin] = imdb_ids.split(',')

year = int(input('Year = '))

for asin, imdb_ids in amazon_movies.items():
	if len(imdb_ids) == 1: continue
	years = [imdb_movies[imdb_id] for imdb_id in imdb_ids]
	if year not in years: continue
	url = 'https://www.amazon.com/dp/' + asin
	print(url)
	for k, imdb_id in enumerate(imdb_ids):
		print(k, imdb_movies[imdb_id], imdb_id)
	k = int(input())
	if k >= 100: break
	if k < len(imdb_id):
		imdb_id = imdb_ids[k]
		imdb_ids.clear()
		imdb_ids.append(imdb_id)

with open('../data/amazon_imdb_match_p3.txt', 'w') as op:
	for asin, imdb_ids in amazon_movies.items():
		s = ','.join(imdb_ids)
		op.write(f'{asin} {s}\n')
