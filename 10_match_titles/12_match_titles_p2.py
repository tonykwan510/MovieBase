# Match Amazon and IMDb titles
# Part 2: Handle multiple matches by using year
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

# Extract years from string
def extract_years(s, val_min, val_max):
	years = re.findall('\D([12]\d{3})\D', s)
	years = [int(year) for year in years if val_min <= int(year) <= val_max]
	years = list(set(years))
	return years

# Extract from description year information of Amazon titles
def load_amazon_meta(path):
	meta = {}
	for line in open(path, 'r'):
		item = json.loads(line.strip())
		if item['description']:
			years = extract_years(item['description'][0], 1888, 2018)
			if len(years) == 1:
				meta[item['asin']] = years[0]
	return meta

print('Loading IMDb metadata...')
imdb_meta = load_imdb_meta('../data/IMDb/imdb_meta.tsv.gz', 'tconst', 'startYear')

print('Loading Amazon metadata...')
amazon_meta = load_amazon_meta('../data/Amazon/amazon_meta.json')

infile = '../data/amazon_imdb_match.txt'
outfile = '../data/amazon_imdb_match_p2.txt'

n1 = 0
n2 = 0
with open(outfile, 'w') as op:
	for line in open(infile, 'r'):
		asin, s = line.split()
		imdb_ids = s.split(',')
		if len(imdb_ids) > 1 and asin in amazon_meta:
			year = amazon_meta[asin]
			matches = [imdb_id for imdb_id in imdb_ids if imdb_meta[imdb_id] == year]
			if matches:
				imdb_ids = matches
				s = ','.join(imdb_ids)
		if len(imdb_ids) == 1:
			n1 += 1
		else:
			n2 += 1
		op.write(f'{asin} {s}\n')

print('Titles with one match =', n1)
print('Titles with multiple matches =', n2)
