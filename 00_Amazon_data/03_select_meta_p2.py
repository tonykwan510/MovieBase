# Filter metadata by review counts
import json

# Products with at least 100 reviews
asins = set()
infile = '../data/Amazon/review_count.csv'
k = 0
for line in open(infile, 'r'):
	k += 1
	if k == 1: continue
	asin = line.split(',')[0]
	asins.add(asin)

infile = '../data/Amazon/amazon_meta.json'
outfile = '../data/Amazon/amazon_meta_p2.json'
with open(outfile, 'w') as op:
	k = nitem = 0
	for line in open(infile, 'r'):
		k += 1
		movie = json.loads(line.strip())
		asin = movie['asin']
		if asin in asins:
			nitem += 1
			op.write(line)

	print('Before: Number of products =', k)
	print('After: Number of products =', nitem)
