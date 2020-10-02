# Match Amazon and IMDb titles
import gzip, json, html, re
from bisect import bisect_left

def load_imdb_meta(path, key, title):
	meta = {}
	k = 0
	for line in gzip.open(path, 'rt', encoding='utf-8'):
		fields = line.strip().split('\t')
		k += 1
		if k == 1:
			headers = fields
			continue
		item = dict(zip(headers, fields))
		meta[item[key]] = item[title]
	return meta

def load_amazon_meta(path, key, val):
	meta = {}
	for line in open(path, 'r'):
		item = json.loads(line.strip())
		meta[item[key]] = html.unescape(item[val])
	return meta

def encode(title, skip_words=[], end_words=[], lower=True, replace=True, skip_par=False):
	if lower: title = title.lower()
	for word in end_words:
		title = title.partition(word)[0]
	if skip_par: title = re.sub('\(.*?\)', '', title)
	if replace: title = re.sub('\W', ' ', title)
	words = title.strip().split()
	if len(words) > 1 and words[-1].lower() == 'the':
		words = words[-1:] + words[:-1]
	code = ' '.join(word for word in words if word.lower() not in skip_words)
	return code

def match(src, desc, out, src_encode, desc_encode):
	src_list = [(key, src_encode(title)) for key, title in src.items()]
	src_list.sort(key=lambda x:x[1])
	src_codes = [item[1] for item in src_list]
	n = len(src_codes)

	for key, title in desc.items():
		if out[key]: continue
		code = desc_encode(title)
		ind = bisect_left(src_codes, code)
		while ind < n and src_codes[ind] == code:
			out[key].append(src_list[ind][0])
			ind += 1
	return

print('Loading IMDb metadata...')
imdb_meta = load_imdb_meta('../data/IMDb/imdb_meta.tsv.gz', 'tconst', 'primaryTitle')

print('Loading Amazon metadata...')
amazon_meta = load_amazon_meta('../data/Amazon/amazon_meta.json', 'asin', 'title')
match_keys = {key: [] for key in amazon_meta.keys()}

k = 1
print(f'Performing matching round {k}...')
src_encode = lambda x: encode(x, lower=False, replace=False)
desc_encode = lambda x: encode(x,
	end_words=['['],
	skip_words=['vhs', 'dvd', 'anglais', 'italien'],
	lower=False, replace=False)
match(imdb_meta, amazon_meta, match_keys, src_encode, desc_encode)

k += 1
print(f'Performing matching round {k}...')
src_encode = lambda x: encode(x, replace=False)
desc_encode = lambda x: encode(x,
	end_words=['['],
	skip_words=['vhs', 'dvd', 'anglais', 'italien'],
	replace=False)
match(imdb_meta, amazon_meta, match_keys, src_encode, desc_encode)

k += 1
print(f'Performing matching round {k}...')
src_encode = lambda x: encode(x)
desc_encode = lambda x: encode(x,
	end_words=['['],
	skip_words=['vhs', 'dvd', 'anglais', 'italien'])
match(imdb_meta, amazon_meta, match_keys, src_encode, desc_encode)

k += 1
print(f'Performing matching round {k}...')
src_encode = lambda x: encode(x)
desc_encode = lambda x: encode(x,
	end_words=['['],
	skip_words=['vhs', 'dvd', 'anglais', 'italien'],
	skip_par=True)
match(imdb_meta, amazon_meta, match_keys, src_encode, desc_encode)

k += 1
print(f'Performing matching round {k}...')
src_encode = lambda x: encode(x, skip_words=['the'])
desc_encode = lambda x: encode(x,
	end_words=['['],
	skip_words=['the', 'vhs', 'dvd', 'anglais', 'italien'],
	skip_par=True)
match(imdb_meta, amazon_meta, match_keys, src_encode, desc_encode)

n1 = sum(len(val) == 0 for val in match_keys.values())
n2 = sum(len(val) == 1 for val in match_keys.values())
n3 = sum(len(val) > 1 for val in match_keys.values())
print('Titles with no match=', n1)
print('Titles with one match =', n2)
print('Titles with multiple matches =', n3)

with open('../data/amazon_imdb_match.csv', 'w') as op:
	for key, val in match_keys.items():
		if len(val) == 1:
			op.write(f'{key},{val[0]}\n')
