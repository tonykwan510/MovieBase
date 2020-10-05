# Match Amazon and IMDb titles (Spark version)
import pandas as pd
import json, html, re
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, collect_list, size, udf
from pyspark.sql.types import StringType

def extract_years(s, val_min, val_max):
	years = re.findall('\D([12]\d{3})\D', s)
	years = [int(year) for year in years if val_min <= int(year) <= val_max]
	years = list(set(years))
	return years

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

def match(src, desc, val, target, src_encode, desc_encode, key='key'):
	joined = desc.withColumn(key, desc_encode(val)).alias('df1') \
		.join(src.withColumn(key, src_encode(val)).select(key, target) \
			.groupby(key).agg(collect_list(target).alias(target)).alias('df2'),
			col(f'df1.{key}') == col(f'df2.{key}'), 'left') \
		.drop(key)
	matched = joined.filter(~col(target).isNull())
	unmatched = joined.filter(col(target).isNull()).drop(target)
	return matched, unmatched

# Load IMDb metadata
path = '../data/IMDb/imdb_meta.tsv.gz'
names = ['imdb_id', 'X1', 'title', 'X2', 'X3', 'year', 'X4', 'X5', 'X6']
usecols = [name for name in names if name[0] != 'X']
imdb_df = pd.read_csv(path, sep='\t', header=0, names=names, usecols=usecols, compression='gzip')

# Save IMDb year to handle multiple matches later
imdb_dict = imdb_df.set_index('imdb_id').year.to_dict()

# Load Amazon metadata
path = '../data/Amazon/amazon_meta.json'
records = []
for line in open(path, 'r'):
	item = json.loads(line.strip())

	# Extract year information from description
	year = 0
	if item['description']:
		years = extract_years(item['description'][0], 1888, 2018)
		if len(years) == 1: year = years[0]

	record = (item['asin'], html.unescape(item['title']), year)
	records.append(record)
amazon_df = pd.DataFrame.from_records(records, columns=['asin', 'title', 'year'])

sc = SparkContext()
sc.setLogLevel('ERROR')
imdb_df = sc.broadcast(imdb_df).value
amazon_df = sc.broadcast(amazon_df).value

spark = SparkSession(sc)

imdb_df = spark.createDataFrame(imdb_df)
amazon_df = spark.createDataFrame(amazon_df)

k = 1
print(f'Performing matching round {k}...')
src_encode = udf(lambda x: encode(x, lower=False, replace=False))
desc_encode = udf(lambda x: encode(x,
	end_words=['['],
	skip_words=['vhs', 'dvd', 'anglais', 'italien'],
	lower=False, replace=False))
matched, amazon_df = match(imdb_df, amazon_df, 'title', 'imdb_id', src_encode, desc_encode)

k += 1
print(f'Performing matching round {k}...')
src_encode = udf(lambda x: encode(x, replace=False))
desc_encode = udf(lambda x: encode(x,
	end_words=['['],
	skip_words=['vhs', 'dvd', 'anglais', 'italien'],
	replace=False))
tmp, amazon_df = match(imdb_df, amazon_df, 'title', 'imdb_id', src_encode, desc_encode)
matched = matched.union(tmp)

k += 1
print(f'Performing matching round {k}...')
src_encode = udf(lambda x: encode(x))
desc_encode = udf(lambda x: encode(x,
	end_words=['['],
	skip_words=['vhs', 'dvd', 'anglais', 'italien']))
tmp, amazon_df = match(imdb_df, amazon_df, 'title', 'imdb_id', src_encode, desc_encode)
matched = matched.union(tmp)

k += 1
print(f'Performing matching round {k}...')
src_encode = udf(lambda x: encode(x))
desc_encode = udf(lambda x: encode(x,
	end_words=['['],
	skip_words=['vhs', 'dvd', 'anglais', 'italien'],
	skip_par=True))
tmp, amazon_df = match(imdb_df, amazon_df, 'title', 'imdb_id', src_encode, desc_encode)
matched = matched.union(tmp)

k += 1
print(f'Performing matching round {k}...')
src_encode = udf(lambda x: encode(x, skip_words=['the']))
desc_encode = udf(lambda x: encode(x,
	end_words=['['],
	skip_words=['the', 'vhs', 'dvd', 'anglais', 'italien'],
	skip_par=True))
tmp, amazon_df = match(imdb_df, amazon_df, 'title', 'imdb_id', src_encode, desc_encode)
matched = matched.union(tmp)

n1 = amazon_df.count()
print('Titles with no match=', n1)

matched = matched.toPandas()
spark.stop()

# Use year to reduce multiple matches
def match_year(row):
	keys = [key for key in row.imdb_id if imdb_dict[key] == row.year]
	return keys if keys else row.imdb_id

inds = matched.imdb_id.apply(len) > 1
matched.loc[inds, 'imdb_id'] = matched[inds].apply(match_year, axis=1)

n2 = (matched.imdb_id.apply(len) == 1).sum()
n3 = (matched.imdb_id.apply(len) > 1).sum()
print('Titles with one match =', n2)
print('Titles with multiple matches =', n3)

matched.imdb_id = matched.imdb_id.apply(lambda x: ','.join(map(str, x)))
matched.sort_values(by='asin') \
    .drop(['title', 'year'], axis=1) \
	.to_csv('../data/amazon_imdb_match.csv', header=False, index=False)
