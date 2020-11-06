# Movie Advisor

Movie recommendation system based on user reviews.

## Introduction

Movie reviews can be confusing. Different websites may rate a movie differently. If we try to take a closer look at the user reviews, there are just too many reviews. What can we do?

In this project, Amazon product reviews and IMDb data are used to built a movie review database, and a movie recommendation system is built on top of the database. Users can use different criteria to search movies, as well as use keyword search to navigate movie reviews.

5-minute demo: https://youtu.be/sw1i-L6jZZc

## Data Sources

Amazon product reviews: Movies and TV reviews
- Link: https://nijianmo.github.io/amazon/index.html
- Files: `Movies_and_TV.json.gz`, `meta_Movies_and_TV.json.gz`

IMDb titles metadata
- Link: https://www.imdb.com/interfaces/
- Files: `title.akas.tsv.gz`, `title.basics.tsv.gz`, `title.ratings.tsv.gz`

## Data Challenge

A big challenge in the project is entity matching because Amazon product titles are very dirty. Below are some examples of Amazon product names and the matched IMDb title.

| Amazon Product Name          | IMDb Title           |
| ---------------------------- | -------------------- |
| Mr \&amp; Mrs Smith VHS      | Mr & Mrs Smith       |
| THE SIXTH SENSE              | The Sixth Sense      |
| Reno 911! - Miami            | Reno 911!: Miami     |
| Timeline (2003) (Widescreen) | Timeline             |
| Heart of the Country, The    | Heart of the Country |

A multiple-round matching approach was used to perform the matching. In each round, the titles are transformed before exact matching is perfromed. The transformations in later rounds are more aggressive to get more matches.
- Round 1: Fix HTML escape characters and discard words such as VHS and DVD.
- Round 2: Turn into lower case.
- Round 3: Discard special characters.
- Round 4: Discard parentheses and the contents.
- Round 5: Discard "the".

## Installation

The project was built with following settings:
- Python 3.7 (Spark did not support Python 3.8)
- OpenJDK 1.8.0
- Scala 2.11.12
- Spark 2.4.7 (Spark 3.x requires Scala 2.12)
- MySQL 8.0.21

Python packages used in the project:
- `boto3` for AWS S3 bucket operations
- `pyspark` for distributed computing
- `pandas` for structured data manipulations
- `s3fs` for reading from AWS S3 bucket
- `pyarrow` for processing Parquet files
- `sqlalchemy` for database operations
- `flask` for web application
- `flask_sqlalchemy` for database operations

## Frontend

The search engine can be found [here](http://www.databuilder.xyz/movie). I will keep it running for as long as I can.The search engine is actually a primitive RESTful API that return HTML by default. Below are the available keys.

Movie endpoint:
- `title` (optional): Title keyword.
- `year` (optional): Release year (up to 2016).
- `genre` (optional): 00 - Action; 01 - Adult; 02 - Adventure; 03 - Animation; 04 - Biography; 05 - Comedy; 06 - Crime; 07 - Documentary; 08 - Drama; 09 - Family; 10 - Fantasy; 11 - Film-Noir; 12 - History; 13 - Horror; 14 - Music; 15 - Musical; 16 - Mystery; 17 - News; 18 - Romance; 19 - Sci-Fi; 20 - Sport; 21 - Thriller; 22 - War; 23 - Western
- `sortkey` (optional): Ordering of movies. `amazon_rating` or `imdb_rating`.
- `page` (optional): Page number. Number of results paer page is currently fixed to be 10.
- `output` (optional): Output format. `html` or `json`. Default is `html`.

Review endpoint:
- `movie`: Movie ID.
- `keyword`(optional): Review keyword. 
- `page` (optional): Page number. Number of results per page is currently fixed to be 10.
- `output` (optional): Output format. `html` or `json`. Default is `html`.

## Repo Structure

Folders and Python scripts are numbered and named to minimize confusion. There is a README file in each folder. Note that some scripts have alternative versions that use different methods to achieve the same result. The alternative versions are denoted by a letter after the number, for example, `11a_match_titles.py`.