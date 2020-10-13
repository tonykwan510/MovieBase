# Movie Advisor

Movie recommendation from user reviews.

## Introduction

Movie reviews can be confusing. Different websites may rate a movie differently. If we try to take a closer look at the user reviews, there are just too many reviews. What can we do?

In this project, Amazon product reviews and IMDb titles metadata are used to built a movie review database, and a movie recommendation system is built on top of the database. Users can use different criteria to search movies, as well as use keyword search to navigate the through the movie reviews.

## Data Sources

Amazon product reviews: Movies and TV reviews
- Link: https://nijianmo.github.io/amazon/index.html
- Files: `Movies_and_TV.json.gz`, `meta_Movies_and_TV.json.gz`

IMDb titles metadata
- Link: https://www.imdb.com/interfaces/
- Files: `title.akas.tsv.gz`, `title.basics.tsv.gz`, `title.ratings.tsv.gz`

## Data Challenge

Data cleaning is needed on Amazon product names before they can be matched with IMDb movie titles. Below are some examples of Amazon product names and the matched IMDb title.

| Amazon Product Name          | IMDb Title           |
| ---------------------------- | -------------------- |
| Mr &amp; Mrs Smith VHS       | Mr & Mrs Smith       |
| THE SIXTH SENSE              | The Sixth Sense      |
| Reno 911! - Miami            | Reno 911!: Miami     |
| Timeline (2003) (Widescreen) | Timeline             |
| Heart of the Country, The    | Heart of the Country |


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
- `sqlalchemy` for database operations
- `flask` for web application
- `flask_sqlalchemy` for database operations

## Repo Structure

Folders and Python scripts are numbered and named to minimize confusion. There is a README file in each folder. Some scripts have alternative versions that use different methods to achieve the same result. The alternative versions are denoted by a letter after the number, for example, `00a_xxx.py`.