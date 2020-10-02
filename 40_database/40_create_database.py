# Create database
import os
import mysql.connector
from mysql.connector import errorcode

def create_database(cursor, database):
	try:
		cursor.execute("CREATE DATABASE {} DEFAULT CHARACTER SET 'utf8'".format(database))
	except mysql.connector.Error as err:
		print(f'Failed creating database {database}: {err}')
		exit(1)

host = os.getenv('MYSQL_HOST')
user = os.getenv('MYSQL_USER')
password = os.getenv('MYSQL_PASSWORD')
database = 'reviews'

conn = mysql.connector.connect(host=host, user=user, password=password)
cursor = conn.cursor()

try:
	cursor.execute(f'USE {database}')
	print(f'Database {database} already exists.')
except mysql.connector.Error as err:
	print(f'Database {database} does not exist.')
	if err.errno == errorcode.ER_BAD_DB_ERROR:
		create_database(cursor, database)
		print(f'Database {database} created successfully.')
	else:
		print(err)
		exit(1)

conn.close()
