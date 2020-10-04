# Web server
import os
from flask import Flask, render_template, request, flash
from flaskext.mysql import MySQL

app = Flask(__name__)
app.config['SECRET_KEY'] = __name__

# Connect to database
app.config['MYSQL_DATABASE_HOST'] = os.getenv('MYSQL_HOST')
app.config['MYSQL_DATABASE_USER'] = os.getenv('MYSQL_USER')
app.config['MYSQL_DATABASE_PASSWORD'] = os.getenv('MYSQL_PASSWORD')
app.config['MYSQL_DATABASE_DB'] = 'reviews'

mysql = MySQL()
mysql.init_app(app)
conn = mysql.connect()
cursor = conn.cursor()

cursor.execute('SELECT DISTINCT year from movies ORDER BY year DESC')
conn.commit()
years = [row[0] for row in cursor.fetchall()]

cursor.execute('SELECT id, name from titleTypes')
conn.commit()
titleTypes = {row[0]:row[1] for row in cursor.fetchall()}

cursor.execute('SELECT id, name from genres')
conn.commit()
genres = {f'{row[0]:02d}':row[1] for row in cursor.fetchall()}

data = {'years': years, 'titleTypes': titleTypes, 'genres': genres}

@app.route('/', methods=['GET', 'POST'])
def main():
	if request.method == 'POST':
		conditions = []
		if request.form['title']: conditions.append("title LIKE '%{}%'".format(request.form['title']))
		if request.form['year']: conditions.append("year = {}".format(request.form['year']))
		if request.form['genre']: conditions.append("genres LIKE '{}'".format(request.form['genre']))
		clause = ' WHERE {}'.format(' AND '.join(conditions)) if conditions else ''
		title = request.form['title']
		sql = f'SELECT title, titleType, year, runtime, genres FROM movies{clause} LIMIT 10'
#		flash(sql)
		cursor.execute(sql)
		conn.commit()
		result = cursor.fetchall()
		return render_template('index.html', data=data, result=result)
	return render_template('index.html', data=data)

if __name__ == '__main__':
	app.run(host='0.0.0.0')
