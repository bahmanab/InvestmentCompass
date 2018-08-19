from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.dialects import postgresql
from werkzeug import generate_password_hash, check_password_hash
 
import geocoder
from urllib.request import urlopen
from urllib.parse import urljoin
import json
import datetime

db = SQLAlchemy()

class User(db.Model):
	__tablename__ = 'users'
	uid = db.Column(db.Integer, primary_key = True)
	firstname = db.Column(db.String(100))
	lastname = db.Column(db.String(100))
	email = db.Column(db.String(120), unique = True)
	pwdhash = db.Column(db.String(54))

	def __init__(self, firstname, lastname, email, password):
		self.firstname = firstname.title()
		self.lastname = lastname.title()
		self.email = email.lower()
		self.set_password(password)

	def set_password(self, password):
		self.pwdhash = generate_password_hash(password)

	def check_password(self, password):
		return check_password_hash(self.pwdhash, password)


class PopularCompanies(object):
  def query_top_10(self, date_range):
    """
        date_range: range of date_time in string format as a list with two element
    """
    
    # query for the top 10 most popular companies in the date_range provided
    sql_command = """SELECT name_cik_table.company_name as company_name,
                            unique_hourly_table.cik as cik, 
                            sum_unique_requests
                     FROM company_name_cik as name_cik_table
                     JOIN (SELECT cik, SUM(unique_requests) as sum_unique_requests
                           FROM unique_requests_nasdaq_hourly
                           WHERE date_time BETWEEN  '{start_date_time}' AND  '{end_date_time}'
                           GROUP BY cik) as unique_hourly_table
                     ON unique_hourly_table.cik = name_cik_table.cik
                     ORDER BY sum_unique_requests DESC
                     LIMIT 10;""".format(start_date_time=date_range[0], end_date_time=date_range[1])

    return db.engine.execute(sql_command)


  def query_one_company(self, cik, date_range):
    sql_command = """SELECT date_time, unique_requests
                     FROM unique_requests_nasdaq_daily
                     WHERE (cik = {cik}) AND (date_time BETWEEN  '{start_date_time}' AND  '{end_date_time}')
                     ORDER BY date_time""".format(start_date_time=date_range[0], end_date_time=date_range[1], cik=cik)

    return db.engine.execute(sql_command)


  def convert_query_result_to_list(self, query_results):
    records_list = []
    for record in query_results:
        records_list.append(list(record))

    return records_list


  def str_to_js_datetime(self, str_datetime):
    return int(datetime.datetime.strptime(str_datetime, "%Y-%m-%d %H:%M").strftime('%s')) * 1000


  def datetime_to_js_datetime_in_list(self, input_list, datetime_index):
    
    output_list = input_list[:]
    for i, record in enumerate(input_list):
      date_time = record[datetime_index]
      if type(date_time) == str:
        js_datetime = self.str_to_js_datetime(date_time)
      elif type(date_time) == datetime.datetime:
        js_datetime = int(date_time.strftime('%s')) * 1000
      output_list[i][datetime_index] = js_datetime

    return output_list

  def get_trend_data_for_cik(self, cik, date_range):
    company_trend = self.query_one_company(cik, date_range)
    company_list = self.convert_query_result_to_list(company_trend)
    company_list = self.datetime_to_js_datetime_in_list(company_list, 0)
    return company_list


        
class Place(object):
  def meters_to_walking_time(self, meters):
    # 80 meters is one minute walking time
    return int(meters / 80)  

  def wiki_path(self, slug):
    return urljoin("http://en.wikipedia.org/wiki/", slug.replace(' ', '_'))
  
  def address_to_latlng(self, address):
    g = geocoder.google(address)
    return (g.lat, g.lng)

  def query(self, address):
    lat, lng = self.address_to_latlng(address)
    
    query_url = 'https://en.wikipedia.org/w/api.php?action=query&list=geosearch&gsradius=5000&gscoord={0}%7C{1}&gslimit=20&format=json'.format(lat, lng)
    g = urlopen(query_url)
    results = g.read().decode('ASCII')
    g.close()

    data = json.loads(str(results))
    
    places = []
    for place in data['query']['geosearch']:
      name = place['title']
      meters = place['dist']
      lat = place['lat']
      lng = place['lon']

      wiki_url = self.wiki_path(name)
      walking_time = self.meters_to_walking_time(meters)

      d = {
        'name': name,
        'url': wiki_url,
        'time': walking_time,
        'lat': lat,
        'lng': lng
      }

      places.append(d)

    return places

class RequestsFileSize(db.Model):
	__tablename__ = 'testdatetime'
	datetime = db.Column(postgresql.TIMESTAMP, primary_key = True)
	size = db.Column(postgresql.FLOAT)
	hll = db.Column(postgresql.ARRAY(db.Integer, dimensions=1))

	def __init__(self, datetime, size, hll):
		self.datetime = datetime
		self.size = size
		self.hll = hll[:]
