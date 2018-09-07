from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.dialects import postgresql
from werkzeug import generate_password_hash, check_password_hash
 
import geocoder
from urllib.request import urlopen
from urllib.parse import urljoin
import json
import datetime

db = SQLAlchemy()


class PopularCompanies(object):
  def find_unique_request(self, cik, date_range):
    sql_command = """SELECT hll
                     FROM unique_requests_nasdaq_hourly
                     WHERE (cik = '{cik}') AND (date_time BETWEEN  '{start_date_time}' AND  '{end_date_time}')
                     """.format(cik=cik, start_date_time=date_range[0], end_date_time=date_range[1])
    results = self.convert_query_result_to_list(db.engine.execute(sql_command))
    return results                


  def query_top_50(self, date_range):
    # query for the top 10 most popular companies in the date_range provided
    sql_command = """SELECT unique_hourly_table.cik as cik, 
                            sum_unique_requests
                     FROM company_name_cik as name_cik_table
                     JOIN (SELECT cik, SUM(unique_requests) as sum_unique_requests
                           FROM unique_requests_nasdaq_hourly
                           WHERE date_time BETWEEN  '{start_date_time}' AND  '{end_date_time}'
                           GROUP BY cik) as unique_hourly_table
                     ON unique_hourly_table.cik = name_cik_table.cik
                     ORDER BY sum_unique_requests DESC
                     LIMIT 20;""".format(start_date_time=date_range[0], end_date_time=date_range[1])

    results = self.convert_query_result_to_list(db.engine.execute(sql_command))
    actual_unique_requests = []
    for record in results:
      print('Record[0]:', record[0])
      unique_requests = self.find_unique_request(cik=record[0], date_range=date_range)
      actual_unique_requests.append((record[0], unique_requests))

    print('actual_unique_requests:', actual_unique_requests)
    sorted_company_list = sorted(actual_unique_requests, key=lambda tup: tup[1], reverse=True)

    return sorted_company_list[:10]



  def query_top_10(self, date_range):
    """
        date_range: range of date_time in string format as a list with two element
    """

    time_span = (datetime.datetime.strptime(date_range[1], "%Y-%m-%d %H:%M") 
                 - datetime.datetime.strptime(date_range[0], "%Y-%m-%d %H:%M"))

    if time_span > datetime.timedelta(days=7):
      table_to_query = 'unique_requests_nasdaq_daily'
    else:
      table_to_query = 'unique_requests_nasdaq_hourly'
    
    # query for the top 10 most popular companies in the date_range provided
    sql_command = """SELECT name_cik_table.company_name as company_name,
                            unique_hourly_table.cik as cik, 
                            sum_unique_requests
                     FROM company_name_cik as name_cik_table
                     JOIN (SELECT cik, SUM(unique_requests) as sum_unique_requests
                           FROM {table_to_query}
                           WHERE date_time BETWEEN  '{start_date_time}' AND  '{end_date_time}'
                           GROUP BY cik) as unique_hourly_table
                     ON unique_hourly_table.cik = name_cik_table.cik
                     ORDER BY sum_unique_requests DESC
                     LIMIT 10;""".format(start_date_time=date_range[0], 
                                         end_date_time=date_range[1],
                                         table_to_query=table_to_query)

    return db.engine.execute(sql_command)


  def query_one_company(self, cik, date_range):
    time_span = (datetime.datetime.strptime(date_range[1], "%Y-%m-%d %H:%M") 
                 - datetime.datetime.strptime(date_range[0], "%Y-%m-%d %H:%M"))

    if time_span > datetime.timedelta(days=7):
      table_to_query = 'unique_requests_nasdaq_daily'
    else:
      table_to_query = 'unique_requests_nasdaq_hourly'

    sql_command = """SELECT date_time, unique_requests
                     FROM {table_to_query}
                     WHERE (cik = {cik}) AND (date_time BETWEEN  '{start_date_time}' AND  '{end_date_time}')
                     ORDER BY date_time""".format(start_date_time=date_range[0], 
                                                  end_date_time=date_range[1], cik=cik,
                                                  table_to_query=table_to_query)

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


  def find_max_in_list(self, trend_data, index):
    try:
      max_field = trend_data[0][index] 
    except:
      max_field = 1

    for i, rows in enumerate(trend_data):
      max_field = max(max_field, trend_data[i][index])

    return max_field
    

class RequestsFileSize(db.Model):
	__tablename__ = 'testdatetime'
	datetime = db.Column(postgresql.TIMESTAMP, primary_key = True)
	size = db.Column(postgresql.FLOAT)
	hll = db.Column(postgresql.ARRAY(db.Integer, dimensions=1))

	def __init__(self, datetime, size, hll):
		self.datetime = datetime
		self.size = size
		self.hll = hll[:]
