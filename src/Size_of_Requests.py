import os
from pyspark import SparkContext
from operator import add
from pyspark.sql import SparkSession
import hyperloglog
from datetime import date, timedelta, datetime, time

def round_time(date_time=None, time_delta=timedelta(minutes=1)):
""" always gets rid of microseconds."""
    round_to = time_delta.total_seconds()

    if date_time == None: 
        date_time = datetime.now()
    seconds = (date_time - date_time.min).seconds  # second since time zero
    rounded = ((seconds + round_to/2) // round_to) * round_to
    return date_time + timedelta(0,rounded-seconds,-date_time.microsecond)


def date_time_str(date_time):
""" converts a datetime object to a string of format: 'YYYY-MM-DD HH:MM:SS' """
    return date_time.isoformat(sep=' ', timespec='seconds')


def get_order_of_required_fields(file_header, required_fields_names):
    """
        Reads the file_header string and extract the order of required fields provided in required_fields_names list                            
    :param file_header: header of an input file providing the name and order of fields in a string
    :param required_fields_names: a list containing name of required fields
    :return: a dictionary with name of required fields as the key and their index of appearance in the records as value
    """

    fields_order = extract_required_fields_order(file_header, required_fields_names)
    req_fields = dict(zip(required_fields_names, fields_order))
    return req_fields


def extract_required_fields(record_string, req_fields_dict, required_fields_names):
    """
        Extract all the fields from a record_string which is raw record with fields separated by comma
        and return required fields.

    :param record_string: raw record for a document request according to FEC description
    :param req_fields_dict: a dictionary containing required fields as key and the index of that field in the comma seperated
                        record as value.
    :return: the required fields as a tuple (ip, date, time, cik, accession, extention)
    """

    all_fields = record_string.split(',')

    try:
        extracted_fields = (all_fields[req_fields_dict[key]] for key in required_fields_names)
    except: # if the record does not match header format a tuple with empty fields is returned which will be skipped
        ('' for i in range(len(required_fields_names))

    return extracted_fields


def str_to_float(x):
    """
        converts a string to float if possible otherwise returns 0.

    :param x: a string containing a floating point number
    :return: floating point number
    """
    try: 
        return float(x) 
    except: 
        return 0
    
# context setup
sc = SparkContext(appName="log analyzer")

hadoop_conf = sc._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
hadoop_conf.set("fs.s3a.awsAccessKeyId", os.environ['AWS_ACCESS_KEY_ID'])
hadoop_conf.set("fs.s3a.awsSecretAccessKey", os.environ['AWS_SECRET_ACCESS_KEY'])

# read the csv log files from S3
text_file = sc.textFile("s3a://edgar-log-files/logs/log20170629.csv")
header_file = sc.textFile("s3a://edgar-log-files/logs-headers/log20170629-header.csv")

# log file data header:
#         ip,date,time,zone,cik,accession,extention,code,size,idx,norefer,noagent,find,crawler,browser
# index:  0   1    2    3    4      5         6      7    8    9    10      11     12    13      14
#        0              1         2     3     4               5              6         7     8     9  10  11  12  13
#  101.81.133.jja, 2017-06-29,00:00:00,0.0,1515671.0,0000940400-17-000412,-index.htm,200.0,6832.0,1.0,0.0,0.0,9.0,0.0,
# the first ten digits of accession is cik

required_fields_names = ['ip', 'date', 'time', 'zone', 'cik', 'accession', 'size']
fields_index = (header_file.map(lambda header: get_order_of_required_fields(header, required_fields_names))
                           .first())


# compute the total size of requested file at an hour windows
sizes = (text_file.map(lambda x: x.split(',')) 
                  .map(lambda x: (x[1] + ' ' + x[2][:2], str_to_float(x[8])))   # first 2 character of x[2] is hour
                  .reduceByKey(lambda x, y: x + y))

# compute the total size of requested file at an hour windows
doc_unique_requests = (text_file.map(lambda x: x.split(',')) 
                                .map(lambda x: (x[1] + ' ' + x[2][:2], str_to_float(x[8])))   # first 2 character of x[2] is hour
                  .reduceByKey(lambda x, y: x + y))
 
# setup parameters for connecting to postgres
mode = "overwrite"
url = "jdbc:postgresql://ec2-34-215-4-191.us-west-2.compute.amazonaws.com/edgar"
properties = {"user": "postgres", "password": "seCurepassword", "driver": "org.postgresql.Driver"}
 
# create a spark sql session
spark = SparkSession(sc)
 
# convert RDD to DataFrame
df = sizes.toDF(["datetime", "size"])
 
df.show()
print("DF count:", df.count())
 
# write the DataFrame to database
df.write.jdbc(url=url, table="testtable", mode=mode, properties=properties)
