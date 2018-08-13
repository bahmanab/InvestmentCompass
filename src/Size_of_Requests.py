import os
from pyspark import SparkContext
from operator import add
from pyspark.sql import SparkSession
import datetime


def round_date_time(date_time=None, time_delta=datetime.timedelta(minutes=1)):
    """
        Truncate a datetime object using a time_delta (always gets rid of microseconds).
    :param date_time: a datetime object
    :param time_delta: timedelta object which is used for truncation
    :return: a truncated datetime object removing modulus(date_time, time_delta)
    """
    round_to = time_delta.total_seconds()

    if date_time is None:
        date_time = datetime.datetime.now()
    seconds = (date_time - date_time.min).seconds  # second since time zero
    rounded = ((seconds + round_to/2) // round_to) * round_to
    return date_time + datetime.timedelta(0,rounded-seconds,-date_time.microsecond)


def date_time_str(date_time):
    """
        Converts a datetime object to a string of format: 'YYYY-MM-DD HH:mm:SS'.
    :param date_time: a datetime object
    :return: a string form of date time object in isoform.
    """
    return date_time.isoformat(sep=' ', timespec='seconds')


def str_to_date_time(input_str):
    """
        Converts a string in iso format 'YYYY-MM-DD HH:mm:SS' to a datetime object.
    :param input_str: a date_time string in iso form.
    :return: a datetime object.
    """
    return datetime.datetime.strptime(input_str, '%Y-%m-%d %H:%M:%S')


def extract_required_fields_order(header, required_fields):
    """
        Extract the all the required fields from the header which is the name of fields separated by comma
        and return required fields.

    :param header: a comma seperated string including list of fields
    :param required_fields: a list containing name of required fields
    :return: a tuple including order of required fields in the header or a line of input data  (zero-indexed)
    """
    all_fields = header.split(',')
    required_fields_order = ()
    for field in required_fields:
        for i, x in enumerate(all_fields):
            if field == x:
                required_fields_order += (i,)

    if len(required_fields_order) != len(required_fields):
        raise Exception('Some of required fields are not found in the header.')

    return required_fields_order


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

    :param record_string: raw record for a document request according to SEC description
    :param req_fields_dict: a dictionary containing required fields as key and the index of that field in the comma
                            seperated record as value.
    :return: the required fields as a list in the other specified by required_fields_names
    """

    all_fields = record_string.split(',')

    extracted_fields = {}
    for key in required_fields_names:
        extracted_fields[key] = all_fields[req_fields_dict[key]]

    return extracted_fields


def str_to_rounded_date_time(d, t, time_delta=1):
    """
        Converts date and time provided in string isoform to datetime object and truncates using time_delta in minutes.
    :param date: string date in isofrom
    :param time: string time in HH:mm:SS form
    :return: datetime object truncated by time_delta
    """
    dt = datetime.timedelta(minutes=time_delta)
    return round_date_time(str_to_date_time(d + ' ' + t), dt)


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

header = "ip,date,time,zone,cik,accession,extention,code,size,idx,norefer,noagent,find,crawler,browser"
# log file data header:
#         ip,date,time,zone,cik,accession,extention,code,size,idx,norefer,noagent,find,crawler,browser
# index:  0   1    2    3    4      5         6      7    8    9    10      11     12    13      14
# sample data:  101.81.133.jja, 2017-06-29,00:00:00,0.0,1515671.0,0000940400-17-000412,-index.htm,200.0,6832.0,1.0,0.0,0.0,9.0,0.0,
# index:              0              1         2     3     4               5              6         7     8     9  10  11  12  13
# Note 1: the actual records do not include browser field even though file headers indicate so.
# Note 2: the first ten digits of accession is company's cik

required_fields_names_local = ['ip', 'date', 'time', 'zone', 'cik', 'accession', 'size']
fields_index_local = get_order_of_required_fields(header, required_fields_names_local)
         
# context setup
sc = SparkContext(appName="log analyzer")

hadoop_conf = sc._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
hadoop_conf.set("fs.s3a.awsAccessKeyId", os.environ['AWS_ACCESS_KEY_ID'])
hadoop_conf.set("fs.s3a.awsSecretAccessKey", os.environ['AWS_SECRET_ACCESS_KEY'])

# broadcast
required_fields_names = sc.broadcast(required_fields_names_local)
fields_index = sc.broadcast(fields_index_local)

# read the csv log files from S3
text_file = sc.textFile("s3a://edgar-log-files/logs/log20170629.csv")

input_rdd = text_file.map(lambda line: extract_required_fields(line, fields_index.value, required_fields_names.value))
             
# compute the total size of requested file at an hour windows
sizes = (input_rdd.map(lambda x: (str_to_rounded_date_time(x['date'], x['time'], time_delta=60), str_to_float(x['size'])))   
                  .reduceByKey(lambda x, y: x + y))

print('Count:', sizes.count())
print(sizes.take(25))
# setup parameters for connecting to postgres
mode = "overwrite"
url = "jdbc:postgresql://ec2-34-215-4-191.us-west-2.compute.amazonaws.com/edgar"
properties = {"user": "postgres", "password": "seCurepassword", "driver": "org.postgresql.Driver"}
 
# create a spark sql session
spark = SparkSession(sc)
 
# convert RDD to DataFrame
df = sizes.toDF(["date_time", "size"])
 
df.show()
print("DF count:", df.count())
 
# write the DataFrame to database
df.write.jdbc(url=url, table="request_filesize", mode=mode, properties=properties)
