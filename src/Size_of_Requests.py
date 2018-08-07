import os
from pyspark import SparkContext
from operator import add
from pyspark.sql import SparkSession


def convert2float(x):
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
text_file = sc.textFile("s3a://edgar-log-files/logs/log20170630_small.csv")


# compute the total size of requested file at an hour windows
sizes = (text_file.map(lambda x: x.split(',')) 
                  .map(lambda x: (x[1] + ' ' + x[2][:2], convert2float(x[8])))   # first 2 character of x[2] is hour
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

# write the DataFrame to database
df.write.jdbc(url=url, table="testtable", mode=mode, properties=properties)
