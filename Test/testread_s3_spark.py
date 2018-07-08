from pyspark import SparkContext, SparkConf
from boto.s3.connection import S3Connection
from pyspark import sql
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
import psycopg2
import boto3
import csv

# connetct to S3 and read csv data from s3 bucket
def read_s3_scv():
    aws_access_key_id='ACCESS_KEY'
    aws_secret_access_key='SECRET_KEY'
    bucketname = 'bucketname'

    s3 = boto3.client('s3')
    resp = s3.list_objects_v2(Bucket=bucketname)

    keys=[]
    for obj in resp['Contents']:
        if obj['Key'].endswith('csv'):
            keys.append(obj['Key'])
            print  obj['Key']

def read_s3_csv_rdd():
    sc = SparkContext(master='spark://IPADDRESS.internal:7077',appName ='read-csv-test')
    csv_rdd = sc.textFile("s3a://bucketname/*.csv")
    csv_rdd_list = csv_rdd.take(30000)
    return csv_rdd_list

def read_s3_json_rdd():
    sc = SparkContext(master='spark://IPADDRESS.internal:7077',appName ='read-json-test')
    sqlContext = SQLContext(sc)
    df = sqlContext.read.json("s3a://bucketname/*.json")
    df.registerTempTable("repo")
    df2 = sqlContext.sql("SELECT id, sample_repo_name, content from repo where content != ''")
    df_rdd = df2.rdd 
    return df_rdd
