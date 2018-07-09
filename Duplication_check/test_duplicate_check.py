from pyspark import SparkContext, SparkConf
from boto.s3.connection import S3Connection
from pyspark import sql
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
import pandas as pd
import psycopg2
import csv

# function to deal with each repo, return dictionary
def each_repo(repo):

    # only consider the cases that repo_name and and content are ASCII
    try:
        repo_id   = repo[0].encode('ascii')
        repo_name = repo[1].encode('ascii')
        print repo_id, repo_name
        content_tmp = repo[2].encode('ascii')
    except:
        return results

    # split content by new line character '\n'
    content_list = content_tmp.split('\n')
    linenum = len(content_list)
    print "line number:", linenum
    if(linenum <= 1): 
        return results

    for line in content_list:
        print line
 

def main():

    # connect to s3 bucket, generate RDD
    sc = SparkContext(master='spark://*******',appName ='duplicate-test')
    sqlContext = SQLContext(sc)
    for i in range(1):
        name_num = '{0:03}'.format(i)
        dataname = "s3a://bucketname-test/*******.json"

        dataframe_json = sqlContext.read.json(dataname)
        dataframe_json.registerTempTable("repos")
        dataframe_sql = sqlContext.sql("SELECT id, sample_repo_name, content from repos where content != '' and id = '118c26070acbe169f57c0c65bf62fa8ad0da7906'")
        dataframe_rdd = dataframe_sql.rdd
    
        # deal with each repo and insert to postgres
        repo_num = dataframe_rdd.count()
        repos = dataframe_rdd.take(1)
        print "dataset name", dataname
        print "number of repo in total:", repo_num
        for repo in repos:
            each_repo(repo)

if __name__ == "__main__":
    main()
