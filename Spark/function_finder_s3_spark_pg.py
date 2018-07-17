from pyspark import SparkContext, SparkConf
from pyspark import sql
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
import pandas as pd
import psycopg2
import csv
import re
from conconfig import *

def find_class(line, class_loc):
    loc_end = line.find('(')
    class_name = line[class_loc : loc_end].strip()
    return class_name

def find_function(line, def_loc):
    def_end = line.find('(')
    function_name = line[def_loc : loc_end].strip()
    return function_name

def find_function_input(line):
    input_name = ' '
    loc1 = line.find('(')
    loc2 = line.find(')')
    if(loc2 - loc1 > 1): 
        input_name = line[loc1 + 1 : loc2]
    return input_name

def find_function_out(line, return_loc, out_name):
    cur_out = line[return_loc : ].strip()
    if (len(cur_out) == 0): 
        continue
    elif(out_name == ' '): 
        out_name = cur_out
    else:
        out_name = out_name + " or " + cur_out
    return out_name

# function to deal with each repo, return dictionary
def each_repo(repo):

    # initialize the tuple
    results = ()

    # only consider the cases that repo_id, repo_name, and content are ASCII
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
 
    # begin to process the content of the repo to extreact the usefull informaton
    class_loc = 10000
    class_name = ' '
    class_cur = ' '
    function_name = ' '
    input_name = ' '
    out_name = ' '
    #check the content of one repo line by line
    for line in content_list:

        # find the location of the first non-space charater 
        loc0 = len(line) - len(line.lstrip())
        
        # find the location of # in one line and skip if # as head
        loc_ps = line.find('#')
        if(loc_ps == loc0): 
            continue

        # find the class by "class "
        pos_class = re.search(r'^[\s]*class\s', line)
        if pos_class != None:
            class_loc = p_class.span()[1]
            class_name = find_class(line, class_loc)

        # find the function by "def "    
        p_func = re.search(r'^[\s]*def\s', line)
        if p_func != None:

            # initial out_name
            out_name = ' '

            # find function name
            def_loc = p_func.span()[1]
            function_name = find_functoin(line, def_loc)

            # find the class_name for current function
            class_cur = ' '
            if(def_loc > class_loc): 
                class_cur = class_name
            else:
                class_name = ' '
                class_loc = 10000

            # find the function input by location of ()
            input_name = find_function_input(line)

            # save to dictionary             
            namedict = dict()
            namedict["repo_id"]         = str(repo_id)[0:40]
            namedict["repo_name"]       = str(repo_name)[0:30]
            namedict["class_name"]      = str(class_cur)[0:20]
            namedict["function_name"]   = str(function_name)[0:30]
            namedict["function_input"]  = str(input_name)[0:50]
            namedict["function_out"]    = ' '
            # save to tuple
            results = results + (namedict,)

        # find function output by checking if there is return 
        pos_return = re.search(r'return\s', line)
        if pos_return != None:
            tuple_len = len(results)
            if(tuple_len == 0): 
                continue
            return_loc = pos_return.span()[1]
            out_name = find_function_out(line, return_loc, out_name)
            # update the output for the last dictionary
            results[tuple_len - 1]["output"] = out_name [0:50]
            
    connection.commit()
    cur.close()
    connection.close()
    
    return results

def postgres_insert(results):
    #connect postgresql for each worker, inorder to insert to table
    try:
        connection = psycopg2.connect("dbname='{0}'".format(dbname) +
                              "user='{0}'".format(dbuser) +
                              "host='{0}'".format(dbhost) +
                              "password='{0}'".format(dbpassword))
    except:
        print "I am unable to connect to the database."
    cur = connection.cursor()

    for x in results:
        # insert to postgresql database
        try:
            cur.executemany("""INSERT INTO github_function(repo_name, class_name, function_name, function_input, function_out, repo_id) \
                 VALUES (%(repo_name)s,%(class_name)s,%(function_name)s, %(function_input)s,%(function_out)s, %(repo_id)s)""", x)
        except:
            print "can not insert to postgres"
        connection.commit()
        
    cur.close()
    connection.close()

def read_s3_json_rdd(dataname):
    dataframe_json = sqlContext.read.json(dataname)
    dataframe_json.registerTempTable("repos")
    dataframe_sql = sqlContext.sql("SELECT id, sample_repo_name, content from repos where content != ''")
    dataframe_rdd = dataframe_sql.rdd
    return dataframe_rdd


def main():
    # connect to postgresql database
    try:
        connection = psycopg2.connect("dbname='{0}'".format(dbname) +
                              "user='{0}'".format(dbuser) +
                              "host='{0}'".format(dbhost) +
                              "password='{0}'".format(dbpassword))
    except:
        print "I am unable to connect to the database."
    cur = connection.cursor()

    # connect to s3 bucket, generate RDD
    sc = SparkContext(master='******',appName ='******')
    sqlContext = SQLContext(sc)
    # read input files one by one, 500M each
    for i in range(0,86):
        name_num = '{0:03}'.format(i)
        dataname = "s3a://bucketname/******_000000000"+name_num+".json"

        # read json data from s3 using dataframe API, convert to rdd
        dataframe_rdd = read_s3_json_rdd(dataname)

        # deal with each repo and insert to postgres
        repo_num = dataframe_rdd.count()
        print "dataset name", dataname
        print "number of repo in total:", repo_num

        #foreach
        #dataframe_rdd.foreach(each_repo1)
        
        #map and foreachPartition
        results_all = ()
        results_all = dataframe_rdd.map(lambda x: each_repo(x))
        results_all.foreachPartition(postgres_insert)

    #records.collect()
    cur.execute("""SELECT * FROM github_function;""")
    rows = cur.fetchall()
    connection.commit()
    cur.close()
    connection.close()

if __name__ == "__main__":
    main()
