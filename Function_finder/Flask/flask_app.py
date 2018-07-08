from flask import Flask
from flask import render_template
from flask import request
from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database
import pandas as pd
import psycopg2
from conconfig import *

app = Flask(__name__)

db = create_engine('postgres://%s%s/%s'%(dbuser,dbhost,dbname))
con = None
con = psycopg2.connect("dbname='{0}'".format(dbname) +
                        "user='{0}'".format(dbuser) +
                        "host='{0}'".format(dbhost) +
                        "password='{0}'".format(dbpassword))

@app.route('/')
def cesareans_input():
    return render_template("input.html")

@app.route('/output')
def cesareans_output():
    #pull 'repository name' from input field and store it
    function = request.args.get('repo_name')
    print(function)
    #query = "SELECT all the functions FROM github_function for one repo_name, order by funciton_name, limit 50"
    query = "SELECT * FROM github_function WHERE repo_name  = '%s' order by function_name limit 50" % function
    print(query)
    query_results = pd.read_sql_query(query,con)
    print(query_results)
    functions = []
    the_result = ''
    for i in range(0,query_results.shape[0]):
        functions.append(dict(repo_name = query_results.iloc[i]['repo_name'], class_name = query_results.iloc[i]['class_name'], function_name = query_results.iloc[i]['function_name'], function_input = query_results.iloc[i]['function_input'], function_out = query_results.iloc[i]['function_out']))
        the_result = ''
    return render_template("output.html", functions = functions, the_result = the_result)

if __name__ == "__main__":
    app.run(host='0.0.0.0', port='80')
