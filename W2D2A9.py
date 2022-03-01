import psycopg2
from pydataset import data
import pandas as pd

# load data from dataset to put in table
df = data('iris')
df = pd.DataFrame(df)

# create connetion to postgreSQL
postgresConnection = psycopg2.connect(host="192.168.0.246", port='5432', database="postgres", user="postgres", password="1qazXSW@")
cursor = postgresConnection.cursor()

# create table in postgreSQL from Python
name_table = 'iris'
# use df.info() to find Dtype
sqlCreateTable = """create table """+name_table+""" (SepalLength real, SepalWidth real, PetalLength real, 
PetalWidth real, Species varchar(20));"""
cursor.execute(sqlCreateTable)
postgresConnection.commit()

# insert all DataFrame information to table
stmt= "INSERT into "+name_table+" (SepalLength, SepalWidth, PetalLength, PetalWidth, Species) values(%s,%s,%s,%s,%s)"
tuples = [tuple(x) for x in df.to_numpy()] 
cursor.executemany(stmt,tuples)
# https://dev.mysql.com/doc/connector-python/en/connector-python-api-mysqlcursor-executemany.html

# check the information
select_count = 'select count(*) from '+name_table+''
cursor.execute(select_count)
cursor.fetchall()

# load information from database to use -- two way
## first select directly and execute from table (without index)
select = 'select * from '+name_table+''
cursor.execute(select)
new_df = cursor.fetchall()
new_df = pd.DataFrame(new_df)

## second use Pandas read_sql engine (with index)
new_df = pd.read_sql('select * from '+name_table+'',postgresConnection)
# https://pythontic.com/pandas/serialization/postgresql

# close connection to database
cursor.close()