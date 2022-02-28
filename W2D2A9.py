import psycopg2
from pydataset import data


# load data from dataset to put in table
df = data('iris')

# create connetion to postgreSQL
postgresConnection = psycopg2.connect(host="192.168.0.246", port='5432', database="postgres", user="postgres", password="1qazXSW@")
cursor = postgresConnection.cursor()

# create table in postgreSQL from Python
name_table = 'iris3'
# use df.info() to find Dtype
sqlCreateTable = "create table "+name_table+" (SepalLength NUMERIC(3, 3), SepalWidth NUMERIC(3, 3), PetalLength NUMERIC(3, 3), PetalWidth NUMERIC(3, 3), Species varchar(20));"
cursor.execute(sqlCreateTable)
postgresConnection.commit()

# insert data to table
def single_insert(postgresConnection, insert_req):
    """ Execute a single INSERT request """
    cursor = postgresConnection.cursor()
    try:
        cursor.execute(insert_req)
        postgresConnection.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        postgresConnection.rollback()
        cursor.close()
        return 1
    cursor.close()

for i in df.index:
    query = """
    INSERT into """+name_table+"""(SepalLength, SepalWidth, PetalLength, PetalWidth, Species) values(%s,%s,%s,%s,%s);
    """ % (df['Sepal.Length'], df['Sepal.Width'], df['Petal.Length'], df['Petal.Width'], df['Species'])
    single_insert(postgresConnection, query)