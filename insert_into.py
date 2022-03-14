import psycopg2
from pandas import read_csv, DataFrame, concat
from numpy import shape
from datetime import datetime
from selenium import webdriver
from bs4 import BeautifulSoup
from time import sleep
from random import randint
from sys import argv

# basic needed definition

count_extract = 100000 // 5000
Serial_Num = []
genders = []
ages = []
nations = []
genders_m = []
ages_m = []
nations_m = []
Serial_Number = []
host_IP = argv[1]
WEBDriver = argv[2]
path_main = argv[3]
log_path = argv[4]
# create specific log file
log = open(log_path, 'w')

def logs(step):
    log.write('%s \t '+step+' \n' % datetime.now())
    print('%s \t '+step+' \n' % datetime.now())

def insert_into_postgre_table(table_name,rows,columns,values):
    column = ','.join(columns)
    n = len(columns)
    a = (('%s'),)*n
    v = ','.join(a)
    for i in range(rows):
        try:
            Insert = "INSERT into "+table_name+" ("+column+") values("+v+")"
            value = values[i]
            cursor.execute(Insert,value)
            postgresConnection.commit()
        except psycopg2.Error as e:
            log.write("The index of row is '{0}' occure in '{1}': \t '{2}' \t '{3}' \t '{4}' \n".format(values[i][0],datetime.now(),e.pgcode,e.pgerror,e.diag.message_detail))
            print("The index of row is '{0}' occure in '{1}': \t '{2}' \t '{3}' \t '{4}'".format(values[i][0],datetime.now(),e.pgcode,e.pgerror,e.diag.message_detail))
    
        if i % 100000 == 0:
            cursor.close()
            logs('The 100000 batch is passed!')
            cursor = postgresConnection.cursor()
        elif i % rows == 0:
            logs('loading data is finished!')
            cursor.close()

## create connection to postgreSQL Database and create table for data (Our database is container)
postgresConnection = psycopg2.connect(host=host_IP, port="5432", database="postgres", user="postgres", password="1qazXSW@")
cursor = postgresConnection.cursor()
cursor.execute('select max(serial_number) from data_source_mondaydata')
latest = cursor.fetchone()
last_sn = int(latest[0][1:])
cursor.close()

logs('loading data from csv is started!')
csv_data = read_csv(path_main)
try:
    csv_data = csv_data.drop('SERIAL_NUMBER',axis = 1)
except:
    print('No Serial_number field exist')

for i in range(len(csv_data)):
    last_sn += 1
    SN = 'C'+'%07d' %last_sn
    Serial_Number.append(SN)

SRI_NUM = DataFrame({'SERIAL_NUMBER':Serial_Number})
data = concat([SRI_NUM,csv_data],axis=1)

logs('loading data to database is started!')
## change format of dataframe to numpy array and insert to table
vals = [tuple(x) for x in data.to_numpy()] 
count_all,_ = shape(vals)
cols = ['SERIAL_NUMBER','PAYMENT_TYPE','PROVINCE_NAME','GENDER','PAYMENT','SMS','DATA','DATA_PACKAGE','VOICE']
table = 'data_source_MondayData'
# it inserts information one by one
cursor = postgresConnection.cursor()
insert_into_postgre_table(table,count_all,cols,vals)

# extract some junk information to add real information to do more analyze practice
## I can extract all information from site but it is a lot information and I got different error
logs('generating junk data is started!')
driver = webdriver.Chrome(WEBDriver)
if count_all > 100000:
    for i in range(count_extract):
        sleep(40) # for avoiding high request from site to be blocked
        url = "https://randomuser.me/api/1.3/?inc=gender,dob,nat&format=xml&results=5000"
        driver.get(url)
        sleep(10) # loading page time it can be right in another way too
        html = driver.page_source
        soup = BeautifulSoup(html,'html.parser')
        sn = i*5000
        for a in soup.find_all('results'):
    # logger counter
            sn += 1
    # extract gender
            gender = a.find('gender')
            if gender is not None:
                genders.append(gender.text)
            else:
                genders.append(genders[randint(0,len(genders)-1)])
    # extract age
            age = a.find('age')
            if age is not None:
                ages.append(age.text)
            else:
                ages.append(ages[randint(0,len(ages)-1)])
    # extract nat
            nation = a.find('nat')
            if nation is not None:
                nations.append(nation.text)
            else:
                nations.append(nations[randint(0,len(nations)-1)])

            if sn % 10000 == 0: # for avoiding high request from site to be blocked
                logs('The 10000 batch is passed!')

            if sn % 50000 == 0: # for avoiding high request from site to be blocked
                sleep(180)
    logs('generating internal junk data is started!')
# creating junk data with the information is collect from site
    for i in range(count_all):
        last_sn += 1
        SN = 'C'+'%07d' %last_sn
        Serial_Num.append(SN)
        genders_m.append(genders[randint(0,len(genders)-1)])
        ages_m.append(ages[randint(0,len(ages)-1)])
        nations_m.append(nations[randint(0,len(nations)-1)])
else:
    logs('generating internal junk data is started!')
    for i in range(count_all):
        sleep(40) # for avoiding high request from site to be blocked
        url = "https://randomuser.me/api/1.3/?inc=gender,dob,nat&format=xml&results=5000"
        driver.get(url)
        sleep(10) # loading page time it can be right in another way too
        html = driver.page_source
        soup = BeautifulSoup(html,'html.parser')
        sn = i*5000
        for a in soup.find_all('results'):
    # logger counter
            sn += 1
    # Serial_Number creator
            last_sn += 1
            SN = 'C'+'%07d' %last_sn
            Serial_Num.append(SN)
    # extract gender
            gender = a.find('gender')
            if gender is not None:
                genders_m.append(gender.text)
            else:
                genders_m.append(genders_m[randint(0,len(genders_m)-1)])
    # extract age
            age = a.find('age')
            if age is not None:
                ages_m.append(age.text)
            else:
                ages_m.append(ages_m[randint(0,len(ages_m)-1)])
    # extract nat
            nation = a.find('nat')
            if nation is not None:
                nations_m.append(nation.text)
            else:
                nations_m.append(nations_m[randint(0,len(nations_m)-1)])

            if sn % 10000 == 0: # for avoiding high request from site to be blocked
                logs('The 10000 batch is passed!')

            if sn % 50000 == 0: # for avoiding high request from site to be blocked
                sleep(180)

data = DataFrame({'Serial_number':Serial_Num,'Gender':genders_m,'AGE':ages_m,'Nationality':nations_m})
logs('insert junk data to database is started!')
## change format of dataframe to numpy array and insert to table
values_s = [tuple(x) for x in data.to_numpy()] 
cols_s = ['SERIAL_NUMBER','Gender','AGE','Nationality']
table = 'junk_data_Monday'
# insert information to junk table
cursor = postgresConnection.cursor()
insert_into_postgre_table(table,count_all,cols_s,values_s)