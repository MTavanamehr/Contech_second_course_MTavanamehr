import psycopg2
from datetime import datetime

# basic needed definition
name_table = 'data_source_MondayData'
Sec_Table = 'junk_data_Monday'

## create connection to postgreSQL Database and create table for data (Our database is container)
postgresConnection = psycopg2.connect(host="192.168.0.246", port="5432", database="postgres", user="postgres", password="1qazXSW@")
cursor = postgresConnection.cursor()

## create table to put information on it
create_table = '''create table '''+name_table+''' (SERIAL_NUMBER varchar(15), PAYMENT_TYPE varchar(5), 
PROVINCE_NAME varchar(50), GENDER varchar(10), PAYMENT numeric, SMS real, DATA real, DATA_PACKAGE real, VOICE real)'''
cursor.execute(create_table)
postgresConnection.commit()

create_table = '''create table '''+Sec_Table+''' (SERIAL_NUMBER varchar(15), Gender varchar(10), 
AGE integer, Nationality varchar(10))'''
cursor.execute(create_table)
postgresConnection.commit()
