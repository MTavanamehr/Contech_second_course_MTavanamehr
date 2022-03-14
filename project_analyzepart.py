import pandas as pd
import psycopg2
import matplotlib.pyplot as plt
from sys import argv

host_IP = argv[1]
# create connetion to postgreSQL and loading data from database
postgresConnection = psycopg2.connect(host=host_IP, port='5432', database="postgres", user="postgres", password="1qazXSW@")
cursor = postgresConnection.cursor()
data = pd.read_sql('select * from Course2_ToolTechniquesforDataScience',postgresConnection)
cursor.close()

# ready for analyze data
## look to data
print('look to data')
print(data.head())
print(data.describe())
print(data.info())
''' we found negative values in SMS it is normal in business, but for ignoring affect of them 
(because the value are usage per price), changing them to zero
'''
print('checking the null values in dataset')
print(data.count())
print(data.isna().sum())
data.dropna(inplace=True) # the rows of that contain with null is less than 1 percent so we drop them
print(data.isnull().sum())

Province_Payment_POS = data.groupby(['province_name','payment_type']).count()['serial_number'].unstack().sort_values(by='POS',ascending=False)['POS']
Province_Payment_PRE = data.groupby(['province_name','payment_type']).count()['serial_number'].unstack().sort_values(by='PRE',ascending=False)['PRE']

Province_Payment_POS.plot(kind='barh')
plt.show()
Province_Payment_PRE.plot(kind='barh')
plt.show()

print('''According to graphs we get below points, \n
     1- the PRE subscribers are more than POS subscribers \n
     2- Two province Tehran & Esfahan has more subscribers than other \n
     3- There is limitation to accept subscribers based on PRE/POS and Province exist on project So for analyze we need to separate Two provinces (Tehran & Esfan) to avoid the negative point in out analyze''')