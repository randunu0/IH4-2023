import mysql.connector
from mysql.connector import errorcode
from DownloadSA import DownloadSA
import UnzipFiles
import os
import glob
import pandas as pd
import sqlalchemy
import datetime as dt

def create_database(cursor):
    try:
        cursor.execute(
            "CREATE DATABASE {} DEFAULT CHARACTER SET 'utf8'".format(DB_NAME))
    except mysql.connector.Error as err:
        print("Failed creating database: {}".format(err))
        exit(1)


DB_NAME = "GRID_ANALYTICS"

db = mysql.connector.connect(
  host="txgridanalytics-database.c3xnwzdtzngd.us-east-2.rds.amazonaws.com",
  user="admin",
  password="dvqLt7v635tuf9Bf",
  database="GRID_ANALYTICS"
)

path = os.getcwd()
website = 'https://sa.ercot.com/misapp/GetReports.do?reportTypeId=15954&reportTitle=Actual%20System%20Load%20by%20Study%20Area&showHTMLView=&mimicKey'
#DownloadSA(website, 'Real Time System Load')
#UnzipFiles.Unzip(path + '\\' + 'Real Time System Load')

RTSL = path + "\\Real Time System Load"

#Creating a cursor object using the cursor() method
cursor = db.cursor()

#Dropping RTSL table if already exists.
cursor.execute("DROP TABLE IF EXISTS RTSL")

#Creating table as per requirement
sql ='''CREATE TABLE RTSL(
   OperatingDay DATE,
   HourEnding TIME,
   Valley FLOAT,
   DSTFlag CHAR
)'''
cursor.execute(sql)

#connecting to sqlalchemy
database_username = "admin"
database_password = "dvqLt7v635tuf9Bf"
database_ip = "txgridanalytics-database.c3xnwzdtzngd.us-east-2.rds.amazonaws.com"
database_name = "GRID_ANALYTICS"
#port = "3306"
database_connection = sqlalchemy.create_engine('mysql+pymysql://{0}:{1}@{2}/{3}'.
                                               format(database_username, database_password, 
                                                      database_ip, database_name))
connection = database_connection.connect() 

# Need to use sqlalchemy to use the df.tosql(whatever)
os.chdir("Real Time System Load")
for file in glob.glob("*.csv"):
    df = pd.read_csv(file)
    df["OperatingDay"] = pd.to_datetime(df["OperatingDay"]).dt.date
    df["HourEnding"] = df["HourEnding"] + ":00"
    df.to_sql( con=connection, name='RTSL', if_exists='append', index=False)
    print(file)

cursor.close()
db.close()