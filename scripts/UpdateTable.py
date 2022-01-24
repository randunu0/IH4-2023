import os
import glob
import pandas as pd
import sqlalchemy
from DownloadSA import DownloadSA
from UnzipFiles import Unzip
import datetime as dt
import shutil

# updateTable(table, source)
#   Input:  Table = Name of the table to update
#           Source = ERCOT URL
#
#   Output: None
#
#   Uses DownloadSA and UnzipFiles functions to download zips, unzip, and upload csv's to DB
#   Downloads to directory '/tmp/<table>' but deletes '/<table>' directory at end   


def updateTable(table, source):
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

    #When implementing to Lambda, folder -> </tmp/ + KEY> 
    DownloadSA(source, 'tmp/' + table)
    Unzip('tmp/' + table)

    os.chdir("tmp/" + table)
    for file in glob.glob("*.csv"):
        df = pd.read_csv(file)

        df.rename(columns = {list(df)[0]:'OperatingDay', list(df)[1]:'HourEnding'}, inplace=True)   # Assumes that the first two columns in a df are Date and Time

        df["OperatingDay"] = pd.to_datetime(df["OperatingDay"]).dt.date
        df["HourEnding"] = df["HourEnding"] + ":00"
        df.to_sql(con=connection, name=table, if_exists='append', index=False)
        print(file)

    os.chdir("..")
    try:
        shutil.rmtree(table)
        print('Done')
    except:
        print("Unhandled Deletion Exception")

    cursor.close()
    db.close()