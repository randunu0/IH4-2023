import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlalchemy
from datetime import datetime
import os
from dotenv import load_dotenv

def scrapeWeb(link):
    url = link
    r = requests.get(url, allow_redirects=False)
    
    soup = BeautifulSoup(r.content, "html.parser")

    time = soup.find("div", {"class": "schedTime rightAlign"}).text
    time = time[14:]

    labels = soup.find_all("td", {"class": "tdLeft"})
    data = soup.find_all("td", {"class": "labelClassCenter"})

    entries = {}
    for i in range(len(labels)):
        entries[labels[i].text] = data[i].text

    return time, entries

def updateWebTable(table, link):
    load_dotenv(dotenv_path="../.env")

    #connecting to sqlalchemy
    database_username = os.getenv("DB_USERNAME")
    database_password = os.getenv("DB_PASSWORD")
    database_ip = os.getenv("DB_IP")
    database_name = os.getenv("DB_NAME")
    #port = "3306"
    database_connection = sqlalchemy.create_engine('mysql+pymysql://{0}:{1}@{2}/{3}'.
                                                   format(database_username, database_password, 
                                                          database_ip, database_name))
    connection = database_connection.connect() 

    df = pd.DataFrame()
    timestamp, entries = scrapeWeb(link)

    dt = datetime.strptime(timestamp, '%b %d, %Y %H:%M:%S')

    if table == 'RTSC':
        data = {'OperatingDay': dt.date(),
                'HourEnding': dt.time(),
                'CurrentFrequency': entries['Current Frequency'],
                'InstTimeError': entries['Instantaneous Time Error'],
                'CBCME': entries['Consecutive BAAL Clock-Minute Exceedances (min)'],
                'Inertia': entries['Current System Inertia']}
        df = df.append(data, ignore_index=True)

    elif table == 'SASC':
        data = {'OperatingDay': dt.date(),
                'HourEnding': dt.time(),
                'UndepRegUp': entries['Undeployed Reg-Up'],
                'UndepRegDown': entries['Undeployed Reg-Down'],
                'DepRegUp': entries['Deployed Reg-Up'],
                'DepRegDown': entries['Deployed Reg-Down'],
                'PRC': entries['ERCOT-wide Physical Responsive Capability (PRC)']}
        df = df.append(data, ignore_index=True)

    df.to_sql(con=connection, name=table, if_exists='append', index=False)
    print("Done")