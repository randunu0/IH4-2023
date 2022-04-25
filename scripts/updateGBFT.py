import pandas as pd
import datetime as dt
from bs4 import BeautifulSoup
import requests
import os
import glob
import sqlalchemy
from dotenv import load_dotenv

def main():
    load_dotenv(dotenv_path="../.env")
    r = requests.get('https://www.ercot.com/gridinfo/generation', allow_redirects=True)
    soup = BeautifulSoup(r.content, "html.parser")
    
    year = str(dt.date.today().year)
    link = soup.find_all('a', {'title' : 'Fuel Mix Report: ' + year})
    file = link[0]["href"]
    
    r = requests.get(file, allow_redirects=True)
    
    open('tmp/FMXR.xlsx', 'wb').write(r.content)
    
    # Select sheet for previous month and read it as df
    months = {
        1: 'Jan',
        2: 'Feb',
        3: 'Mar',
        4: 'Apr',
        5: 'May',
        6: 'Jun',
        7: 'Jul',
        8: 'Aug',
        9: 'Sep',
        10: 'Oct',
        11: 'Nov',
        12: 'Dec'
    }
    
    month = months[(dt.date.today().replace(day=1) - dt.timedelta(days=1)).month]
   
    os.chdir('tmp')
    xlsx = glob.glob('*.xlsx')
    sheets = pd.read_excel(xlsx[0], sheet_name=[month])
    
    source = sheets[month]
    source.rename(columns={'0:00': '24:00'}, inplace=True)
    df = pd.DataFrame(columns=['OperatingDay', 'HourEnding', 'Fuel', 'SettlementType', 'Generation'])

    for i in range(source.shape[0]):
        item = source.iloc[i]
        entry = [0] * 5
        entry[0] = item['Date'].date()
        entry[2] = item['Fuel']
        entry[3] = item['Settlement Type']
        hr = 0
        min = 15
        while str(hr) + ':' + str(min) != '24:15':
            if min == 0:
                entry[1] = str(hr) + ':00'
                entry[4] = item[str(hr) + ':00']
            else:
                entry[1] = str(hr) + ':' + str(min)
                entry[4] = item[str(hr) + ':' + str(min)]
            if min == 45:
                min = 0
                hr += 1
            else:
                min += 15
                df.loc[len(df)] = entry

    res = df.sort_values(by=['Fuel', 'OperatingDay', 'HourEnding'])
    res = res.pivot(index=['OperatingDay','HourEnding'], columns=['Fuel'], values=['Generation'])
    res.reset_index(inplace=True)

    res.columns = res.columns.droplevel(level=0)
    res.columns = ['OperatingDay', 'HourEnding', 'Biomass', 'Coal', 'Gas', 'Gas-CC', 'Hydro', 'Nuclear', 'Other', 'Solar', 'Wind']

    database_username = os.getenv("DB_USERNAME")
    database_password = os.getenv("DB_PASSWORD")
    database_ip = os.getenv("DB_IP")
    database_name = os.getenv("DB_NAME")
    db_connection = sqlalchemy.create_engine('mysql+pymysql://{0}:{1}@{2}/{3}'.format(database_username, database_password, database_ip, database_name))

    connection = db_connection.connect() 

    res.to_sql(con=connection, name="GBFT", if_exists='append', index=False)

    connection.close()
    db_connection.dispose()
    os.remove('FMXR.xlsx')

if __name__ == "__main__":
    main()