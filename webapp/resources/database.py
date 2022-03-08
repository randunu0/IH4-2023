from tracemalloc import start
import pandas as pd
import sqlalchemy
import datetime

def combine_date_time_24bug(date, time):
    combined = []
    for x in range(len(date)):
        if time[x] == datetime.time(hour=0, minute=0):
            temp_combination = str(datetime.datetime.combine(date[x] + datetime.timedelta(days=1), time[x]).strftime('%Y-%m-%d %H:%M'))
        else:
            temp_combination = str(datetime.datetime.combine(date[x], time[x]).strftime('%Y-%m-%d %H:%M'))
        combined.append(temp_combination)
    return combined

def combine_date_time(date, time):
    combined = []
    for x in range(len(date)):
        combined.append(str(datetime.datetime.combine(date[x], time[x]).strftime('%Y-%m-%d %H:%M')))
    return combined

def get_chart(chart_type, start_date, end_date):

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

    df = pd.read_sql_table(chart_type, connection)

    if chart_type == "RTSL":
        if (start_date and end_date):
            df = df[df['OperatingDay'] >= pd.Timestamp(start_date)]
            df = df[df['OperatingDay'] <= pd.Timestamp(end_date)]
        ch_data = df["Valley"].tolist()
        ch_days = df['OperatingDay'].tolist()
        ch_times = df['HourEnding'].tolist()

        ch_days_dt = []
        for day in ch_days:
            ch_days_dt.append(pd.Timestamp.to_pydatetime(day))
        
        ch_labels = combine_date_time_24bug(ch_days_dt, ch_times)
        return ch_data, ch_labels
    
    if chart_type == "SWL":
        if (start_date and end_date):
            df = df[df['OperatingDay'] >= pd.Timestamp(start_date)]
            df = df[df['OperatingDay'] <= pd.Timestamp(end_date)]
        ch_data = df['Demand'].tolist()
        ch_days = df['OperatingDay'].tolist()
        ch_times = df['HourEnding'].tolist()

        ch_days_dt = []
        for day in ch_days:
            ch_days_dt.append(pd.Timestamp.to_pydatetime(day))
        
        ch_labels = combine_date_time(ch_days_dt, ch_times)
        return ch_data, ch_labels
    
    if chart_type == "SPP":
        if (start_date and end_date):
            df = df[df['OperatingDay'] >= pd.Timestamp(start_date)]
            df = df[df['OperatingDay'] <= pd.Timestamp(end_date)]
        ch_data = df['SystemWide'].tolist()
        ch_days = df['OperatingDay'].tolist()
        ch_times = df['HourEnding'].tolist()

        ch_days_dt = []
        for day in ch_days:
            ch_days_dt.append(pd.Timestamp.to_pydatetime(day))
        
        ch_labels = combine_date_time(ch_days_dt, ch_times)
        return ch_data, ch_labels

    return df