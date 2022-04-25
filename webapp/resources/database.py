import pandas as pd
import sqlalchemy
import datetime
import os

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

def convert_date_time(dates, times):
    dates_dt = []
    for day in dates:
        dates_dt.append(pd.Timestamp.to_pydatetime(day))
    return combine_date_time_24bug(dates_dt, times)

def get_chart(chart_type, start_date, end_date):

    #connecting to sqlalchemy
    database_username = os.getenv("DB_USERNAME")
    database_password = os.getenv("DB_PASSWORD")
    database_ip = os.getenv("DB_IP")
    database_name = os.getenv("DB_NAME")
    #port = os.getenv("DB_PORT")
    database_connection = sqlalchemy.create_engine('mysql+pymysql://{0}:{1}@{2}/{3}'.
                                                   format(database_username, database_password, 
                                                          database_ip, database_name))
    connection = database_connection.connect() 

    # -----------------------------------
    # System Wide Demand
    # -----------------------------------
    if chart_type == "system-wide-demand":
        df = pd.read_sql_table("SWD", connection)
        if (start_date and end_date):
            df = df[df['OperatingDay'] >= pd.Timestamp(start_date)]
            df = df[df['OperatingDay'] <= pd.Timestamp(end_date)]
        else:
            # DEFAULT VIEW: two weeks
            end_date = df.iloc[-1].get('OperatingDay')
            start_date = end_date - pd.Timedelta(days=14)
            df = df[df['OperatingDay'] >= pd.Timestamp(start_date)]
            df = df[df['OperatingDay'] <= pd.Timestamp(end_date)]                        
        ch_data_mw = df["Demand"].tolist()
        ch_data = [entry / 1000 for entry in ch_data_mw]
        ch_days = df['OperatingDay'].tolist()
        ch_times = df['HourEnding'].tolist()

        ch_days_dt = []
        for day in ch_days:
            ch_days_dt.append(pd.Timestamp.to_pydatetime(day))
        
        ch_labels = combine_date_time(ch_days_dt, ch_times)
        return ch_data, ch_labels

    # -----------------------------------
    # Fuel Type Generation
    # TODO Download Fewer Data Points
    # -----------------------------------
    if chart_type == "fuel-type-generation":
        df = pd.read_sql_table("GBFT", connection)
        if (start_date and end_date):
            df = df[df['OperatingDay'] >= pd.Timestamp(start_date)]
            df = df[df['OperatingDay'] <= pd.Timestamp(end_date)]
        else:
            # DEFAULT VIEW: one week
            end_date = df.iloc[-1].get('OperatingDay')
            start_date = end_date - pd.Timedelta(days=7)
            df = df[df['OperatingDay'] >= pd.Timestamp(start_date)]
            df = df[df['OperatingDay'] <= pd.Timestamp(end_date)] 
        
        df = df.sort_values(by=['OperatingDay', 'HourEnding'])

        ch_data = {
            "Biomass": df["Biomass"].tolist(),
            "Coal": df["Coal"].tolist(),
            "Gas": df["Gas"].tolist(),
            "Gas-CC": df["Gas-CC"].tolist(),
            "Hydro": df["Hydro"].tolist(),
            "Nuclear": df["Nuclear"].tolist(),
            "Other": df["Other"].tolist(),
            "Solar": df["Solar"].tolist(),
            "Wind": df["Wind"].tolist()
        }

        ch_days = df['OperatingDay'].tolist()
        ch_times = df['HourEnding'].tolist()

        ch_days_dt = []
        for day in ch_days:
            ch_days_dt.append(pd.Timestamp.to_pydatetime(day))
        
        ch_labels = combine_date_time(ch_days_dt, ch_times)
        return ch_data, ch_labels

    # -----------------------------------
    # System Frequency
    # TODO hour selector
    # -----------------------------------
    if chart_type == "system-frequency":
        df = pd.read_sql_table("RTSC", connection)
        if (start_date and end_date):
            df = df[df['OperatingDay'] >= pd.Timestamp(start_date)]
            df = df[df['OperatingDay'] <= pd.Timestamp(end_date)]
        else:
            # DEFAULT VIEW: one day
            end_date = df.iloc[-1].get('OperatingDay')
            start_date = end_date - pd.Timedelta(days=1)
            df = df[df['OperatingDay'] >= pd.Timestamp(start_date)]
            df = df[df['OperatingDay'] <= pd.Timestamp(end_date)] 
        ch_data = df["CurrentFrequency"].tolist()
        ch_days = df['OperatingDay'].tolist()
        ch_times = df['HourEnding'].tolist()

        ch_days_dt = []
        for day in ch_days:
            ch_days_dt.append(pd.Timestamp.to_pydatetime(day))
        
        ch_labels = combine_date_time(ch_days_dt, ch_times)
        return ch_data, ch_labels
    
    # -----------------------------------
    # Wind and PV
    # -----------------------------------
    if chart_type == "wind-and-pv":
        df = pd.read_sql_table("SPP", connection)
        if (start_date and end_date):
            df = df[df['OperatingDay'] >= pd.Timestamp(start_date)]
            df = df[df['OperatingDay'] <= pd.Timestamp(end_date)]
        else:
            # DEFAULT VIEW: one week
            end_date = df.iloc[-1].get('OperatingDay')
            start_date = end_date - pd.Timedelta(days=7)
            df = df[df['OperatingDay'] >= pd.Timestamp(start_date)]
            df = df[df['OperatingDay'] <= pd.Timestamp(end_date)] 
        solar_data = df['SystemWide'].tolist()

        # Solar Times are the same as Wind Times so disregard one
        # solar_days = df['OperatingDay'].tolist()
        # solar_times = df['HourEnding'].tolist()

        # solar_days_dt = []
        # for day in solar_days:
        #     solar_days_dt.append(pd.Timestamp.to_pydatetime(day))
        
        # solar_labels = combine_date_time(solar_days_dt, solar_times)

        df = pd.read_sql_table("WPP", connection)
        if (start_date and end_date):
            df = df[df['OperatingDay'] >= pd.Timestamp(start_date)]
            df = df[df['OperatingDay'] <= pd.Timestamp(end_date)]
        else:
            # DEFAULT VIEW: one week
            end_date = df.iloc[-1].get('OperatingDay')
            start_date = end_date - pd.Timedelta(days=7)
            df = df[df['OperatingDay'] >= pd.Timestamp(start_date)]
            df = df[df['OperatingDay'] <= pd.Timestamp(end_date)] 
        wind_data = df['SystemWide'].tolist()
        wind_days = df['OperatingDay'].tolist()
        wind_times = df['HourEnding'].tolist()

        wind_days_dt = []
        for day in wind_days:
            wind_days_dt.append(pd.Timestamp.to_pydatetime(day))
        
        wind_labels = combine_date_time(wind_days_dt, wind_times)


        return [wind_data, solar_data], wind_labels

    # -----------------------------------
    # Electricity Prices
    # -----------------------------------
    if chart_type == "electricity-prices":
        df = pd.read_sql_table("SMPP_LZ", connection)
        if (start_date and end_date):
            df = df[df['OperatingDay'] >= pd.Timestamp(start_date)]
            df = df[df['OperatingDay'] <= pd.Timestamp(end_date)]
        else:
            # DEFAULT VIEW: one day
            end_date = df.iloc[-1].get('OperatingDay')
            start_date = end_date - pd.Timedelta(days=1)
            df = df[df['OperatingDay'] >= pd.Timestamp(start_date)]
            df = df[df['OperatingDay'] <= pd.Timestamp(end_date)] 
        
        df = df.sort_values(by=['SettlementPointName', 'OperatingDay', 'HourEnding'])

        df = df.pivot(index=['OperatingDay','HourEnding'], columns=['SettlementPointName'], values=['SettlementPointPrice'])
        df.reset_index(inplace=True)

        ch_data = {
            "LZ_AEN": df["SettlementPointPrice"]["LZ_AEN"].tolist(),
            "LZ_CPS": df["SettlementPointPrice"]["LZ_CPS"].tolist(),
            "LZ_HOUSTON": df["SettlementPointPrice"]["LZ_HOUSTON"].tolist(),
            "LZ_LCRA": df["SettlementPointPrice"]["LZ_LCRA"].tolist(),
            "LZ_NORTH": df["SettlementPointPrice"]["LZ_NORTH"].tolist(),
            "LZ_RAYBN": df["SettlementPointPrice"]["LZ_RAYBN"].tolist(),
            "LZ_SOUTH": df["SettlementPointPrice"]["LZ_SOUTH"].tolist(),
            "LZ_WEST": df["SettlementPointPrice"]["LZ_WEST"].tolist()
        }

        ch_days = df['OperatingDay'].tolist()
        ch_times = df['HourEnding'].tolist()

        ch_days_dt = []
        for day in ch_days:
            ch_days_dt.append(pd.Timestamp.to_pydatetime(day))
        
        ch_labels = combine_date_time(ch_days_dt, ch_times)
        return ch_data, ch_labels

    return pd.read_sql_table(chart_type, connection)