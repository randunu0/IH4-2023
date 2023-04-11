from time import strftime
import pandas as pd
import sqlalchemy
import datetime
import os

def td_to_dt(td):
    time = [str(td.components.hours), str(td.components.minutes), str(td.components.seconds)]
    return datetime.datetime.strptime(':'.join(time), '%H:%M:%S').time()

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
        if (start_date and end_date):
            end_date = pd.Timestamp(end_date)
            start_date = pd.Timestamp(start_date)
            query = """SELECT * FROM GRID_ANALYTICS.SWD WHERE OperatingDay BETWEEN '""" + start_date.strftime("%Y-%m-%d") + """' AND '""" + end_date.strftime("%Y-%m-%d") +"""'"""
        else:
            query = """SELECT * FROM GRID_ANALYTICS.SWD ORDER BY OperatingDay DESC LIMIT 1"""
            df = pd.read_sql_query(query, connection)
            end_date = df.iloc[-1].get('OperatingDay')
            # DEFAULT VIEW: two weeks
            start_date = end_date - pd.Timedelta(days=14)
            query = """SELECT * FROM GRID_ANALYTICS.SWD WHERE OperatingDay BETWEEN '""" + start_date.strftime("%Y-%m-%d") + """' AND '""" + end_date.strftime("%Y-%m-%d") +"""'"""
        df = pd.read_sql_query(query, connection)
        
        ch_data_mw = df["Demand"].tolist()
        ch_data = [entry / 1000 for entry in ch_data_mw]
        
        df["HourEnding"] = df["HourEnding"].apply(td_to_dt)
        ch_times = df['HourEnding'].tolist()
        ch_days = df['OperatingDay'].tolist()

        ch_labels = combine_date_time(ch_days, ch_times)

        #query = """SELECT * FROM GRID_ANALYTICS.SWD WHERE DEMAND = (SELECT MAX(Demand) AND OperatingDay BETWEEN '""" + start_date.strftime("%Y-%m-%d") + """' AND '""" + end_date.strftime("%Y-%m-%d") +"""'""" + """)"""
        query = "SELECT OperatingDay, MAX(Demand) as max_demand FROM GRID_ANALYTICS.SWD WHERE OperatingDay BETWEEN '" + start_date.strftime("%Y-%m-%d") + "' AND '" + end_date.strftime("%Y-%m-%d") + "' AND Demand = (SELECT MAX(Demand) FROM GRID_ANALYTICS.SWD WHERE OperatingDay BETWEEN '" + start_date.strftime("%Y-%m-%d") + "' AND '" + end_date.strftime("%Y-%m-%d") + "')"

        df_peak = pd.read_sql_query(query, connection)
        peak_val = df_peak["max_demand"].tolist()[0] / 1000
        peak_date = df_peak["OperatingDay"].tolist()[0]

        return ch_data, ch_labels, peak_val, peak_date

    # -----------------------------------
    # Fuel Type Generation
    # TODO Decimate
    # -----------------------------------
    if chart_type == "fuel-type-generation":
        if (start_date and end_date):
            end_date = pd.Timestamp(end_date)
            start_date = pd.Timestamp(start_date)
            query = """SELECT * FROM GRID_ANALYTICS.GBFT WHERE OperatingDay BETWEEN '""" + start_date.strftime("%Y-%m-%d") + """' AND '""" + end_date.strftime("%Y-%m-%d") +"""'"""
        else:
            query = """SELECT * FROM GRID_ANALYTICS.GBFT ORDER BY OperatingDay DESC LIMIT 1"""
            df = pd.read_sql_query(query, connection)
            end_date = df.iloc[-1].get('OperatingDay')
            # DEFAULT VIEW: one week
            start_date = end_date - pd.Timedelta(days=7)
            query = """SELECT * FROM GRID_ANALYTICS.GBFT WHERE OperatingDay BETWEEN '""" + start_date.strftime("%Y-%m-%d") + """' AND '""" + end_date.strftime("%Y-%m-%d") +"""'"""
        df = pd.read_sql_query(query, connection)        
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

        df["HourEnding"] = df["HourEnding"].apply(td_to_dt)
        ch_times = df['HourEnding'].tolist()
        ch_days = df['OperatingDay'].tolist()
        
        ch_labels = combine_date_time_24bug(ch_days, ch_times)
        
        query = """SELECT * FROM GRID_ANALYTICS.SWD WHERE DEMAND = (SELECT MAX(Demand) AND OperatingDay BETWEEN '""" + start_date.strftime("%Y-%m-%d") + """' AND '""" + end_date.strftime("%Y-%m-%d") +"""'""" + """)"""
        df_peak = pd.read_sql_query(query, connection)
        peak_val = df_peak["Demand"]
        peak_date = df_peak["OperatingDay"]

        return ch_data, ch_labels, peak_val, peak_date

    # -----------------------------------
    # System Frequency
    # TODO hour selector
    # -----------------------------------
    if chart_type == "system-frequency":
        if (start_date and end_date):
            end_date = pd.Timestamp(end_date)
            start_date = pd.Timestamp(start_date)
            query = """SELECT * FROM GRID_ANALYTICS.RTSC WHERE OperatingDay BETWEEN '""" + start_date.strftime("%Y-%m-%d") + """' AND '""" + end_date.strftime("%Y-%m-%d") +"""'"""
        else:
            query = """SELECT * FROM GRID_ANALYTICS.RTSC ORDER BY OperatingDay DESC LIMIT 1"""
            df = pd.read_sql_query(query, connection)
            end_date = df.iloc[-1].get('OperatingDay')
            # DEFAULT VIEW: one day
            start_date = end_date - pd.Timedelta(days=1)
            query = """SELECT * FROM GRID_ANALYTICS.RTSC WHERE OperatingDay BETWEEN '""" + start_date.strftime("%Y-%m-%d") + """' AND '""" + end_date.strftime("%Y-%m-%d") +"""'"""
        df = pd.read_sql_query(query, connection)

        ch_data = df["CurrentFrequency"].tolist()

        df["HourEnding"] = df["HourEnding"].apply(td_to_dt)
        ch_times = df['HourEnding'].tolist()
        ch_days = df['OperatingDay'].tolist()
        
        ch_labels = combine_date_time(ch_days, ch_times)

        #query = """SELECT * FROM GRID_ANALYTICS.SWD WHERE Demand = (SELECT MAX(Demand) AND OperatingDay BETWEEN '""" + start_date.strftime("%Y-%m-%d") + """' AND '""" + end_date.strftime("%Y-%m-%d") +"""'""" + """)"""
        #query = """SELECT * FROM GRID_ANALYTICS.RTSC WHERE CurrentFrequency = (SELECT MAX(CurrentFrequency) FROM GRID_ANALYTICS.RTSC)"""
        #query = "SELECT * FROM GRID_ANALYTICS.RTSC WHERE CurrentFrequency = (SELECT MAX(CurrentFrequency) FROM GRID_ANALYTICS.RTSC WHERE OperatingDay BETWEEN '" + start_date.strftime("%Y-%m-%d") + "' AND '" + end_date.strftime("%Y-%m-%d") + "')"
        
        #query = "SELECT OperatingDay FROM (GRID_ANALYTICS.RTSC WHERE (CurrentFrequency = (SELECT MAX(CurrentFrequency) FROM (GRID_ANALYTICS.RTSC WHERE (OperatingDay BETWEEN '" + start_date.strftime("%Y-%m-%d") + "' AND '" + end_date.strftime("%Y-%m-%d") +"')))))"
        #query = "SELECT OperatingDay FROM GRID_ANALYTICS.RTSC WHERE CurrentFrequency = (SELECT MAX(CurrentFrequency) FROM GRID_ANALYTICS.RTSC WHERE OperatingDay BETWEEN '" + start_date.strftime("%Y-%m-%d") + "' AND '" + end_date.strftime("%Y-%m-%d") + "')"
        #query = SELECT OperatingDay BETWEEN '2023-02-11' AND '2023-02-11' AND (CurrentFrequency = (SELECT MAX(CurrentFrequency) FROM GRID_ANALYTICS.RTSC WHERE OperatingDay BETWEEN '2023-02-11' AND '2023-02-11'))
        query = "SELECT OperatingDay, MAX(CurrentFrequency) as max_frequency FROM GRID_ANALYTICS.RTSC WHERE OperatingDay BETWEEN '" + start_date.strftime("%Y-%m-%d") + "' AND '" + end_date.strftime("%Y-%m-%d") + "' AND CurrentFrequency = (SELECT MAX(CurrentFrequency) FROM GRID_ANALYTICS.RTSC WHERE OperatingDay BETWEEN '" + start_date.strftime("%Y-%m-%d") + "' AND '" + end_date.strftime("%Y-%m-%d") + "')"
        df_peak = pd.read_sql_query(query, connection)
        peak_val = df_peak["max_frequency"].tolist()[0]
        peak_date = df_peak["OperatingDay"].tolist()[0]

        return ch_data, ch_labels, peak_val, peak_date
    
    # -----------------------------------
    # Wind and PV
    # -----------------------------------
    if chart_type == "wind-and-pv":
        if (start_date and end_date):
            end_date = pd.Timestamp(end_date)
            start_date = pd.Timestamp(start_date)
            query = """SELECT * FROM GRID_ANALYTICS.SPP WHERE OperatingDay BETWEEN '""" + start_date.strftime("%Y-%m-%d") + """' AND '""" + end_date.strftime("%Y-%m-%d") +"""'"""
        else:
            query = """SELECT * FROM GRID_ANALYTICS.SPP ORDER BY OperatingDay DESC LIMIT 1"""
            df = pd.read_sql_query(query, connection)
            end_date = df.iloc[-1].get('OperatingDay')
            # DEFAULT VIEW: one week
            start_date = end_date - pd.Timedelta(days=7)
            query = """SELECT * FROM GRID_ANALYTICS.SPP WHERE OperatingDay BETWEEN '""" + start_date.strftime("%Y-%m-%d") + """' AND '""" + end_date.strftime("%Y-%m-%d") +"""'"""
        df = pd.read_sql_query(query, connection)

        pv_data = df['SystemWide'].tolist()
        # PV Times are the same as Wind Times so disregard one or the other

        if (start_date and end_date):
            end_date = pd.Timestamp(end_date)
            start_date = pd.Timestamp(start_date)
            query = """SELECT * FROM GRID_ANALYTICS.WPP WHERE OperatingDay BETWEEN '""" + start_date.strftime("%Y-%m-%d") + """' AND '""" + end_date.strftime("%Y-%m-%d") +"""'"""
        else:
            query = """SELECT * FROM GRID_ANALYTICS.WPP ORDER BY OperatingDay DESC LIMIT 1"""
            df = pd.read_sql_query(query, connection)
            end_date = df.iloc[-1].get('OperatingDay')
            # DEFAULT VIEW: one week
            start_date = end_date - pd.Timedelta(days=7)
            query = """SELECT * FROM GRID_ANALYTICS.WPP WHERE OperatingDay BETWEEN '""" + start_date.strftime("%Y-%m-%d") + """' AND '""" + end_date.strftime("%Y-%m-%d") +"""'"""
        df = pd.read_sql_query(query, connection)
        
        wind_data = df['SystemWide'].tolist()
        ch_data = [wind_data, pv_data]

        df["HourEnding"] = df["HourEnding"].apply(td_to_dt)
        wind_times = df['HourEnding'].tolist()
        wind_days = df['OperatingDay'].tolist()

        wind_labels = combine_date_time(wind_days, wind_times)
        ch_labels = wind_labels

        #query = """SELECT * FROM GRID_ANALYTICS.SWD WHERE DEMAND = (SELECT MAX(Demand) AND OperatingDay BETWEEN '""" + start_date.strftime("%Y-%m-%d") + """' AND '""" + end_date.strftime("%Y-%m-%d") +"""'""" + """)"""

        # wind peak        
        query = """SELECT OperatingDay, MAX(SystemWide) FROM GRID_ANALYTICS.WPP WHERE OperatingDay BETWEEN '""" + start_date.strftime("%Y-%m-%d") + """' AND '""" + end_date.strftime("%Y-%m-%d") +"""'"""

        # pv peak
        query_pv = """SELECT OperatingDay, MAX(SystemWide) FROM GRID_ANALYTICS.SPP WHERE OperatingDay BETWEEN '""" + start_date.strftime("%Y-%m-%d") + """' AND '""" + end_date.strftime("%Y-%m-%d") +"""'"""


        df_peak = pd.read_sql_query(query, connection)
        df_peak_pv = pd.read_sql_query(query_pv, connection)
        
        peak_val = df_peak["MAX(SystemWide)"]
        peak_val.loc[1] = df_peak_pv["MAX(SystemWide)"]

        peak_date = df_peak["OperatingDay"]
        peak_date.loc[1] = df_peak_pv["OperatingDay"]

        return ch_data, ch_labels, peak_val, peak_date

    # -----------------------------------
    # Electricity Prices
    # -----------------------------------
    if chart_type == "electricity-prices":
        if (start_date and end_date):
            end_date = pd.Timestamp(end_date)
            start_date = pd.Timestamp(start_date)
            query = """SELECT * FROM GRID_ANALYTICS.SMPP_LZ WHERE OperatingDay BETWEEN '""" + start_date.strftime("%Y-%m-%d") + """' AND '""" + end_date.strftime("%Y-%m-%d") +"""'"""
        else:
            query = """SELECT * FROM GRID_ANALYTICS.SMPP_LZ ORDER BY OperatingDay DESC LIMIT 1"""
            df = pd.read_sql_query(query, connection)
            end_date = df.iloc[-1].get('OperatingDay')
            # DEFAULT VIEW: one day
            start_date = end_date - pd.Timedelta(days=1)
            query = """SELECT * FROM GRID_ANALYTICS.SMPP_LZ WHERE OperatingDay BETWEEN '""" + start_date.strftime("%Y-%m-%d") + """' AND '""" + end_date.strftime("%Y-%m-%d") +"""'"""
        df = pd.read_sql_query(query, connection)
 
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

        df["HourEnding"] = df["HourEnding"].apply(td_to_dt)
        ch_times = df['HourEnding'].tolist()
        ch_days = df['OperatingDay'].tolist()
        
        ch_labels = combine_date_time(ch_days, ch_times)

        #query = """SELECT * FROM GRID_ANALYTICS.SWD WHERE DEMAND = (SELECT MAX(Demand) AND OperatingDay BETWEEN '""" + start_date.strftime("%Y-%m-%d") + """' AND '""" + end_date.strftime("%Y-%m-%d") +"""'""" + """)"""
        # In the given time range: For each SettlementPointName, give me the max SettlementPointPrice and the OperatingDay it occurred on, then sort by OperatingDay

        # (SELECT MAX(SettlementPointPrice) FROM GRID_ANALYTICS.SMPP_LZ WHERE OperatingDay BETWEEN '""" + start_date.strftime("%Y-%m-%d") + """' AND '""" + end_date.strftime("%Y-%m-%d") +"""'""" + """)
        #query = """SELECT MAX(SettlementPointPrice), OperatingDay FROM GRID_ANALYTICS.SMPP_LZ WHERE OperatingDay BETWEEN '""" + start_date.strftime("%Y-%m-%d") + """' AND '""" + end_date.strftime("%Y-%m-%d") +"""'"""

        query = """SELECT OperatingDay, MAX(SettlementPointPrice) FROM GRID_ANALYTICS.SMPP_LZ WHERE OperatingDay BETWEEN '""" + start_date.strftime("%Y-%m-%d") + """' AND '""" + end_date.strftime("%Y-%m-%d") +"""'"""
        
        df_peak = pd.read_sql_query(query, connection)
        peak_val = df_peak["MAX(SettlementPointPrice)"]
        peak_date = df_peak["OperatingDay"]

        return ch_data, ch_labels, peak_val, peak_date

    return pd.read_sql_table(chart_type, connection)