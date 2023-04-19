from time import strftime
import pandas as pd
import sqlalchemy
from sqlalchemy import text
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
    # Demand Curve Capacity
    # -----------------------------------
    if chart_type == "demand-curve-capacity":
        if (start_date and end_date):
            end_date = pd.Timestamp(end_date)
            start_date = pd.Timestamp(start_date)
            query = """SELECT * FROM GRID_ANALYTICS.PRC WHERE OperatingDay BETWEEN '""" + start_date.strftime("%Y-%m-%d") + """' AND '""" + end_date.strftime("%Y-%m-%d") +"""'"""
        else:
            query = """SELECT * FROM GRID_ANALYTICS.PRC ORDER BY OperatingDay DESC LIMIT 1"""
            df = pd.read_sql_query(text(query), connection)
            end_date = df.iloc[-1].get('OperatingDay')
            # DEFAULT VIEW: one day
            start_date = end_date - pd.Timedelta(days=1)
            query = """SELECT * FROM GRID_ANALYTICS.PRC WHERE OperatingDay BETWEEN '""" + start_date.strftime("%Y-%m-%d") + """' AND '""" + end_date.strftime("%Y-%m-%d") +"""'"""
        df = pd.read_sql_query(text(query), connection)

        or_data = df["OnlineReserveCap"].tolist()
        oo_data = df["OnOffReserveCap"].tolist()

        df["HourEnding"] = df["HourEnding"].apply(td_to_dt)
        ch_times = df['HourEnding'].tolist()
        ch_days = df['OperatingDay'].tolist()
        
        ch_labels = combine_date_time(ch_days, ch_times)
        return [or_data, oo_data], ch_labels


    # -----------------------------------
    # Responsive Reserve Capacity
    # -----------------------------------
    if chart_type == "responsive-reserve-capacity":
        if (start_date and end_date):
            end_date = pd.Timestamp(end_date)
            start_date = pd.Timestamp(start_date)
            query = """SELECT * FROM GRID_ANALYTICS.PRC WHERE OperatingDay BETWEEN '""" + start_date.strftime("%Y-%m-%d") + """' AND '""" + end_date.strftime("%Y-%m-%d") +"""'"""
        else:
            query = """SELECT * FROM GRID_ANALYTICS.PRC ORDER BY OperatingDay DESC LIMIT 1"""
            df = pd.read_sql_query(text(query), connection)
            end_date = df.iloc[-1].get('OperatingDay')
            # DEFAULT VIEW: one day
            start_date = end_date - pd.Timedelta(days=1)
            query = """SELECT * FROM GRID_ANALYTICS.PRC WHERE OperatingDay BETWEEN '""" + start_date.strftime("%Y-%m-%d") + """' AND '""" + end_date.strftime("%Y-%m-%d") +"""'"""
        df = pd.read_sql_query(text(query), connection)

        gr_data = df["GenerationResources"].tolist()
        lr_data = df["LoadResources"].tolist()

        df["HourEnding"] = df["HourEnding"].apply(td_to_dt)
        ch_times = df['HourEnding'].tolist()
        ch_days = df['OperatingDay'].tolist()
        
        ch_labels = combine_date_time(ch_days, ch_times)

        return [gr_data, lr_data], ch_labels



    # -----------------------------------
    # System Ancillary Services
    # -----------------------------------
    if chart_type == "physical-responsive-capability":
        if (start_date and end_date):
            end_date = pd.Timestamp(end_date)
            start_date = pd.Timestamp(start_date)
            query = """SELECT * FROM GRID_ANALYTICS.PRC WHERE OperatingDay BETWEEN '""" + start_date.strftime("%Y-%m-%d") + """' AND '""" + end_date.strftime("%Y-%m-%d") +"""'"""
        else:
            query = """SELECT * FROM GRID_ANALYTICS.PRC ORDER BY OperatingDay DESC LIMIT 1"""
            df = pd.read_sql_query(text(query), connection)
            end_date = df.iloc[-1].get('OperatingDay')
            # DEFAULT VIEW: one day
            start_date = end_date - pd.Timedelta(days=1)
            query = """SELECT * FROM GRID_ANALYTICS.PRC WHERE OperatingDay BETWEEN '""" + start_date.strftime("%Y-%m-%d") + """' AND '""" + end_date.strftime("%Y-%m-%d") +"""'"""
        df = pd.read_sql_query(text(query), connection)

        ch_data = df["PhysicalResponsiveCapability"].tolist()

        df["HourEnding"] = df["HourEnding"].apply(td_to_dt)
        ch_times = df['HourEnding'].tolist()
        ch_days = df['OperatingDay'].tolist()
        
        ch_labels = combine_date_time(ch_days, ch_times)

        return ch_data, ch_labels


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
            df = pd.read_sql_query(text(query), connection)
            end_date = df.iloc[-1].get('OperatingDay')
            # DEFAULT VIEW: two weeks
            start_date = end_date - pd.Timedelta(days=14)
            query = """SELECT * FROM GRID_ANALYTICS.SWD WHERE OperatingDay BETWEEN '""" + start_date.strftime("%Y-%m-%d") + """' AND '""" + end_date.strftime("%Y-%m-%d") +"""'"""
        df = pd.read_sql_query(text(query), connection)
        
        ch_data_mw = df["Demand"].tolist()
        ch_data = [entry / 1000 for entry in ch_data_mw]
        
        df["HourEnding"] = df["HourEnding"].apply(td_to_dt)
        ch_times = df['HourEnding'].tolist()
        ch_days = df['OperatingDay'].tolist()

        ch_labels = combine_date_time(ch_days, ch_times)

        #query = """SELECT * FROM GRID_ANALYTICS.SWD WHERE DEMAND = (SELECT MAX(Demand) AND OperatingDay BETWEEN '""" + start_date.strftime("%Y-%m-%d") + """' AND '""" + end_date.strftime("%Y-%m-%d") +"""'""" + """)"""
        query = "SELECT MAX(Demand) FROM GRID_ANALYTICS.SWD WHERE Demand = (SELECT MAX(Demand) FROM GRID_ANALYTICS.SWD WHERE OperatingDay BETWEEN '" + start_date.strftime("%Y-%m-%d") + "' AND '" + end_date.strftime("%Y-%m-%d") + "')"
        query_date = "SELECT OperatingDay FROM GRID_ANALYTICS.SWD WHERE Demand = (SELECT MAX(Demand) FROM GRID_ANALYTICS.SWD WHERE OperatingDay BETWEEN '" + start_date.strftime("%Y-%m-%d") + "' AND '" + end_date.strftime("%Y-%m-%d") + "')"
        # all time max
        query_max = "SELECT MAX(Demand) FROM GRID_ANALYTICS.SWD WHERE Demand = (SELECT MAX(Demand) FROM GRID_ANALYTICS.SWD)"
        query_max_date = "SELECT OperatingDay FROM GRID_ANALYTICS.SWD WHERE Demand = (SELECT MAX(Demand) FROM GRID_ANALYTICS.SWD)"

        df_peak = pd.read_sql_query(query, connection)
        df_peak_max = pd.read_sql_query(query_max, connection)
        peak_val = [df_peak["MAX(Demand)"].to_list()[0] / 1000]
        peak_val.append(df_peak_max["MAX(Demand)"].tolist()[0] / 1000)

        df_peak_date = pd.read_sql_query(query_date, connection)
        df_peak_max_date = pd.read_sql_query(query_max_date, connection)
        peak_date = [df_peak_date["OperatingDay"].to_list()[0]]
        peak_date.append(df_peak_max_date["OperatingDay"].tolist()[0])
        
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
            df = pd.read_sql_query(text(query), connection)
            end_date = df.iloc[-1].get('OperatingDay')
            # DEFAULT VIEW: one week
            start_date = end_date - pd.Timedelta(days=7)
            query = """SELECT * FROM GRID_ANALYTICS.GBFT WHERE OperatingDay BETWEEN '""" + start_date.strftime("%Y-%m-%d") + """' AND '""" + end_date.strftime("%Y-%m-%d") +"""'"""
        df = pd.read_sql_query(text(query), connection)        
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
        
        query = """SELECT * FROM GRID_ANALYTICS.GBFT WHERE OperatingDay BETWEEN '""" + start_date.strftime("%Y-%m-%d") + """' AND '""" + end_date.strftime("%Y-%m-%d") +"""'"""
        df_peak = pd.read_sql_query(query, connection)
        
        df_peak_coal = df_peak.sort_values(by=['Coal'], ascending=False).head(1)
        peak_val_coal = df_peak_coal["Coal"]
        
        df_peak_gas = df_peak.sort_values(by=['Gas'], ascending=False).head(1)
        peak_val_gas = df_peak_gas["Gas"]

        df_peak_gas_cc = df_peak.sort_values(by=['Gas-CC'], ascending=False).head(1)
        peak_val_gas_cc = df_peak_gas_cc["Gas-CC"]

        df_peak_hydro = df_peak.sort_values(by=['Hydro'], ascending=False).head(1)
        peak_val_hydro = df_peak_hydro["Hydro"]

        df_peak_nuclear = df_peak.sort_values(by=['Nuclear'], ascending=False).head(1)
        peak_val_nuclear = df_peak_nuclear["Nuclear"]

        df_peak_other = df_peak.sort_values(by=['Other'], ascending=False).head(1)
        peak_val_other = df_peak_other["Other"]

        df_peak_solar = df_peak.sort_values(by=['Solar'], ascending=False).head(1)
        peak_val_solar = df_peak_solar["Solar"]

        df_peak_wind = df_peak.sort_values(by=['Wind'], ascending=False).head(1)
        peak_val_wind = df_peak_wind["Wind"]

            



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
            df = pd.read_sql_query(text(query), connection)
            end_date = df.iloc[-1].get('OperatingDay')
            # DEFAULT VIEW: one day
            start_date = end_date - pd.Timedelta(days=1)
            query = """SELECT * FROM GRID_ANALYTICS.RTSC WHERE OperatingDay BETWEEN '""" + start_date.strftime("%Y-%m-%d") + """' AND '""" + end_date.strftime("%Y-%m-%d") +"""'"""
        df = pd.read_sql_query(text(query), connection)

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
        
        query = "SELECT MAX(CurrentFrequency) as max_frequency FROM GRID_ANALYTICS.RTSC WHERE OperatingDay BETWEEN '" + start_date.strftime("%Y-%m-%d") + "' AND '" + end_date.strftime("%Y-%m-%d") + "' AND CurrentFrequency = (SELECT MAX(CurrentFrequency) FROM GRID_ANALYTICS.RTSC WHERE OperatingDay BETWEEN '" + start_date.strftime("%Y-%m-%d") + "' AND '" + end_date.strftime("%Y-%m-%d") + "')"
        query_date = "SELECT OperatingDay FROM GRID_ANALYTICS.RTSC WHERE OperatingDay BETWEEN '" + start_date.strftime("%Y-%m-%d") + "' AND '" + end_date.strftime("%Y-%m-%d") + "' AND CurrentFrequency = (SELECT MAX(CurrentFrequency) FROM GRID_ANALYTICS.RTSC WHERE OperatingDay BETWEEN '" + start_date.strftime("%Y-%m-%d") + "' AND '" + end_date.strftime("%Y-%m-%d") + "')"

        # all time max frequency
        query_max = "SELECT MAX(CurrentFrequency) as max_frequency FROM GRID_ANALYTICS.RTSC"
        query_max_date = "SELECT OperatingDay FROM GRID_ANALYTICS.RTSC WHERE CurrentFrequency = (SELECT MAX(CurrentFrequency) FROM GRID_ANALYTICS.RTSC)"

        df_peak = pd.read_sql_query(query, connection)
        df_peak_date = pd.read_sql_query(query_date, connection)
        df_peak_max = pd.read_sql_query(query_max, connection)
        df_peak_max_date = pd.read_sql_query(query_max_date, connection)

        peak_val = [df_peak["max_frequency"].tolist()[0]]
        peak_date = [df_peak_date["OperatingDay"].tolist()[0]]
        peak_val.append(df_peak_max["max_frequency"].tolist()[0])
        peak_date.append(df_peak_max_date["OperatingDay"].tolist()[0])

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
            df = pd.read_sql_query(text(query), connection)
            end_date = df.iloc[-1].get('OperatingDay')
            # DEFAULT VIEW: one week
            start_date = end_date - pd.Timedelta(days=7)
            query = """SELECT * FROM GRID_ANALYTICS.SPP WHERE OperatingDay BETWEEN '""" + start_date.strftime("%Y-%m-%d") + """' AND '""" + end_date.strftime("%Y-%m-%d") +"""'"""
        df = pd.read_sql_query(text(query), connection)

        pv_data = df['SystemWide'].tolist()
        # PV Times are the same as Wind Times so disregard one or the other

        if (start_date and end_date):
            end_date = pd.Timestamp(end_date)
            start_date = pd.Timestamp(start_date)
            query = """SELECT * FROM GRID_ANALYTICS.WPP WHERE OperatingDay BETWEEN '""" + start_date.strftime("%Y-%m-%d") + """' AND '""" + end_date.strftime("%Y-%m-%d") +"""'"""
        else:
            query = """SELECT * FROM GRID_ANALYTICS.WPP ORDER BY OperatingDay DESC LIMIT 1"""
            df = pd.read_sql_query(text(query), connection)
            end_date = df.iloc[-1].get('OperatingDay')
            # DEFAULT VIEW: one week
            start_date = end_date - pd.Timedelta(days=7)
            query = """SELECT * FROM GRID_ANALYTICS.WPP WHERE OperatingDay BETWEEN '""" + start_date.strftime("%Y-%m-%d") + """' AND '""" + end_date.strftime("%Y-%m-%d") +"""'"""
        df = pd.read_sql_query(text(query), connection)
        
        wind_data = df['SystemWide'].tolist()
        ch_data = [wind_data, pv_data]

        df["HourEnding"] = df["HourEnding"].apply(td_to_dt)
        wind_times = df['HourEnding'].tolist()
        wind_days = df['OperatingDay'].tolist()

        wind_labels = combine_date_time(wind_days, wind_times)
        ch_labels = wind_labels

        #query = """SELECT * FROM GRID_ANALYTICS.SWD WHERE DEMAND = (SELECT MAX(Demand) AND OperatingDay BETWEEN '""" + start_date.strftime("%Y-%m-%d") + """' AND '""" + end_date.strftime("%Y-%m-%d") +"""'""" + """)"""

        # wind peak        
        query_wind = """SELECT MAX(SystemWide) FROM GRID_ANALYTICS.WPP WHERE OperatingDay BETWEEN '""" + start_date.strftime("%Y-%m-%d") + """' AND '""" + end_date.strftime("%Y-%m-%d") +"""'"""
        #wind peak date
        query_wind_date = """SELECT OperatingDay FROM GRID_ANALYTICS.WPP WHERE SystemWide = (SELECT MAX(SystemWide) FROM GRID_ANALYTICS.WPP WHERE OperatingDay BETWEEN '""" + start_date.strftime("%Y-%m-%d") + """' AND '""" + end_date.strftime("%Y-%m-%d") +"""')"""
        # pv peak
        query_pv = """SELECT MAX(SystemWide) FROM GRID_ANALYTICS.SPP WHERE OperatingDay BETWEEN '""" + start_date.strftime("%Y-%m-%d") + """' AND '""" + end_date.strftime("%Y-%m-%d") +"""'"""
        # pv peak date
        query_pv_date = """SELECT OperatingDay FROM GRID_ANALYTICS.SPP WHERE SystemWide = (SELECT MAX(SystemWide) FROM GRID_ANALYTICS.SPP WHERE OperatingDay BETWEEN '""" + start_date.strftime("%Y-%m-%d") + """' AND '""" + end_date.strftime("%Y-%m-%d") +"""')"""

        # absolute wind peak
        query_max_wind = """SELECT MAX(SystemWide) FROM GRID_ANALYTICS.WPP"""
        # absolute wind peak date
        query_max_wind_date = """SELECT OperatingDay FROM GRID_ANALYTICS.WPP WHERE SystemWide = (SELECT MAX(SystemWide) FROM GRID_ANALYTICS.WPP)"""
        # absolute pv peak
        query_max_pv = """SELECT MAX(SystemWide) FROM GRID_ANALYTICS.SPP"""
        # absolute pv peak date
        query_max_pv_date = """SELECT OperatingDay FROM GRID_ANALYTICS.SPP WHERE SystemWide = (SELECT MAX(SystemWide) FROM GRID_ANALYTICS.SPP)"""

        df_peak = pd.read_sql_query(query_wind, connection)
        df_peak_pv = pd.read_sql_query(query_pv, connection)
        df_peak_max_wind = pd.read_sql_query(query_max_wind, connection)
        df_peak_max_pv = pd.read_sql_query(query_max_pv, connection)

        df_peak_date = pd.read_sql_query(query_wind_date, connection)
        df_peak_pv_date = pd.read_sql_query(query_pv_date, connection)
        df_peak_max_wind_date = pd.read_sql_query(query_max_wind_date, connection)
        df_peak_max_pv_date = pd.read_sql_query(query_max_pv_date, connection)

        peak_val = df_peak["MAX(SystemWide)"]
        peak_val.loc[1] = df_peak_pv["MAX(SystemWide)"]
        peak_val.loc[2] = df_peak_max_wind["MAX(SystemWide)"]
        peak_val.loc[3] = df_peak_max_pv["MAX(SystemWide)"]

        peak_date = df_peak_date["OperatingDay"]
        peak_date.loc[1] = df_peak_pv_date["OperatingDay"]
        peak_date.loc[2] = df_peak_max_wind_date["OperatingDay"]
        peak_date.loc[3] = df_peak_max_pv_date["OperatingDay"]

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
            df = pd.read_sql_query(text(query), connection)
            end_date = df.iloc[-1].get('OperatingDay')
            # DEFAULT VIEW: one day
            start_date = end_date - pd.Timedelta(days=1)
            query = """SELECT * FROM GRID_ANALYTICS.SMPP_LZ WHERE OperatingDay BETWEEN '""" + start_date.strftime("%Y-%m-%d") + """' AND '""" + end_date.strftime("%Y-%m-%d") +"""'"""
        df = pd.read_sql_query(text(query), connection)
 
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