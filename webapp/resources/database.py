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

        # OnlineReserveCap peak        
        query_or = """SELECT MAX(OnlineReserveCap) FROM GRID_ANALYTICS.PRC WHERE OperatingDay BETWEEN '""" + start_date.strftime("%Y-%m-%d") + """' AND '""" + end_date.strftime("%Y-%m-%d") +"""'"""
        # OnlineReserveCap peak date
        query_or_date = """SELECT OperatingDay FROM GRID_ANALYTICS.PRC WHERE OnlineReserveCap = (SELECT MAX(OnlineReserveCap) FROM GRID_ANALYTICS.PRC WHERE OperatingDay BETWEEN '""" + start_date.strftime("%Y-%m-%d") + """' AND '""" + end_date.strftime("%Y-%m-%d") +"""')"""
        # OnOffReserveCap peak
        query_oo = """SELECT MAX(OnOffReserveCap) FROM GRID_ANALYTICS.PRC WHERE OperatingDay BETWEEN '""" + start_date.strftime("%Y-%m-%d") + """' AND '""" + end_date.strftime("%Y-%m-%d") +"""'"""
        # OnOffReserveCap peak date
        query_oo_date = """SELECT OperatingDay FROM GRID_ANALYTICS.PRC WHERE OnOffReserveCap = (SELECT MAX(OnOffReserveCap) FROM GRID_ANALYTICS.PRC WHERE OperatingDay BETWEEN '""" + start_date.strftime("%Y-%m-%d") + """' AND '""" + end_date.strftime("%Y-%m-%d") +"""') AND OperatingDay BETWEEN '""" + start_date.strftime("%Y-%m-%d") + """' AND '""" + end_date.strftime("%Y-%m-%d") +"""'"""

        # absolute OnlineReserveCap peak
        query_max_or = """SELECT MAX(OnlineReserveCap) FROM GRID_ANALYTICS.PRC"""
        # absolute OnlineReserveCap peak date
        query_max_or_date = """SELECT OperatingDay FROM GRID_ANALYTICS.PRC WHERE OnlineReserveCap = (SELECT MAX(OnlineReserveCap) FROM GRID_ANALYTICS.PRC)"""
        # absolute OnOffReserveCap peak
        query_max_oo = """SELECT MAX(OnOffReserveCap) FROM GRID_ANALYTICS.PRC"""
        # absolute OnOffReserveCap peak date
        query_max_oo_date = """SELECT OperatingDay FROM GRID_ANALYTICS.PRC WHERE OnOffReserveCap = (SELECT MAX(OnOffReserveCap) FROM GRID_ANALYTICS.PRC)"""

        df_peak_or = pd.read_sql_query(query_or, connection)
        df_peak_oo = pd.read_sql_query(query_oo, connection)
        df_peak_max_or = pd.read_sql_query(query_max_or, connection)
        df_peak_max_oo = pd.read_sql_query(query_max_oo, connection)

        df_peak_or_date = pd.read_sql_query(query_or_date, connection)
        df_peak_oo_date = pd.read_sql_query(query_oo_date, connection)
        df_peak_max_or_date = pd.read_sql_query(query_max_or_date, connection)
        df_peak_max_oo_date = pd.read_sql_query(query_max_oo_date, connection)

        peak_val = df_peak_or["MAX(OnlineReserveCap)"]
        peak_val.loc[1] = df_peak_oo["MAX(OnOffReserveCap)"]
        peak_val.loc[2] = df_peak_max_or["MAX(OnlineReserveCap)"]
        peak_val.loc[3] = df_peak_max_oo["MAX(OnOffReserveCap)"]

        peak_date = df_peak_or_date["OperatingDay"]
        peak_date.loc[1] = df_peak_oo_date["OperatingDay"]
        peak_date.loc[2] = df_peak_max_or_date["OperatingDay"]
        peak_date.loc[3] = df_peak_max_oo_date["OperatingDay"]


        return [or_data, oo_data], ch_labels, peak_val, peak_date


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

        # GenerationResources peak        
        query_gen = """SELECT MAX(GenerationResources) FROM GRID_ANALYTICS.PRC WHERE OperatingDay BETWEEN '""" + start_date.strftime("%Y-%m-%d") + """' AND '""" + end_date.strftime("%Y-%m-%d") +"""'"""
        # GenerationResources peak date
        query_gen_date = """SELECT OperatingDay FROM GRID_ANALYTICS.PRC WHERE GenerationResources = (SELECT MAX(GenerationResources) FROM GRID_ANALYTICS.PRC WHERE OperatingDay BETWEEN '""" + start_date.strftime("%Y-%m-%d") + """' AND '""" + end_date.strftime("%Y-%m-%d") +"""')"""
        # LoadResources peak
        query_load = """SELECT MAX(LoadResources) FROM GRID_ANALYTICS.PRC WHERE OperatingDay BETWEEN '""" + start_date.strftime("%Y-%m-%d") + """' AND '""" + end_date.strftime("%Y-%m-%d") +"""'"""
        # LoadResources peak date
        query_load_date = """SELECT OperatingDay FROM GRID_ANALYTICS.PRC WHERE LoadResources = (SELECT MAX(LoadResources) FROM GRID_ANALYTICS.PRC WHERE OperatingDay BETWEEN '""" + start_date.strftime("%Y-%m-%d") + """' AND '""" + end_date.strftime("%Y-%m-%d") +"""')"""

        # absolute generation peak
        query_max_gen = """SELECT MAX(GenerationResources) FROM GRID_ANALYTICS.PRC"""
        # absolute generation peak date
        query_max_gen_date = """SELECT OperatingDay FROM GRID_ANALYTICS.PRC WHERE GenerationResources = (SELECT MAX(GenerationResources) FROM GRID_ANALYTICS.PRC)"""
        # absolute load peak
        query_max_load = """SELECT MAX(LoadResources) FROM GRID_ANALYTICS.PRC"""
        # absolute load peak date
        query_max_load_date = """SELECT OperatingDay FROM GRID_ANALYTICS.PRC WHERE LoadResources = (SELECT MAX(LoadResources) FROM GRID_ANALYTICS.PRC)"""

        df_peak_gen = pd.read_sql_query(query_gen, connection)
        df_peak_load = pd.read_sql_query(query_load, connection)
        df_peak_max_gen = pd.read_sql_query(query_max_gen, connection)
        df_peak_max_load = pd.read_sql_query(query_max_load, connection)

        df_peak_gen_date = pd.read_sql_query(query_gen_date, connection)
        df_peak_load_date = pd.read_sql_query(query_load_date, connection)
        df_peak_max_gen_date = pd.read_sql_query(query_max_gen_date, connection)
        df_peak_max_load_date = pd.read_sql_query(query_max_load_date, connection)

        peak_val = df_peak_gen["MAX(GenerationResources)"]
        peak_val.loc[1] = df_peak_load["MAX(LoadResources)"]
        peak_val.loc[2] = df_peak_max_gen["MAX(GenerationResources)"]
        peak_val.loc[3] = df_peak_max_load["MAX(LoadResources)"]

        peak_date = df_peak_gen_date["OperatingDay"]
        peak_date.loc[1] = df_peak_load_date["OperatingDay"]
        peak_date.loc[2] = df_peak_max_gen_date["OperatingDay"]
        peak_date.loc[3] = df_peak_max_load_date["OperatingDay"]

        return [gr_data, lr_data], ch_labels, peak_val, peak_date



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

        #query to select peak value and date of entry with highest PhysicalResponsiveCapability in the selected time period        
        query_val = """SELECT PhysicalResponsiveCapability FROM GRID_ANALYTICS.PRC WHERE PhysicalResponsiveCapability = (SELECT MAX(PhysicalResponsiveCapability) FROM GRID_ANALYTICS.PRC WHERE OperatingDay BETWEEN '""" + start_date.strftime("%Y-%m-%d") + """' AND '""" + end_date.strftime("%Y-%m-%d") +"""')"""
        query_date = """SELECT OperatingDay FROM GRID_ANALYTICS.PRC WHERE PhysicalResponsiveCapability = (SELECT MAX(PhysicalResponsiveCapability) FROM GRID_ANALYTICS.PRC WHERE OperatingDay BETWEEN '""" + start_date.strftime("%Y-%m-%d") + """' AND '""" + end_date.strftime("%Y-%m-%d") +"""')"""

        df_val = pd.read_sql_query(text(query_val), connection)
        df_date = pd.read_sql_query(text(query_date), connection)

        #query to select peak value and date of entry with highest PhysicalResponsiveCapability of all time
        query_max_val = """SELECT PhysicalResponsiveCapability FROM GRID_ANALYTICS.PRC WHERE PhysicalResponsiveCapability = (SELECT MAX(PhysicalResponsiveCapability) FROM GRID_ANALYTICS.PRC)"""
        query_max_date = """SELECT OperatingDay FROM GRID_ANALYTICS.PRC WHERE PhysicalResponsiveCapability = (SELECT MAX(PhysicalResponsiveCapability) FROM GRID_ANALYTICS.PRC)"""

        df_max_val = pd.read_sql_query(text(query_max_val), connection)
        df_max_date = pd.read_sql_query(text(query_max_date), connection)

        peak_val = [df_val.iloc[-1].get('PhysicalResponsiveCapability')]
        peak_date = [df_date.iloc[-1].get('OperatingDay')]

        max_val = df_max_val.iloc[-1].get('PhysicalResponsiveCapability')
        max_date = df_max_date.iloc[-1].get('OperatingDay')

        peak_val.append(max_val)
        peak_date.append(max_date)

        return ch_data, ch_labels, peak_val, peak_date


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
        
        #select the entry with the highest coal value of all time
        query_coal = """SELECT * FROM GRID_ANALYTICS.GBFT WHERE Coal = (SELECT MAX(Coal) FROM GRID_ANALYTICS.GBFT)"""
        df_peak_coal = pd.read_sql_query(query_coal, connection)
        peak_val_coal = df_peak_coal["Coal"].tolist()[0]
        peak_date_coal = df_peak_coal["OperatingDay"].tolist()[0]
        
        #select the entry with the highest gas value of all time
        query_gas = """SELECT * FROM GRID_ANALYTICS.GBFT WHERE Gas = (SELECT MAX(Gas) FROM GRID_ANALYTICS.GBFT)"""
        df_peak_gas = pd.read_sql_query(query_gas, connection)
        peak_val_gas = df_peak_gas["Gas"].tolist()[0]
        peak_date_gas = df_peak_gas["OperatingDay"].tolist()[0]

        #select the entry with the highest gas-cc value of all time
        query_gas_cc = """SELECT * FROM GRID_ANALYTICS.GBFT WHERE "Gas-CC" = (SELECT MAX("Gas-CC") FROM GRID_ANALYTICS.GBFT)"""
        df_peak_gas_cc = pd.read_sql_query(query_gas_cc, connection)
        peak_val_gas_cc = df_peak_gas_cc["Gas-CC"].tolist()[0]
        peak_date_gas_cc = df_peak_gas_cc["OperatingDay"].tolist()[0]

        #select the entry with the highest hydro value of all time
        query_hydro = """SELECT * FROM GRID_ANALYTICS.GBFT WHERE Hydro = (SELECT MAX(Hydro) FROM GRID_ANALYTICS.GBFT)"""
        df_peak_hydro = pd.read_sql_query(query_hydro, connection)
        peak_val_hydro = df_peak_hydro["Hydro"].tolist()[0]
        peak_date_hydro = df_peak_hydro["OperatingDay"].tolist()[0]

        #select the entry with the highest nuclear value of all time
        query_nuclear = """SELECT * FROM GRID_ANALYTICS.GBFT WHERE Nuclear = (SELECT MAX(Nuclear) FROM GRID_ANALYTICS.GBFT)"""
        df_peak_nuclear = pd.read_sql_query(query_nuclear, connection)
        peak_val_nuclear = df_peak_nuclear["Nuclear"].tolist()[0]
        peak_date_nuclear = df_peak_nuclear["OperatingDay"].tolist()[0]

        #select the entry with the highest other value of all time
        query_other = """SELECT * FROM GRID_ANALYTICS.GBFT WHERE Other = (SELECT MAX(Other) FROM GRID_ANALYTICS.GBFT)"""
        df_peak_other = pd.read_sql_query(query_other, connection)
        peak_val_other = df_peak_other["Other"].tolist()[0]
        peak_date_other = df_peak_other["OperatingDay"].tolist()[0]

        #select the entry with the highest solar value of all time
        query_solar = """SELECT * FROM GRID_ANALYTICS.GBFT WHERE Solar = (SELECT MAX(Solar) FROM GRID_ANALYTICS.GBFT)"""
        df_peak_solar = pd.read_sql_query(query_solar, connection)
        peak_val_solar = df_peak_solar["Solar"].tolist()[0]
        peak_date_solar = df_peak_solar["OperatingDay"].tolist()[0]

        #select the entry with the highest wind value of all time
        query_wind = """SELECT * FROM GRID_ANALYTICS.GBFT WHERE Wind = (SELECT MAX(Wind) FROM GRID_ANALYTICS.GBFT)"""
        df_peak_wind = pd.read_sql_query(query_wind, connection)
        peak_val_wind = df_peak_wind["Wind"].tolist()[0]
        peak_date_wind = df_peak_wind["OperatingDay"].tolist()[0]

        #select the entry with the highest biomass value of all time
        query_biomass = """SELECT * FROM GRID_ANALYTICS.GBFT WHERE Biomass = (SELECT MAX(Biomass) FROM GRID_ANALYTICS.GBFT)"""
        df_peak_biomass = pd.read_sql_query(query_biomass, connection)
        peak_val_biomass = df_peak_biomass["Biomass"].tolist()[0]
        peak_date_biomass = df_peak_biomass["OperatingDay"].tolist()[0]

        #find the all time highest value across all categories
        peak_val_list = [peak_val_biomass, peak_val_coal, peak_val_gas, peak_val_gas_cc, 
                         peak_val_hydro, peak_val_nuclear, peak_val_other, peak_val_solar, peak_val_wind]
        peak_date_list = [peak_date_biomass, peak_date_coal, peak_date_gas, peak_date_gas_cc,
                            peak_date_hydro, peak_date_nuclear, peak_date_other, peak_date_solar, peak_date_wind]
        peak_val_all_time = max(peak_val_list)
        peak_date_all_time = peak_date_list[peak_val_list.index(peak_val_all_time)]

        #python list of all categories
        categories = ["Biomass", "Coal", "Gas", "Gas-CC", "Hydro", "Nuclear", "Other", "Solar", "Wind"]

        #df_peak_biomass = df_peak.sort_values(by=['Biomass'], ascending=False).head(1)
        #peak_val_biomass = df_peak_biomass["Biomass"]

        #use for loop to find max value and date for each category in the date range
        peak_val_list = []
        peak_date_list = []
        for category in categories:
            df_peak_category = df_peak.sort_values(by=[category], ascending=False).head(1)
            peak_val_list.append(df_peak_category[category].tolist()[0])
            peak_date_list.append(df_peak_category["OperatingDay"].tolist()[0])

        #select the overall max value from the list of max values
        peak_val = [max(peak_val_list)]
        #select the date of the overall max value from the list of max dates
        peak_date = [peak_date_list[peak_val_list.index(peak_val[0])]]

        #append the all time max value and date to the list of max values and dates
        peak_val.append(peak_val_all_time)
        peak_date.append(peak_date_all_time)
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