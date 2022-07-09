#!/Users/danielschneider/opt/anaconda3/bin/python

from operator import index
from colorama import init
import requests
import pandas as pd
import csv, os
from datetime import datetime
import sqlalchemy
import psycopg2
from configparser import ConfigParser

parser = ConfigParser()
parser.read('caiso.config')
DB_PATH = parser['db_info']['DB_PATH']

#General CAISO stats
URL = "https://www.caiso.com/outlook/SP/stats.txt?_=1657108858287"
#Net demand 
ND_URL = "https://www.caiso.com/outlook/SP/netdemand.csv?_=1657109785458"
#Net fuel info
NF_URL = "https://www.caiso.com/outlook/SP/fuelsource.csv?_=1657110047583"

def request_and_transform(URL : str) -> list:

    #Get HTTP response from CAISO
    data = requests.get(URL)

    #Convert to UTF-8
    decoded_data = data.content.decode('utf-8')

    #Convert text to "2D array"
    array = list(csv.reader(decoded_data.splitlines(), delimiter=','))

    return array

#Define headers using the first list in array
demand_list = request_and_transform(ND_URL)
fuel_list = request_and_transform(NF_URL)

#Populate rows of df's
def create_df(array : list, headers : list) -> pd.DataFrame:
    #init dataframes with respective headers
    init_df = pd.DataFrame(columns=headers)
    for row in range(1,len(array)):
        init_df.loc[row] = array[row]

    return init_df

demand = create_df(demand_list, demand_list[0])
fuel = create_df(fuel_list, fuel_list[0])

#Creates combined dataset and converts to csv
def log_data(df1 : pd.DataFrame, df2 : pd.DataFrame) -> None:
    agg = df1.merge(df2, how='left', on='Time')
    agg.insert(0, 'date_in', datetime.today())

    agg.columns = [
        "date_in",
        "time_in",
        "hour_forecast",
        "current_demand",
        "net_demand",
        "net_demand_forecast",
        "demand_response",
        "solar",
        "wind",
        "geothermal",
        "biomass",
        "biogas",
        "small_hydro",
        "coal",
        "nuclear",
        "natural_gas",
        "large_hydro",
        "batteries",
        "imports",
        "other"]

    agg.fillna(0, inplace=True)
    agg.replace("", 0, inplace=True)
    path = "/Users/danielschneider/DataEng/caiso/data"
    fpath = os.path.join(path, "stats-{}.csv".format(datetime.today().strftime('%Y-%m-%d')))
    agg.to_csv(fpath)

    return agg

final = log_data(demand, fuel)
engine = sqlalchemy.create_engine(DB_PATH)
try:
    final.to_sql('demand', con=engine, if_exists='append', index=False)
except (Exception, psycopg2.DatabaseError) as error:
    print(error)
finally:
    print('Connection to db closed')

message = "Successfully ran at {}".format(datetime.now())
print(message)





