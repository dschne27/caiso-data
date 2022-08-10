#!/Users/danielschneider/opt/anaconda3/bin/python

import requests
import pandas as pd
import csv
import os
from datetime import date, datetime, timedelta
import sqlalchemy
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert
import psycopg2
from configparser import ConfigParser
import sys
import uuid

parser = ConfigParser()
parser.read('caiso.config')
DB_PATH = parser['db_info']['DB_PATH']

#General CAISO stats
URL = "https://www.caiso.com/outlook/SP/stats.txt?_=1657108858287"
#Net demand 
ND_URL = "https://www.caiso.com/outlook/SP/netdemand.csv?_=1657109785458"
ND_HIST = 'https://www.caiso.com/outlook/SP/History/20220710/netdemand.csv?_=1657540401662'

#Net fuel info
NF_URL = "https://www.caiso.com/outlook/SP/fuelsource.csv?_=1657110047583"
NF_HIST = "https://www.caiso.com/outlook/SP/History/20220710/fuelsource.csv?_=1657540862724"

#SQL format for dataframe 
dtypes = {
    "date_in" : sqlalchemy.DateTime(),
    "time_in" : sqlalchemy.Time(),
    "hour_forecast" : sqlalchemy.types.INTEGER(),
    "current_demand" : sqlalchemy.types.INTEGER(),
    "net_demand" : sqlalchemy.types.INTEGER(),
    "net_demand_forecast" : sqlalchemy.types.INTEGER(),
    "demand_response" : sqlalchemy.types.INTEGER(),
    "solar" : sqlalchemy.types.INTEGER(),
    "wind" : sqlalchemy.types.INTEGER(),
    "geothermal" : sqlalchemy.types.INTEGER(),
    "biomass" : sqlalchemy.types.INTEGER(),
    "biogas" : sqlalchemy.types.INTEGER(),
    "small_hydro" : sqlalchemy.types.INTEGER(),
    "coal" : sqlalchemy.types.INTEGER(),
    "nuclear" : sqlalchemy.types.INTEGER(),
    "natural_gas" : sqlalchemy.types.INTEGER(),
    "large_hydro" : sqlalchemy.types.INTEGER(),
    "batteries" : sqlalchemy.types.INTEGER(),
    "imports" : sqlalchemy.types.INTEGER(),
    "other" : sqlalchemy.types.INTEGER()
}

def run():

    #get the target date to send in request
    flag = int(sys.argv[1])
    today_str = datetime.strftime(datetime.today(), "%Y%m%d")
    today = datetime.strptime(today_str, "%Y%m%d")
    target_date = today - timedelta(days=flag)
    query_date = target_date.strftime("%Y%m%d")

    ND_URL = ""
    NF_URL = ""
    if flag == 0 or len(sys.argv) == 1:
        ND_URL = "https://www.caiso.com/outlook/SP/netdemand.csv?_=1657109785458" 
        NF_URL = "https://www.caiso.com/outlook/SP/fuelsource.csv?_=1657110047583"

    else:
        NF_URL = "https://www.caiso.com/outlook/SP/History/{}/fuelsource.csv?_=1657540862724".format(query_date)
        ND_URL = "https://www.caiso.com/outlook/SP/History/{}/netdemand.csv?_=1657540401662".format(query_date)
    
    
    #Define headers using the first list in array
    demand_list = request_and_transform(ND_URL)
    fuel_list = request_and_transform(NF_URL)

    demand = create_df(demand_list, demand_list[0])
    fuel = create_df(fuel_list, fuel_list[0])

    final = log_data(demand, fuel, target_date)
    engine = sqlalchemy.create_engine(DB_PATH)

    #Insert new rows of data
    insert_query = text(f""" INSERT INTO public.demand VALUES 
                    {','.join([str(i) for i in list(final.to_records(index=False))])} """)
    engine.connect().execute(insert_query)




    message = "Successfully ran at {}".format(datetime.now())
    print(message)


# https://gist.github.com/pedrovgp/b46773a1240165bf2b1448b3f70bed32
def upsert_df(df: pd.DataFrame, table_name: str, engine: sqlalchemy.engine.Engine):
    """Implements the equivalent of pd.DataFrame.to_sql(..., if_exists='update')
    (which does not exist). Creates or updates the db records based on the
    dataframe records.
    Conflicts to determine update are based on the dataframes index.
    This will set unique keys constraint on the table equal to the index names
    1. Create a temp table from the dataframe
    2. Insert/update from temp table into table_name
    Returns: True if successful
    """

    # If the table does not exist, we should just use to_sql to create it
    if not engine.execute(
        f"""SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE  table_schema = 'public'
            AND    table_name   = '{table_name}');
            """
    ).first()[0]:
        df.to_sql(table_name, engine)
        return True

    # If it already exists...
    temp_table_name = f"temp_{uuid.uuid4().hex[:6]}"
    df.to_sql(temp_table_name, engine, index=True)

    index = list(df.index.names)
    index_sql_txt = ", ".join([f'"{i}"' for i in index])
    columns = list(df.columns)
    headers = index + columns
    headers_sql_txt = ", ".join(
        [f'"{i}"' for i in headers]
    )  # index1, index2, ..., column 1, col2, ...

    # col1 = exluded.col1, col2=excluded.col2
    update_column_stmt = ", ".join([f'"{col}" = EXCLUDED."{col}"' for col in columns])

    # For the ON CONFLICT clause, postgres requires that the columns have unique constraint
    query_pk = f"""
    ALTER TABLE "{table_name}" DROP CONSTRAINT IF EXISTS unique_constraint_for_upsert_{table_name};
    ALTER TABLE "{table_name}" ADD CONSTRAINT unique_constraint_for_upsert_{table_name} UNIQUE ({index_sql_txt});
    """
    engine.execute(query_pk)

    # Compose and execute upsert query
    query_upsert = f"""
    INSERT INTO "{table_name}" ({headers_sql_txt}) 
    SELECT {headers_sql_txt} FROM "{temp_table_name}"
    ON CONFLICT ({index_sql_txt}) DO UPDATE 
    SET {update_column_stmt};
    """
    engine.execute(query_upsert)
    engine.execute(f"DROP TABLE {temp_table_name}")

    return True



def request_and_transform(URL : str) -> list:

    #Get HTTP response from CAISO
    data = requests.get(URL)

    #Convert to UTF-8
    decoded_data = data.content.decode('utf-8')

    #Convert text to "2D array"
    array = list(csv.reader(decoded_data.splitlines(), delimiter=','))

    return array

#Populate rows of df's
def create_df(array : list, headers : list) -> pd.DataFrame:
    #init dataframes with respective headers
    init_df = pd.DataFrame(columns=headers)
    for row in range(1,len(array)):
        init_df.loc[row] = array[row]

    return init_df



#Creates combined dataframe, converts it to a csv, and returns final dataframe
def log_data(df1 : pd.DataFrame, df2 : pd.DataFrame, target_date : datetime) -> pd.DataFrame:
    agg = df1.merge(df2, how='left', on='Time')
    agg.insert(0, 'date_in', target_date)

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

    # drop second midnight entry (last row of df) which ruins values
    agg.drop([288], axis=0, inplace=True)
    path = "/Users/danielschneider/DataEng/caiso/data"
    # ec2_path = "/home/ec2-user/data/caiso/data"
    fpath = os.path.join(path, "stats-{}.csv".format(target_date.strftime('%Y-%m-%d')))
    agg.to_csv(fpath)

    return agg

run()





