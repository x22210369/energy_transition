import json
from pymongo import MongoClient
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, exc, text
import pandas.io.sql as sqlio
from dagster import asset

@asset(
  deps=["electricity_installed_json_to_mongodb"]
)
def electricity_installed_capacity_Extract_transform_load():

    source_mongodb_uri = 'mongodb+srv://mongo:mongo@dapprojcluster.8o36trt.mongodb.net/'  
    source_database_name = 'Electricity'      
    source_collection_name = 'Electricity_Installed_Capacity'
    
    client_source = MongoClient(source_mongodb_uri)
    source_database = client_source[source_database_name]
    source_collection = source_database[source_collection_name]

    cursor = source_collection.find()
    data = list(cursor)

    electricity_installed_capacity_df = pd.DataFrame(data)

    columns_to_drop = ['IS02', 'IS03', 'Indicator', 'Unit', 'Source', 'CTS_code', 'CTS_name', 'CTS_Full_Descriptor']
    electricity_installed_capacity_df = electricity_installed_capacity_df.drop(columns_to_drop, axis=1, errors='ignore')

    columns_to_transform = [
        '2000', '2001', '2002',
        '2003', '2004', '2005',
        '2006', '2007', '2008',
        '2009', '2010', '2011',
        '2012', '2013', '2014',
        '2015', '2016', '2017',
        '2018', '2019', '2020',
        '2021'
    ]

    columns_to_transform_existing = [col for col in columns_to_transform if col in electricity_installed_capacity_df.columns]

    electricity_installed_capacity_df[columns_to_transform_existing] = electricity_installed_capacity_df[columns_to_transform_existing].apply(pd.to_numeric, errors='coerce')

    electricity_installed_capacity_df[columns_to_transform_existing] = electricity_installed_capacity_df[columns_to_transform_existing].replace({None: np.nan})
    electricity_installed_capacity_df[columns_to_transform_existing] = electricity_installed_capacity_df[columns_to_transform_existing].fillna(0)

    print(electricity_installed_capacity_df)

    connection_string = "postgresql+psycopg2://postgres:dap@localhost:5433/energy"

    # Table Creation String
    table_create_string = """
        CREATE TABLE IF NOT EXISTS Electricity_Installed_Capacity (
            ObjectId2 SERIAL PRIMARY KEY,
            Country VARCHAR(255),
            Technology VARCHAR(255),
            Energy_Type VARCHAR(255),
            "2000" FLOAT,
            "2001" FLOAT,
            "2002" FLOAT,
            "2003" FLOAT,
            "2004" FLOAT,
            "2005" FLOAT,
            "2006" FLOAT,
            "2007" FLOAT,
            "2008" FLOAT,
            "2009" FLOAT,
            "2010" FLOAT,
            "2011" FLOAT,
            "2012" FLOAT,
            "2013" FLOAT,
            "2014" FLOAT,
            "2015" FLOAT,
            "2016" FLOAT,
            "2017" FLOAT,
            "2018" FLOAT,
            "2019" FLOAT,
            "2020" FLOAT,
            "2021" FLOAT,
            "2022" FLOAT
        );
    """

    try:
        engine = create_engine(connection_string)
        with engine.connect() as connection:
            connection.execution_options(isolation_level="AUTOCOMMIT")
            connection.execute(text(table_create_string))
            print("Table successfully created")
    except exc.SQLAlchemyError as dbError:
        print("Postgres Error", dbError)
    finally:
        if 'engine' in locals():
            engine.dispose()

    # Loading data into the 'Electricity_Installed_Capacity' table in PostgreSQL
    electricity_installed_capacity_df.drop(columns=['_id']).to_sql('Electricity_Installed_Capacity', engine, if_exists='replace', index=False)
    print("Data successfully loaded into PostgreSQL")

