import json
from dagster import asset
from pymongo import MongoClient
from . import constants
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, exc, text
import pandas.io.sql as sqlio
import csv

@asset(
  deps=["electricity_generation_json_to_mongodb"]
)
def electricity_generation_extract_and_transformation_load():
    # Connect to MongoDB
    mongodb_uri = 'mongodb+srv://mongo:mongo@dapprojcluster.8o36trt.mongodb.net/'  
    database_name = 'Electricity'      
    collection_name = 'Electricity_Generation'
    
    client = MongoClient(mongodb_uri)
    database = client[database_name]
    collection = database[collection_name]

    # Fetch data from MongoDB
    cursor = collection.find()
    data = list(cursor)

    # Convert data to a DataFrame
    electricity_generation_df = pd.DataFrame(data)
    columns_to_drop = ['IS02', 'IS03', 'Indicator', 'Unit', 'Source', 'CTS_code', 'CTS_name', 'CTS_Full_Descriptor']
    
    # Drop unwanted columns
    electricity_generation_df.drop(columns=columns_to_drop, axis=1, inplace=True)

    # Specify columns for conversion and replacement
    columns_to_transform = [str(year) for year in range(2000, 2023)]

    # Convert specified columns to numeric
    electricity_generation_df[columns_to_transform] = electricity_generation_df[columns_to_transform].apply(pd.to_numeric, errors='coerce')

    # Replace None with NaN and then NaN with 0
    electricity_generation_df.fillna(0, inplace=True)

    print(electricity_generation_df)

    # PostgreSQL Database Connection
    connection_string = "postgresql+psycopg2://postgres:dap@localhost:5433/energy"

    # Table Creation String
    table_create_string = """
        CREATE TABLE IF NOT EXISTS Electricity_Generation (
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

    # Load data into the 'Electricity_Generation' table in PostgreSQL
    electricity_generation_df.drop(columns=['_id']).to_sql('Electricity_Generation', engine, if_exists='replace', index=False)
    print("Data successfully loaded into PostgreSQL")

