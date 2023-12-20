import pandas as pd
import matplotlib.pyplot as plt
from sqlalchemy import create_engine, exc, text
import psycopg2
from psycopg2 import sql
import pandas.io.sql as sqlio
import csv
from dagster import asset

db_params = {
    "host": "localhost",
    "database": "energy",
    "user": "postgres",
    "password": "dap",
    "port": 5433
}
@asset(
        deps=["annual_GHG_air_emission_to_postgresql"]
)
def greenhouse_gas_emission_extract_transform_load():

    connection = psycopg2.connect(**db_params)
    cursor = connection.cursor()

    table_name = 'ghc'

    query = sql.SQL("SELECT * FROM {}").format(sql.Identifier(table_name))

    cursor.execute(query)

    greenhouse_gas_emission_data = cursor.fetchall()

    greenhouse_gas_emission_data_df = pd.DataFrame(greenhouse_gas_emission_data)

    print("Original DataFrame columns:")
    print(greenhouse_gas_emission_data_df.columns)

    greenhouse_gas_emission_data_df.columns = ["Country", "IS02", "IS03", "Indicator", "Unit", "Industry", "Gas_Type",
                                                "Source", "CTS_code", "CTS_name", "CTS_Full_Descriptor", "Seasonal_Adjustment",
                                                "2010", "2011", "2012", "2013", "2014", "2015", "2016", "2017", "2018",
                                                "2019", "2020", "2021"]
    print("\nModified DataFrame columns:")
    print(greenhouse_gas_emission_data_df.columns)

    columns_to_drop = ['IS02', 'IS03', 'Indicator', 'Unit', 'Source', 'CTS_code', 'CTS_name', 'CTS_Full_Descriptor', 'Seasonal_Adjustment']

    greenhouse_gas_emission_data_df.drop(columns=columns_to_drop, axis=1, inplace=True)

    columns_to_transform = [str(year) for year in range(2010, 2022)]  # Update the range based on actual column names

    greenhouse_gas_emission_data_df[columns_to_transform] = greenhouse_gas_emission_data_df[columns_to_transform].apply(pd.to_numeric, errors='coerce')

    greenhouse_gas_emission_data_df.fillna(0, inplace=True)

    print("\nDataFrame after transformation:")
    print(greenhouse_gas_emission_data_df)

    connection_string = "postgresql+psycopg2://postgres:dap@localhost:5433/energy"

    table_name1 = "Greenhouse_gas_emissions"

    table_create_string = """
        CREATE TABLE IF NOT EXISTS {table_name1} (
            ObjectId2 SERIAL PRIMARY KEY,
            Country VARCHAR(255),
            Industry VARCHAR(255),
            Gas_Type VARCHAR(255),
            Scale VARCHAR(255),
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
            "2021" FLOAT
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

    # Loading data into the 'Electricity_Generation' table in PostgreSQL
    greenhouse_gas_emission_data_df.to_sql('Greenhouse_gas_emissions', engine, if_exists='replace', index=False)
    print("Data successfully loaded into PostgreSQL")

