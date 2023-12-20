import pandas as pd
import matplotlib.pyplot as plt
import psycopg2
from psycopg2 import sql
from dagster import asset
import seaborn as sns
import plotly.express as px

db_params = {
    "host": "localhost",
    "database": "energy",
    "user": "postgres",
    "password": "dap",
    "port": 5433
}
#---------------------------------------------------------------------------------------------------------------------------------
#-----------------------------------Total Greenhouse Gas Emissions Over Years by Gas Type-----------------------------------------

@asset(
        deps=["greenhouse_gas_emission_extract_transform_load"]
)
def total_Greenhouse_Gas_Emissions_by_gas_type():
    # Connect to the PostgreSQL database
    try:
        connection = psycopg2.connect(**db_params)
        cursor = connection.cursor()

        # Replace 'your_table' with the actual table name
        table_name = 'Greenhouse_gas_emissions'

        # Build the SQL query to fetch data
        query = sql.SQL("SELECT * FROM {}").format(sql.Identifier(table_name))

        # Execute the query
        cursor.execute(query)

        # Fetch all the rows
        greenhouse_gas_data = cursor.fetchall()

        greenhouse_gas_df = pd.DataFrame(greenhouse_gas_data)

        greenhouse_gas_df.columns = ["Country", "Industry", "Gas_Type", "2010", "2011", "2012", "2013", "2014", "2015", 
                                     "2016", "2017", "2018", "2019", "2020", "2021"]

        # Extract years
        years = [str(year) for year in range(2010, 2022)]

        # Convert data types and fill missing values
        greenhouse_gas_df[years] = greenhouse_gas_df[years].apply(pd.to_numeric, errors='coerce').fillna(0)

        # Group by gas type and sum the values for each year
        grouped_df = greenhouse_gas_df.groupby('Gas_Type')[years].sum().transpose()

        # Plot the graph
        plt.figure(figsize=(12, 6))
        for gas_type in grouped_df.columns:
            plt.plot(grouped_df.index, grouped_df[gas_type], marker='o', label=gas_type)

        plt.xlabel('Year')
        plt.ylabel('Total Greenhouse Gas Emissions')
        plt.title('Total Greenhouse Gas Emissions Over Years by Gas Type')
        plt.legend()
        plt.grid(True)
        # Save the plot to a file
        plt.savefig("data\outputs\Total_Greenhouse_Gas_Emissions.png")
        # Show the plot (if needed)
        plt.show()

    except Exception as e:
        print(f"Error: {e}")

    finally:
        # Close the cursor and connection
        if cursor:
            cursor.close()
        if connection:
            connection.close()

#----------------------------------------------------------------------------------------------------------------------------------
#-------------------------------------------------Total Greenhouse Gas Emissions by Industry------------------------------------
@asset(
        deps=["greenhouse_gas_emission_extract_transform_load"]
)          
def Identify_Significant_Industries():
    # Connect to the PostgreSQL database
    try:
        connection = psycopg2.connect(**db_params)
        cursor = connection.cursor()

        # Replace 'your_table' with the actual table name
        table_name = 'Greenhouse_gas_emissions'

        # Build the SQL query to fetch data
        query = sql.SQL("SELECT * FROM {}").format(sql.Identifier(table_name))

        # Execute the query
        cursor.execute(query)

        # Fetch all the rows
        greenhouse_gas_data = cursor.fetchall()

        greenhouse_gas_df = pd.DataFrame(greenhouse_gas_data)

        greenhouse_gas_df.columns = ["Country", "Industry", "Gas_Type", "2010", "2011", "2012", "2013", "2014", "2015", 
                                     "2016", "2017", "2018", "2019", "2020", "2021"]

        # Extract years
        years = [str(year) for year in range(2010, 2022)]

        # Convert data types and fill missing values
        greenhouse_gas_df[years] = greenhouse_gas_df[years].apply(pd.to_numeric, errors='coerce').fillna(0)

        # Group by industry and sum the values for each year
        total_emissions_by_industry = greenhouse_gas_df.groupby('Industry')[years].sum()

        # Calculate the total emissions for each industry across all years
        total_emissions_by_industry['Total_Emissions'] = total_emissions_by_industry.sum(axis=1)

        # Sort industries by total emissions in descending order
        sorted_industries = total_emissions_by_industry.sort_values(by='Total_Emissions', ascending=False)

        # Define a color palette using seaborn
        colors = sns.color_palette("viridis", len(sorted_industries))

        # Plot the results horizontally with different colors for each industry
        plt.figure(figsize=(14, 5))
        plt.barh(sorted_industries.index, sorted_industries['Total_Emissions'], color=colors)
        plt.ylabel('Industry', fontsize=14)
        plt.xlabel('Total Greenhouse Gas Emissions', fontsize=14)
        plt.title('Total Greenhouse Gas Emissions by Industry', fontsize=16)
        plt.tight_layout()  # Adjust layout to prevent cutting off labels
        # Save the plot to a file
        plt.savefig("data\outputs\Total_Greenhouse_Gas_Emissions_by_Industry.png")
        plt.show()

    except Exception as e:
        print(f"Error: {e}")

    finally:
        # Close the cursor and connection
        if cursor:
            cursor.close()
        if connection:
            connection.close()

#-----------------------------------------------------------------------------------------------------------------------------------
#-------------------------------------------------------------

@asset(
        deps=["greenhouse_gas_emission_extract_transform_load"]
) 
def total_Greenhouse_Gas_Emissions_by_gas_type_funnel():
    # Connect to the PostgreSQL database
    try:
        connection = psycopg2.connect(**db_params)
        cursor = connection.cursor()

        # Replace 'your_table' with the actual table name
        table_name = 'Greenhouse_gas_emissions'

        # Build the SQL query to fetch data
        query = sql.SQL("SELECT * FROM {}").format(sql.Identifier(table_name))

        # Execute the query
        cursor.execute(query)

        # Fetch all the rows
        greenhouse_gas_data = cursor.fetchall()

        greenhouse_gas_df = pd.DataFrame(greenhouse_gas_data)

        greenhouse_gas_df.columns = ["Country", "Industry", "Gas_Type", "2010", "2011", "2012", "2013", "2014", "2015", 
                                     "2016", "2017", "2018", "2019", "2020", "2021"]

        # Extract years
        years = [str(year) for year in range(2010, 2022)]

        # Convert data types and fill missing values
        greenhouse_gas_df[years] = greenhouse_gas_df[years].apply(pd.to_numeric, errors='coerce').fillna(0)

        # Group by gas type and sum the values for each year
        total_emissions_by_gas_type = greenhouse_gas_df.groupby('Gas_Type')[years].sum().sum(axis=1)

        # Calculate the percentage of total emissions for each gas type
        total_percentage = (total_emissions_by_gas_type / total_emissions_by_gas_type.sum()) * 100

        # Create a DataFrame for plotting
        df_plot = pd.DataFrame({
            'Gas_Type': total_emissions_by_gas_type.index,
            'Total_Emissions': total_emissions_by_gas_type.values,
            'Percentage': total_percentage.values
        })

        # Sort the DataFrame by total emissions
        df_plot = df_plot.sort_values(by='Total_Emissions', ascending=False)

        # Create a funnel chart using plotly
        fig = px.funnel(df_plot, x='Total_Emissions', y='Gas_Type', title='Total Greenhouse Gas Emissions by Gas Type',
                        labels={'Gas_Type': 'Gas Type', 'Total_Emissions': 'Total Emissions'},
                        hover_data=['Percentage'])
        fig.show()

    except Exception as e:
        print(f"Error: {e}")

    finally:
        # Close the cursor and connection
        if cursor:
            cursor.close()
        if connection:
            connection.close()