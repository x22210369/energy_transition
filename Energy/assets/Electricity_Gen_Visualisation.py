import psycopg2
from psycopg2 import sql
import pandas as pd
import matplotlib.pyplot as plt
from dagster import asset

# Replace these with your actual database credentials
db_params = {
    "host": "localhost",
    "database": "energy",
    "user": "postgres",
    "password": "dap",
    "port": 5433
}

#-------------------------------------------------------------------------------------------------------------------------------------

#------------------------------------------Total_Electriticty_Generation_By_Technology-------------------------------------------------
@asset(
        deps=["electricity_generation_extract_and_transformation_load"]
)
def Total_Electricity_Generation_By_Technology():
    # Connect to the PostgreSQL database
    try:
        connection = psycopg2.connect(**db_params)
        cursor = connection.cursor()

        # Replace 'your_table' with the actual table name
        table_name = 'Electricity_Generation'

        # Build the SQL query to fetch data
        query = sql.SQL("SELECT * FROM {}").format(sql.Identifier(table_name))

        # Execute the query
        cursor.execute(query)

        # Fetch all the rows
        Electricity_gen_data = cursor.fetchall()

        electricity_gen_data_df = pd.DataFrame(Electricity_gen_data)
        #print(electricity_gen_data_df)

        electricity_gen_data_df.columns =["Country","Technology","Energy_Type", "2000","2001","2002","2003","2004","2005","2006","2007",
                                          "2008","2009","2010","2011","2012","2013","2014","2015", "2016", "2017","2018","2019","2020","2021",
                                          "2022"]

        # Extract years
        years = [str(year) for year in range(2000, 2022)]

        # Convert data types and fill missing values
        electricity_gen_data_df[years] = electricity_gen_data_df[years].apply(pd.to_numeric, errors='coerce').fillna(0)

        # Group by technology and sum the values for each year
        grouped_df = electricity_gen_data_df.groupby('Technology')[years].sum().transpose()

        # Plot the graph
        plt.figure(figsize=(12, 6))
        for technology in grouped_df.columns:
            plt.plot(grouped_df.index, grouped_df[technology], marker='o', label=technology)

        plt.xlabel('Year')
        plt.ylabel('Total Electricity Generation (GWh)')
        plt.title('Total Electricity Generation Over Years for Different Technologies')
        plt.legend()
        plt.grid(True)
        # Save the plot to a file
        plt.savefig("data\outputs\Total_Electricity_Generation_By_Technology.png")
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

#-------------------------------------------------------------------------------------------------------------------------------------
#---------------------------------Cumulative_Percent_Share_Technologies by _Total_Electricity_Generation------------------------------
@asset(
        deps=["electricity_generation_extract_and_transformation_load"]
)  
def Cumulative_Prcnt_Share_Technologies_Total_Elect_Generation():
    # Connect to the PostgreSQL database
    try:
        connection = psycopg2.connect(**db_params)
        cursor = connection.cursor()

        # Replace 'your_table' with the actual table name
        table_name = 'Electricity_Generation'

        # Build the SQL query to fetch data
        query = sql.SQL("SELECT * FROM {}").format(sql.Identifier(table_name))

        # Execute the query
        cursor.execute(query)

        # Fetch all the rows
        Electricity_gen_data = cursor.fetchall()

        electricity_gen_data_df = pd.DataFrame(Electricity_gen_data)
        #print(electricity_gen_data_df)

        electricity_gen_data_df.columns =["Country","Technology","Energy_Type", "2000","2001","2002","2003","2004","2005","2006","2007",
                                          "2008","2009","2010","2011","2012","2013","2014","2015", "2016", "2017","2018","2019","2020","2021",
                                          "2022"]
        print(electricity_gen_data_df)

        # Extract years
        years = [str(year) for year in range(2000, 2022)]

        # Group by Technology and sum the generation for each year
        grouped_df = electricity_gen_data_df.groupby('Technology').sum().loc[:, years]

        # Calculate total generation by summing across all years for each technology
        total_generation_by_technology = grouped_df.sum(axis=1)

        # Calculate the total generation across all technologies
        total_generation_all_technologies = total_generation_by_technology.sum()

        # Calculate the percentage share for each technology
        percentages = (total_generation_by_technology / total_generation_all_technologies) * 100

        # Create a bar chart for the cumulative percentage share with annotations
        plt.figure(figsize=(12, 6))
        bars = plt.bar(percentages.index, percentages.values)

        # Annotate each bar with the percentage value
        for bar, value in zip(bars, percentages.values):
            plt.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 1, f'{value:.2f}%', ha='center', va='bottom')

        plt.xlabel('Technology')
        plt.ylabel('Cumulative Percentage Share (%)')
        plt.title('Cumulative Percentage Share of Technologies in Total Generation (2000-2021)')
        plt.xticks(rotation=45, ha='right')
        plt.tight_layout()
        # Save the plot to a file
        plt.savefig("data\outputs\Cumulative_Prcnt_Share_Technologies_Total_Elect_Generation.png")
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
#--------------------------------------Growth of Renewable and Non-Renewable over Years----------------------------------------------
@asset(
        deps=["electricity_generation_extract_and_transformation_load"]
)  
def Growth_of_Energy_over_time():
    # Connect to the PostgreSQL database
    try:
        connection = psycopg2.connect(**db_params)
        cursor = connection.cursor()

        # Replace 'your_table' with the actual table name
        table_name = 'Electricity_Generation'

        # Build the SQL query to fetch data
        query = sql.SQL("SELECT * FROM {}").format(sql.Identifier(table_name))

        # Execute the query
        cursor.execute(query)

        # Fetch all the rows
        Electricity_gen_data = cursor.fetchall()

        electricity_gen_data_df = pd.DataFrame(Electricity_gen_data)
        #print(electricity_gen_data_df)

        electricity_gen_data_df.columns =["Country","Technology","Energy_Type", "2000","2001","2002","2003","2004","2005","2006","2007",
                                          "2008","2009","2010","2011","2012","2013","2014","2015", "2016", "2017","2018","2019","2020","2021",
                                          "2022"]
        print(electricity_gen_data_df)

        # Extract years
        years = [str(year) for year in range(2000, 2022)]

        # Create lists to store total energy generation values
        total_renewable_energy_values = electricity_gen_data_df.loc[
            electricity_gen_data_df['Energy_Type'] == 'Total Renewable', years].sum(axis=0)

        total_non_renewable_energy_values = electricity_gen_data_df.loc[
            electricity_gen_data_df['Energy_Type'] != 'Total Renewable', years].sum(axis=0)

        # Create a line chart for the growth of both renewable and non-renewable energy over time
        plt.figure(figsize=(12, 6))
        plt.plot(years, total_renewable_energy_values, marker='o', label='Renewable Energy')
        plt.plot(years, total_non_renewable_energy_values, marker='o', label='Non-Renewable Energy')
        plt.xlabel('Year')
        plt.ylabel('Total Energy Generation')
        plt.title('Growth of Renewable and Non-Renewable Energy Over Time (2000-2022)')
        plt.legend()
        plt.grid(True)
        # Save the plot to a file
        plt.savefig("data\outputs\Growth_of_Energy_over_time.png")
        plt.show()

        # Close the database connection
        cursor.close()
        connection.close()

    except Exception as e:
        print(f"Error: {e}")

    finally:
        # Close the cursor and connection
        if cursor:
            cursor.close()
        if connection:
            connection.close()

#------------------------------------------------------------------------------------------------------------------------------------
#----------------------------------Renewable and Non-renewable share in the total Energy mix-----------------------------------------
@asset(
        deps=["electricity_generation_extract_and_transformation_load"]
)          
def energy_share_in_the_total_energy_mix_over_years():
    # Connect to the PostgreSQL database
    try:
        connection = psycopg2.connect(**db_params)
        cursor = connection.cursor()

        # Replace 'your_table' with the actual table name
        table_name = 'Electricity_Generation'

        # Build the SQL query to fetch data
        query = sql.SQL("SELECT * FROM {}").format(sql.Identifier(table_name))

        # Execute the query
        cursor.execute(query)

        # Fetch all the rows
        electricity_gen_data = cursor.fetchall()

        electricity_gen_data_df = pd.DataFrame(electricity_gen_data)

        electricity_gen_data_df.columns = ["Country", "Technology", "Energy_Type", "2000", "2001", "2002", "2003", "2004", "2005", "2006", "2007",
                                           "2008", "2009", "2010", "2011", "2012", "2013", "2014", "2015", "2016", "2017", "2018", "2019", "2020", "2021", "2022"]

        print(electricity_gen_data_df)

        # Extract years
        years = [str(year) for year in range(2000, 2022)]

        # Calculate the percentage share of renewable and non-renewable energy each year
        total_energy_values = electricity_gen_data_df[years].sum(axis=0)
        total_renewable_energy_values = electricity_gen_data_df[electricity_gen_data_df['Energy_Type'] == 'Total Renewable'][years].sum(axis=0)
        total_non_renewable_energy_values = electricity_gen_data_df[electricity_gen_data_df['Energy_Type'] == 'Total Non-Renewable'][years].sum(axis=0)

        renewable_share_percentage = (total_renewable_energy_values / total_energy_values) * 100
        non_renewable_share_percentage = (total_non_renewable_energy_values / total_energy_values) * 100

        # Create a line chart for the increasing share of both renewable and non-renewable in the total energy mix
        plt.figure(figsize=(12, 6))
        plt.plot(years, renewable_share_percentage, marker='o', label='Renewable Share')
        plt.plot(years, non_renewable_share_percentage, marker='o', label='Non-Renewable Share')
        plt.xlabel('Year')
        plt.ylabel('Percentage Share (%)')
        plt.title('Renewable and Non-Renewable Share in the Total Energy Mix (2000-2022)')
        plt.legend()
        plt.grid(True)
        # Save the plot to a file
        plt.savefig("data\outputs\energy_share_in_the_total_energy_mix_over_years.png")
        plt.show()

    except Exception as e:
        print(f"Error fetching or plotting data: {e}")
    finally:
        # Close the cursor and connection
        if cursor:
            cursor.close()
        if connection:
            connection.close()

#---------------------------------------------------------------------------------------------------------------------------------
#-------------------------------Total Electricity Generation for G20 Countries (2000-2022)---------------------------------------

@asset(
        deps=["electricity_generation_extract_and_transformation_load"]
)    
def total_Electricity_Generation_for_G20_Countries():
    # Connect to the PostgreSQL database
    try:
        connection = psycopg2.connect(**db_params)
        cursor = connection.cursor()

        # Replace 'your_table' with the actual table name
        table_name = 'Electricity_Generation'

        # Build the SQL query to fetch data
        query = sql.SQL("SELECT * FROM {}").format(sql.Identifier(table_name))

        # Execute the query
        cursor.execute(query)

        # Fetch all the rows
        electricity_gen_data = cursor.fetchall()

        electricity_gen_data_df = pd.DataFrame(electricity_gen_data)

        electricity_gen_data_df.columns = ["Country", "Technology", "Energy_Type", "2000", "2001", "2002", "2003", "2004", "2005", "2006", "2007",
                                          "2008", "2009", "2010", "2011", "2012", "2013", "2014", "2015", "2016", "2017", "2018", "2019", "2020", "2021",
                                          "2022"]

        # Extract years
        years = [str(year) for year in range(2000, 2022)]

        # Convert data types and fill missing values
        electricity_gen_data_df[years] = electricity_gen_data_df[years].apply(pd.to_numeric, errors='coerce').fillna(0)

        # Create a new column for total electricity generation
        electricity_gen_data_df['Total_Electricity_Generation'] = electricity_gen_data_df[years].sum(axis=1)

        # List of G20 countries
        g20_countries = ['Argentina', 'Australia', 'Brazil', 'Canada', 'China', 'France', 'Germany', 'India',
                         'Indonesia', 'Italy', 'Japan', 'Mexico', 'Russia', 'Saudi Arabia', 'South Africa',
                         'South Korea', 'Turkey', 'United Kingdom', 'United States', 'European Union']

        # Filter the DataFrame to include only G20 countries
        df_g20 = electricity_gen_data_df[electricity_gen_data_df['Country'].isin(g20_countries)]

        # Group by country and calculate the total electricity generation for each
        total_electricity_generation_g20 = df_g20.groupby('Country')['Total_Electricity_Generation'].sum()

        # Plot a bar chart for the total electricity generation of G20 countries
        plt.figure(figsize=(12, 6))
        total_electricity_generation_g20.plot(kind='bar', color='lightgreen')
        plt.xlabel('Country')
        plt.ylabel('Total Electricity Generation')
        plt.title('Total Electricity Generation for G20 Countries (2000-2022)')
        plt.xticks(rotation=45, ha='right')
        # Save the plot to a file
        plt.savefig("data\outputs\Total_Electricity_Generation_for_G20_Countries .png")
        plt.show()

    except Exception as e:
        print(f"Error: {e}")

    finally:
        # Close the cursor and connection
        if cursor:
            cursor.close()
        if connection:
            connection.close()

#-------------------------------------------------------------------------------------------------------------------------------------
#---------------------------------------Total Electricity Generation Over Years for Different Technologies----------------------------
@asset(
        deps=["electricity_generation_extract_and_transformation_load"]
)
def plot_electricity_generation_by_technology():
    # Connect to the PostgreSQL database
    try:
        connection = psycopg2.connect(**db_params)
        cursor = connection.cursor()

        # Replace 'your_table' with the actual table name
        table_name = 'Electricity_Generation'

        # Build the SQL query to fetch data
        query = sql.SQL("SELECT * FROM {}").format(sql.Identifier(table_name))

        # Execute the query
        cursor.execute(query)

        # Fetch all the rows
        electricity_gen_data = cursor.fetchall()

        electricity_gen_df = pd.DataFrame(electricity_gen_data)

        electricity_gen_df.columns = ["Country", "Technology", "Energy_Type", "2000", "2001", "2002", "2003", "2004", "2005", "2006", "2007",
                                      "2008", "2009", "2010", "2011", "2012", "2013", "2014", "2015", "2016", "2017", "2018", "2019", "2020", "2021",
                                      "2022"]

        # Extract years
        years = [str(year) for year in range(2000, 2022)]

        # Convert data types and fill missing values
        electricity_gen_df[years] = electricity_gen_df[years].apply(pd.to_numeric, errors='coerce').fillna(0)

        # Group by technology and sum the values for each year
        grouped_df = electricity_gen_df.groupby('Technology')[years].sum().transpose()

        # Plot the graph
        plt.figure(figsize=(12, 6))
        grouped_df.plot(kind='bar', stacked=True)
        plt.xlabel('Year')
        plt.ylabel('Total Electricity Generation (GWh)')
        plt.title('Total Electricity Generation Over Years for Different Technologies')
        plt.legend(title='Technology', bbox_to_anchor=(1.05, 1), loc='upper left')
        # Save the plot to a file
        plt.savefig("data\outputs\Total_Electricity_Generation_Over_Years_for_Different_Technologies .png")
        plt.show()

    except Exception as e:
        print(f"Error: {e}")

    finally:
        # Close the cursor and connection
        if cursor:
            cursor.close()
        if connection:
            connection.close()