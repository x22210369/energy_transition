import psycopg2
from psycopg2 import sql
import pandas as pd
import matplotlib.pyplot as plt
from dagster import asset

db_params = {
    "host": "localhost",
    "database": "energy",
    "user": "postgres",
    "password": "dap",
    "port": 5433
}

#------------------------------------------------------------------------------------------------------------------------------------
#---------------------------------------Total Electricity Installed Capacity By years(2000-2021)--------------------------------------
@asset(
        deps=["electricity_installed_capacity_Extract_transform_load"]
)
def Total_Electricity_Installed_Capacity_By_Technology():
    try:
        connection = psycopg2.connect(**db_params)
        cursor = connection.cursor()

        table_name = 'Electricity_Installed_Capacity'

        query = sql.SQL("SELECT * FROM {}").format(sql.Identifier(table_name))

        cursor.execute(query)

        Electricity_installed_data = cursor.fetchall()

        Electricity_installed_data_df = pd.DataFrame(Electricity_installed_data)
        #print(Electricity_installed_data_df)

        Electricity_installed_data_df.columns =["Country","Technology","Energy_Type", "2000","2001","2002","2003","2004","2005","2006","2007",
                                          "2008","2009","2010","2011","2012","2013","2014","2015", "2016", "2017","2018","2019","2020","2021",
                                          "2022"]

        years = [str(year) for year in range(2000, 2021)]

        Electricity_installed_data_df[years] = Electricity_installed_data_df[years].apply(pd.to_numeric, errors='coerce').fillna(0)

        grouped_df = Electricity_installed_data_df.groupby('Technology')[years].sum().transpose()

        plt.figure(figsize=(12, 6))
        for technology in grouped_df.columns:
            plt.plot(grouped_df.index, grouped_df[technology], marker='o', label=technology)

        plt.xlabel('Year')
        plt.ylabel('Total Electricity Installed Capacity (MW)')
        plt.title('Total Electricity Installed Capacity Over Years for Different Technologies')
        plt.legend()
        plt.grid(True)

        plt.savefig("data\outputs\Total_Electricity_Installed_By_Technology.png")
 
        plt.show()

    except Exception as e:
        print(f"Error: {e}")

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

#------------------------------------------------------------------------------------------------------------------------------------
#--------------------------------------Growth of Renewable and Non-Renewable over time for Installed Capacity------------------------

@asset(
        deps=["electricity_installed_capacity_Extract_transform_load"]
)
def Growth_of_Energy_over_time_Installed_Capacity():
    try:
        connection = psycopg2.connect(**db_params)
        cursor = connection.cursor()

        table_name = 'Electricity_Installed_Capacity'

        query = sql.SQL("SELECT * FROM {}").format(sql.Identifier(table_name))

        cursor.execute(query)

        Electricity_installed_data = cursor.fetchall()

        Electricity_installed_data_df = pd.DataFrame(Electricity_installed_data)
        #print(Electricity_installed_data_df)

        Electricity_installed_data_df.columns =["Country","Technology","Energy_Type", "2000","2001","2002","2003","2004","2005","2006","2007",
                                          "2008","2009","2010","2011","2012","2013","2014","2015", "2016", "2017","2018","2019","2020","2021",
                                          "2022"]

        print(Electricity_installed_data_df)

        years = [str(year) for year in range(2000, 2021)]

        total_renewable_energy_values = Electricity_installed_data_df.loc[
            Electricity_installed_data_df['Energy_Type'] == 'Total Renewable', years].sum(axis=0)

        total_non_renewable_energy_values = Electricity_installed_data_df.loc[
            Electricity_installed_data_df['Energy_Type'] != 'Total Renewable', years].sum(axis=0)

        # Creating a line chart for the growth of both renewable and non-renewable energy over time
        plt.figure(figsize=(12, 6))
        plt.plot(years, total_renewable_energy_values, marker='o', label='Renewable Energy')
        plt.plot(years, total_non_renewable_energy_values, marker='o', label='Non-Renewable Energy')
        plt.xlabel('Year')
        plt.ylabel('Total Electricity Installed Capacity')
        plt.title('Growth of Renewable and Non-Renewable Energy Over Time (2000-2022) for Electricity Installed Capacity')
        plt.legend()
        plt.grid(True)

        plt.savefig("data\outputs\Growth_of_Energy_over_time.png")
        plt.show()

    
        cursor.close()
        connection.close()

    except Exception as e:
        print(f"Error: {e}")

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

#--------------------------------------------------------------------------------------------------------------------------------------
#---------------------------------------------Renewable and Non-renewable share in the total Energy mix-------------------------------
@asset(
        deps=["electricity_installed_capacity_Extract_transform_load"]
)      
def energy_share_in_the_total_energy_mix_over_years_Installed():
    try:
        connection = psycopg2.connect(**db_params)
        cursor = connection.cursor()

        table_name = 'Electricity_Installed_Capacity'

        query = sql.SQL("SELECT * FROM {}").format(sql.Identifier(table_name))

        cursor.execute(query)

        Electricity_installed_data = cursor.fetchall()

        Electricity_installed_data_df = pd.DataFrame(Electricity_installed_data)
        #print(Electricity_installed_data_df)

        Electricity_installed_data_df.columns =["Country","Technology","Energy_Type", "2000","2001","2002","2003","2004","2005","2006","2007",
                                          "2008","2009","2010","2011","2012","2013","2014","2015", "2016", "2017","2018","2019","2020","2021",
                                          "2022"]

        print(Electricity_installed_data_df)

        years = [str(year) for year in range(2000, 2022)]

        # Calculating the percentage share of renewable and non-renewable energy each year
        total_energy_values = Electricity_installed_data_df[years].sum(axis=0)
        total_renewable_energy_values = Electricity_installed_data_df[Electricity_installed_data_df['Energy_Type'] == 'Total Renewable'][years].sum(axis=0)
        total_non_renewable_energy_values = Electricity_installed_data_df[Electricity_installed_data_df['Energy_Type'] == 'Total Non-Renewable'][years].sum(axis=0)

        renewable_share_percentage = (total_renewable_energy_values / total_energy_values) * 100
        non_renewable_share_percentage = (total_non_renewable_energy_values / total_energy_values) * 100

        # Creating a line chart for the increasing share of both renewable and non-renewable in the total energy mix
        plt.figure(figsize=(12, 6))
        plt.plot(years, renewable_share_percentage, marker='o', label='Renewable Share')
        plt.plot(years, non_renewable_share_percentage, marker='o', label='Non-Renewable Share')
        plt.xlabel('Year')
        plt.ylabel('Percentage Share (%)')
        plt.title('Renewable and Non-Renewable Share in the Total Energy Mix (2000-2022) for Installed Capacity')
        plt.legend()
        plt.grid(True)

        plt.savefig("data\outputs\energy_share_in_the_total_energy_mix_over_years_installed_capacity.png")
        plt.show()

    except Exception as e:
        print(f"Error fetching or plotting data: {e}")
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

#------------------------------------------------------------------------------------------------------------------------------------
#-----------------------------------------Total Electricity Installed Capacity for G20 Countries (2000-2022)----------------------
@asset(
        deps=["electricity_installed_capacity_Extract_transform_load"]
) 
def plot_installed_capacity_g20():
    try:
        connection = psycopg2.connect(**db_params)
        cursor = connection.cursor()

        table_name = 'Electricity_Installed_Capacity'

        query = sql.SQL("SELECT * FROM {}").format(sql.Identifier(table_name))

        cursor.execute(query)

        installed_capacity_data = cursor.fetchall()

        installed_capacity_df = pd.DataFrame(installed_capacity_data)

        installed_capacity_df.columns = ["Country", "Technology", "Installed_Capacity_Type", "2000", "2001", "2002", "2003", "2004", "2005", "2006", "2007",
                                          "2008", "2009", "2010", "2011", "2012", "2013", "2014", "2015", "2016", "2017", "2018", "2019", "2020", "2021",
                                          "2022"]

        years = [str(year) for year in range(2000, 2023)]

        installed_capacity_df[years] = installed_capacity_df[years].apply(pd.to_numeric, errors='coerce').fillna(0)

        installed_capacity_df['Total_Installed_Capacity'] = installed_capacity_df[years].sum(axis=1)

        g20_countries = ['Argentina', 'Australia', 'Brazil', 'Canada', 'China', 'France', 'Germany', 'India',
                         'Indonesia', 'Italy', 'Japan', 'Mexico', 'Russia', 'Saudi Arabia', 'South Africa',
                         'South Korea', 'Turkey', 'United Kingdom', 'United States', 'European Union']

        # Filtering the DataFrame to include only G20 countries
        df_g20 = installed_capacity_df[installed_capacity_df['Country'].isin(g20_countries)]

        # Grouping by country and calculate the total installed capacity for each
        total_installed_capacity_g20 = df_g20.groupby('Country')['Total_Installed_Capacity'].sum()

        # Plotting a bar chart for the total installed capacity of G20 countries
        plt.figure(figsize=(12, 6))
        total_installed_capacity_g20.plot(kind='bar', color='skyblue')
        plt.xlabel('Country')
        plt.ylabel('Total Electricity Installed Capacity')
        plt.title('Total Electricity Installed Capacity for G20 Countries (2000-2022)')
        plt.xticks(rotation=45, ha='right')
        plt.savefig("data\outputs\Total_Electricity_Installed_Capacity_for_G20_Countries .png")
        plt.show()

    except Exception as e:
        print(f"Error: {e}")

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()
