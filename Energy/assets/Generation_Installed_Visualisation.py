import pandas as pd
import matplotlib.pyplot as plt
from sqlalchemy import create_engine
from dagster import asset

# Replace these with your actual database credentials
db_params = {
    "host": "localhost",
    "database": "energy",
    "user": "postgres",
    "password": "dap",
    "port": 5433
}
#----------------------------------------------------------------------------------------------------------------
@asset(
        deps=["electricity_installed_capacity_Extract_transform_load","electricity_generation_extract_and_transformation_load"]
) 
def Capacity_factor_by_technology_over_year():
    def calculate_capacity_factor(actual_generation, max_capacity):
        return (actual_generation / max_capacity) * 100 if max_capacity != 0 else 0

    try:
        # Create a SQLAlchemy engine
        engine = create_engine(f"postgresql://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['database']}")

        # Fetch data for electricity generation
        generation_table = 'Electricity_Generation'
        df_generation = pd.read_sql_table(generation_table, con=engine)

        # Fetch data for electricity installed capacity
        capacity_table = 'Electricity_Installed_Capacity'
        df_capacity = pd.read_sql_table(capacity_table, con=engine)

        # Extract unique technologies
        unique_technologies = df_generation['Technology'].unique()

        # Extract years
        years = [str(year) for year in range(2000, 2022)]  # Adjust the range based on your data

        results_list = []

        # Iterate through each unique technology
        for technology in unique_technologies:
            row_data = {'Technology': technology}

            for year in years:
                actual_generation = df_generation[df_generation['Technology'] == technology][year].sum() if not df_generation.empty else 0
                max_capacity = df_capacity[df_capacity['Technology'] == technology][year].sum() if not df_capacity.empty else 0
                capacity_factor = calculate_capacity_factor(actual_generation, max_capacity)
                row_data[f'Capacity_Factor_{year}'] = capacity_factor

            results_list.append(row_data)

        # Convert the list of dictionaries to a DataFrame
        df_capacity_factor = pd.DataFrame(results_list)

        # Plot the capacity factor for each unique technology over the years
        plt.figure(figsize=(10, 7))
        for _, row in df_capacity_factor.iterrows():
            plt.plot(years, row[1:], marker='o', label=row['Technology'])

        plt.xlabel('Year')
        plt.ylabel('Capacity Factor (%)')
        plt.title('Capacity Factor by Unique Technology (2000-2021)')
        plt.legend(bbox_to_anchor=(0.70, 1), loc='upper left')
        plt.grid(True)
        # Save the plot to a file
        plt.savefig("data\outputs\Capacity_factor_by_technology_over_year.png")
        plt.show()

    except Exception as e:
        print(f"Error fetching or plotting data: {e}")

