# fmt: off
from dagster import Definitions, load_assets_from_modules

from .assets import  Electricity_Generation_ETL, Data_Fetching_and_storing, Electricity_Gen_Visualisation, Electricity_Installed_Capacity_ETL, Electricity_Installed_Visualisation, Generation_Installed_Visualisation,Greenhouse_gas_visualisation,Greenhouse_gas_ETL


Data_Fetching_and_storing_assets = load_assets_from_modules([Data_Fetching_and_storing])
Electricity_Generation_ETL_assets = load_assets_from_modules([Electricity_Generation_ETL])
Electricity_Installed_Capacity_ETL_assets = load_assets_from_modules([Electricity_Installed_Capacity_ETL])
Greenhouse_Gas_Emissions_ETL_assets = load_assets_from_modules([Greenhouse_gas_ETL])
ELectricity_Gen_visualisation_assets = load_assets_from_modules([Electricity_Gen_Visualisation])
ELectricity_Installed_Capacity_visualisation_assets = load_assets_from_modules([Electricity_Installed_Visualisation])
Generation_Installed_Visualisation_assets = load_assets_from_modules([Generation_Installed_Visualisation])
Greenhouse_gas_Visualisation_assets = load_assets_from_modules([Greenhouse_gas_visualisation])

defs = Definitions(
    assets=[*Data_Fetching_and_storing_assets, *Electricity_Generation_ETL_assets,*Electricity_Installed_Capacity_ETL_assets,*Greenhouse_Gas_Emissions_ETL_assets,
            *ELectricity_Gen_visualisation_assets, *ELectricity_Installed_Capacity_visualisation_assets,
            *Generation_Installed_Visualisation_assets,*Greenhouse_gas_Visualisation_assets]
)
