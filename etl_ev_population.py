
"""# Import packages"""
import pandas as pd
import numpy as np
import os

#from shapely import wkt
#import geopandas as gpd

import configparser
from sqlalchemy import create_engine

import warnings
warnings.filterwarnings('ignore')

"""# 1.Extract and Explore"""

dt=pd.read_csv('Electric_Vehicle_Population_Data.csv',dtype=str)
dt=dt[dt['State']=='WA']

dt.shape

dt.info()

dt.head(3)

#rename fields

dt.rename(columns={'VIN (1-10)':'vin',
                   'Electric Vehicle Type':'ev_type',
                   'Clean Alternative Fuel Vehicle (CAFV) Eligibility':'cafv_eligibility',
                   'Electric Range':'electric_range',
                   'State':'state',
                   "County":"county",
                   'City':'city',
                   'Base MSRP':'base_msrp',
                   'Model Year':'model_year',
                   'Make':'make',
                   'Model':'model',
                   'Postal Code':'postal_code',
                   'Legislative District':'legislative_district',
                   'DOL Vehicle ID':'dol_vehicle_id',
                   'Vehicle Location':'vehicle_location',
                   'Electric Utility':'electric_utility',
                   '2020 Census Tract':'census_tract',
                 },inplace=True)

# redefine data type

dt['electric_range']=dt['electric_range'].astype(float)
dt['base_msrp'] = dt['base_msrp'].astype(float)
dt['model_year']=dt['model_year'].astype(int)


# explore the distribution of numerical feilds "electric_range" and "base_msrp"

dt[["electric_range","base_msrp"]].describe()

# explore the central tendency of model year

dt['model_year'].quantile([0,0.1,0.25,0.5,0.75,0.9,1]).astype(int)

# explore the dispersion of categorical fields "ev_type" and "cafv_eligibility" 

dt['ev_type'].value_counts()
dt['cafv_eligibility'].value_counts().sort_index()

"""# 2. Clean and Transform """

# derive "Latitude" field and "Lonitude" field from "Vehicle Localtion"

dt['vehicle_location'].replace(np.nan,'POINT EMPTY',inplace=True)
dt[['latitude','longitude']]=dt['vehicle_location'].str.extract(r'POINT \(([-\d\.]+) ([-\d\.]+)\)')

#dt['latitude'] = dt['vehicle_location'].apply(wkt.loads).apply(lambda p: p.x if p.is_empty == False else np.nan)
#dt['longitude'] = dt['vehicle_location'].apply(wkt.loads).apply(lambda p: p.y if p.is_empty == False else np.nan)

# handle missing value handling ("electric_range" and "base_msrp)

# replace 0 with nan then filling missing value with group median

dt["electric_range"].replace({0:np.nan},inplace=True)
dt["base_msrp"].replace({0:np.nan},inplace=True)

def fill_missing(dt,cols_list):
    for col in cols_list:
        dt[col] = dt.groupby(['make', 'model'])[col].transform(lambda x: x.fillna(x.median()))
        dt[col] = dt.groupby('make')[col].transform(lambda x: x.fillna(x.median()))
        dt[col] = dt[col].fillna(dt[col].median())
    return dt

dt=fill_missing(dt,['base_msrp','electric_range'])

# encode 'ev_type' and 'cafv_eligibility'

dt['ev_type'].replace({'Battery Electric Vehicle (BEV)':'1',
                       'Plug-in Hybrid Electric Vehicle (PHEV)':'2'},inplace=True)

dt['cafv_eligibility'].replace({'Clean Alternative Fuel Vehicle Eligible':'1',
                                 'Not eligible due to low battery range':'2',
                                 'Eligibility unknown as battery range has not been researched':'3'},inplace=True)

"""# 3. Load Data """

"""# create dim datasets and fact datasets"""

# Dim_Vehicle_model
Dim_Vehicle_model=dt[['make','model','model_year','ev_type','electric_range','base_msrp']].drop_duplicates().reset_index(drop=True)
Dim_Vehicle_model['vehicle_model_id']=Dim_Vehicle_model.index+1
dt=dt.merge(Dim_Vehicle_model,on=['make','model','model_year','ev_type','electric_range','base_msrp'],how='left')

# Dim_Location
Dim_Location=dt[['state','county','city','postal_code','latitude','longitude']].drop_duplicates().reset_index(drop=True)
Dim_Location['location_id']=Dim_Location.index+1
dt=dt.merge(Dim_Location,on=['state','county','city','postal_code','latitude','longitude'],how='left')

# Dim_Policy_Eligibility
Dim_Policy_Eligibility=dt[['cafv_eligibility']].drop_duplicates().reset_index(drop=True)
Dim_Policy_Eligibility['policy_id']=Dim_Policy_Eligibility.index+1
dt=dt.merge(Dim_Policy_Eligibility,on='cafv_eligibility',how='left')

# Dim_District
Dim_District=dt[['legislative_district','census_tract']].drop_duplicates().reset_index(drop=True)
Dim_District['district_id']=Dim_District.index+1
dt=dt.merge(Dim_District,on=['legislative_district','census_tract'],how='left')

# Dim_Utility
Dim_Utility=dt[['electric_utility']].drop_duplicates().reset_index(drop=True)
Dim_Utility['utility_id']=Dim_Utility.index+1
dt=dt.merge(Dim_Utility,on='electric_utility',how='left')
# Fact_EV

Fact_EV=dt[['vin','dol_vehicle_id','vehicle_model_id','location_id','policy_id','district_id','utility_id']].copy()
Fact_EV['ev_id']=Fact_EV.index+1

"""# read Config file and build connection with sql server"""


script_dir = os.getcwd()
config_path = os.path.join(script_dir, 'db_config.ini')
config = configparser.ConfigParser()
files_read=config.read(config_path)

if not files_read:
    raise FileNotFoundError("Failed to read configuration file")
else:
    print(f"Successfully read:{files_read}")

db=config['sqlserver']
server=db['server']
database=db['database']
username=db['username']
password=db['password']
driver=db['driver']

connection_string = f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server"

engine = create_engine(connection_string)

"""# complete the data loading process"""

Dim_Vehicle_model.to_sql('dim_vehicle_model',engine,if_exists='replace',index=False)
Dim_Location.to_sql('dim_location',engine,if_exists='replace',index=False)
Dim_Policy_Eligibility.to_sql('dim_policy_eligibility',engine,if_exists='replace',index=False)
Dim_District.to_sql('dim_district',engine,if_exists='replace',index=False)
Dim_Utility.to_sql('dim_utility',engine,if_exists='replace',index=False)
Fact_EV.to_sql('fact_ev',engine,if_exists='replace',index=False)