"""
Purpose: Preprocess and agg some of the alt data.
Loads from S3, standardizes date formats, groups by census tract and time periods,
and merges with sales data. Output to postgres.
"""

import pandas as pd
import numpy as np
from sqlalchemy import create_engine

pd.set_option('display.max_columns', None)

DB_CONN = 'postgresql://darien:@localhost:5432/alt_data'

# Load data from S3
DATA_DIR = 's3://general-scratch/alt_data'

datasets = {
    'sales': 'All_Boroughs_geocoded_With_2023.csv',
    'complaints': 'DOB_Complaints_Geocoded.csv',
    'operating_businesses': 'Businesses_Operating_Geocoded.csv',
    'evictions': 'Evictions_Geocoded.csv',
    'restaurants': 'Restaurants_Geocoded.csv',
    'health_inspections': 'DOHMH_New_York_City_Restaurant_Inspection_Results.csv',
    'citi': 'citibike_geocoded_v2.parquet'
}

data = {k: pd.read_csv(f'{DATA_DIR}/{v}', storage_options={'anon': False}) if v.endswith('.csv') 
        else pd.read_parquet(f'{DATA_DIR}/{v}', storage_options={'anon': False}) 
        for k, v in datasets.items()}

def standardize_dates(df, date_col, new_col='date'):
    df[new_col] = pd.to_datetime(df[date_col])
    df['year'] = df[new_col].dt.year
    df['yr-month'] = df[new_col].dt.strftime('%Y-%m')
    return df

def group_by_tract(df, col='tract', bins=range(0, 303000, 10000)):
    """Group data by census tract."""
    df['tract_1000_grp'] = pd.cut(df[col], bins=bins, right=True, labels=False) + 1
    return df


data['sales'] = standardize_dates(data['sales'], 'SALE_DATE')
data['complaints'] = standardize_dates(data['complaints'], 'Date Entered')
data['operating_businesses'] = standardize_dates(data['operating_businesses'], 'license_creation_date')
data['evictions'] = standardize_dates(data['evictions'], 'executed_date')
data['restaurants'] = standardize_dates(data['restaurants'], 'Time of Submission')
data['health_inspections'] = standardize_dates(data['health_inspections'], 'INSPECTION DATE')
data['health_inspections'] = data['health_inspections'].dropna(subset=['SCORE'])
data['health_inspections']['GRADE'] = data['health_inspections']['SCORE'].apply(lambda x: 'A' if x < 14 else ('B' if 14 <= x <= 27 else 'C'))

for key in ['sales', 'complaints', 'operating_businesses', 'evictions', 'restaurants']:
    data[key] = group_by_tract(data[key])

data['health_inspections'] = group_by_tract(data['health_inspections'], 'Census Tract')

# Aggregate data
aggregations = {
    'operating_businesses_yr': ['year', 'tract_1000_grp'],
    'evictions_yr': ['year', 'tract_1000_grp'],
    'restaurants_yr': ['year', 'tract_1000_grp'],
    'complaints_month': ['yr-month', 'tract_1000_grp'],
    'operating_businesses_month': ['yr-month', 'tract_1000_grp'],
    'evictions_month': ['yr-month', 'tract_1000_grp'],
    'restaurants_month': ['yr-month', 'tract_1000_grp'],
}

for key, group_cols in aggregations.items():
    source = key.split('_')[0]
    data[key] = data[source].groupby(group_cols, as_index=False).agg({'tract': 'count'}).sort_values(by=group_cols)

# Merge sales data with other datasets
merged_datasets = ['evictions', 'complaints', 'restaurants', 'operating_businesses']
for dataset in merged_datasets:
    data[f'sales_{dataset}'] = pd.merge(
        data['sales'].groupby(['yr-month', 'tract_1000_grp'], as_index=False).agg({'SALE_PRICE': 'mean'}),
        data[f'{dataset}_month'],
        how='right', on=['yr-month', 'tract_1000_grp']
    )

engine = create_engine(DB_CONN)
for key, df in data.items():
    df.to_sql(key, engine, if_exists='replace', index=False)