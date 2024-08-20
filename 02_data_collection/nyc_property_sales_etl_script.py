# nyc_property_sales_etl_script.py
"""
Purpose: Process and geocode NYC property sales data from Excel files,
combining data from multiple boroughs and years.
"""

import os
import re
import boto3
import pandas as pd
from dotenv import load_dotenv
from utils.census_geocode_api import fetch_geocode_coordinates, extract_data
from utils import census_geocode_api as census_api
from utils import db_utils

import warnings
warnings.filterwarnings("ignore")
pd.options.mode.chained_assignment = None
pd.set_option("display.max_columns", None)
pd.set_option("display.max_rows", None)

load_dotenv()
GOOGLE_MAPS_API_KEY = os.getenv('GOOGLE_MAPS_API_KEY')
psql_conn = db_utils.get_postgres_conn()

COLS = [
    "BOROUGH", "NEIGHBORHOOD", "BUILDING_CLASS_CATEGORY", "TAX_CLASS_PRESENT",
    "BLOCK", "LOT", "EASE-MENT", "BUILDING_CLASS_PRESENT", "ADDRESS",
    "APARTMENT_NUMBER", "ZIP_CODE", "RESIDENTIAL_UNITS", "COMMERCIAL_UNITS",
    "TOTAL_UNITS", "LAND_SF", "GROSS_SF", "YEAR_BUILT", "TAX_CLASS_SALE",
    "BUILDING_CLASS_SALE", "SALE_PRICE", "SALE_DATE"
]

def read_excel_files(links):
    """Read and combine multiple Excel files into a single DataFrame."""
    df_combined = pd.DataFrame()
    for link in links:
        try:
            data = pd.read_excel(link, header=None, skiprows=10)
            df_combined = df_combined.append(data)
        except Exception as e:
            print(f"Error reading file {link}: {e}")
    df_combined.columns = COLS
    return df_combined

def filter_property_types(df):
    df["BULIDING_CLASS_NUM"] = df["BUILDING_CLASS_CATEGORY"].apply(
        lambda x: re.sub("[^0-9]", "", x.split(" ")[0])
    )
    df["BULIDING_CLASS_NUM"] = pd.to_numeric(df["BULIDING_CLASS_NUM"])
    keep_nums = list(range(1, 18)) + [23, 28] + list(range(42, 50))
    return df[df["BULIDING_CLASS_NUM"].isin(keep_nums)]

def calculate_price_metrics(df):
    """calculate ppu and ppsf"""
    df["PP_UNIT"] = df.apply(
        lambda row: (
            row["SALE_PRICE"] / row["TOTAL_UNITS"]
            if row["TOTAL_UNITS"] != 0 and pd.notna(row["TOTAL_UNITS"])
            else row["SALE_PRICE"]
        ),
        axis=1,
    )
    df["PPSF"] = df.apply(
        lambda row: row["SALE_PRICE"] / row["GROSS_SF"] if row["GROSS_SF"] != 0 else 0,
        axis=1,
    )
    return df

def update_location_info(df):
    loc_key = {1: "Manhattan", 2: "Bronx", 3: "Brooklyn", 4: "Queens", 5: "Staten Island"}
    if list(df["BOROUGH"])[0] in loc_key.keys():
        df["BOROUGH"] = df["BOROUGH"].map(loc_key, na_action="ignore")
    df["STATE"] = "NY"
    return df

def preprocess_df(df):
    df = df.dropna(subset=["SALE_PRICE"])
    df["SALE_PRICE"] = df["SALE_PRICE"].astype(int)
    df = df[(df["SALE_PRICE"] > 10000) & (df["SALE_PRICE"] < 7500000)] # filter out outliers that are less representative
    df = filter_property_types(df)
    df = calculate_price_metrics(df)
    df = update_location_info(df)
    df = df[df["PP_UNIT"] >= 50000]
    df['Date'] = pd.to_datetime(df['SALE_DATE'])
    df['Year'] = df['Date'].dt.year
    return df

def geocode_sales_data(df, save_path):
    geocoded_df = census_api.geocode_multi_batch(
        df,
        address_col="ADDRESS",
        city_col="BOROUGH",
        state_col="STATE",
        zip_col="ZIP_CODE",
        save_path=save_path
    )
    geocoded_df.to_sql("nyc_property_sales_geocoded", psql_conn, if_exists="replace", index=False)
    return geocoded_df

links = []
for borough in ['manhattan', 'bronx', 'brooklyn', 'queens', 'statenisland', 'staten_island']:
    links.append(f'https://www.nyc.gov/assets/finance/downloads/pdf/rolling_sales/rollingsales_{borough}.xlsx')

df = read_excel_files(links)
df = preprocess_df(df)

geocoded_df = geocode_sales_data(df, "Geocoded_Data/All_Boroughs_geocoded.csv")

df_2023 = pd.read_csv('Geocoded_Data/preprocessed_NOT_geocoded_2023.csv')
geocoded_df_2023 = geocode_sales_data(df_2023, "Geocoded_Data/All_Boroughs_geocoded_2023.csv")

df_all = pd.read_csv('Geocoded_Data/All_Boroughs_geocoded.csv')
df_2023 = pd.read_csv('Geocoded_Data/All_Boroughs_geocoded_2023.csv')
df_both = pd.concat([df_all, df_2023], axis=0, ignore_index=True)
df_both['Date'] = pd.to_datetime(df_both['SALE_DATE'])
df_both['Year'] = df_both['Date'].dt.year


df_both.to_csv('Geocoded_Data/All_Boroughs_geocoded_With_2023.csv', index=False)

s3 = boto3.client('s3')
s3_bucket = 'general-scratch'
s3_key = 'alt_data/All_Boroughs_geocoded_With_2023.csv'
s3.upload_file('Geocoded_Data/All_Boroughs_geocoded_With_2023.csv', s3_bucket, s3_key)
print(f"File uploaded to s3://{s3_bucket}/{s3_key}")

df_both.to_sql("nyc_property_sales_all", psql_conn, if_exists="replace", index=False)
print("Data processing and geocoding completed.")