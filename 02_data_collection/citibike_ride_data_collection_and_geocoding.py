
# citibike_ride_data_collection_and_geocoding.py
"""
Purpose: Obtain, preprocess, and geocode Citi Bike historical ride data from public S3 bucket 
containing zip files with monthly CSVs. 

- Use Dask, since total rows surpasses 100m, to read the CSVs in async, standardize the column names and concatenate them. 
- Use google maps and census apis to obtain standarized address to obtain census level geocoding.
"""

import os
import io
import tempfile
from zipfile import ZipFile
from concurrent.futures import ThreadPoolExecutor
import concurrent.futures

import boto3
import s3fs
import pandas as pd
import dask.dataframe as dd
from tqdm import tqdm


import warnings
warnings.filterwarnings("first")

from dotenv import load_dotenv
load_dotenv()
from utils.census_geocode_api import fetch_geocode_coordinates, extract_data
GOOGLE_MAPS_API_KEY = os.getenv('GOOGLE_MAPS_API_KEY')

from utils import db_utils
psql_conn = db_utils.get_postgres_conn()

BUCKET_NAME = "tripdata"
BASE_URL = "https://s3.amazonaws.com/tripdata/"

COLUMN_MAPPING = {
    "starttime": "start_time", "Start Time": "start_time", "started_at": "start_time",
    "stoptime": "stop_time", "Stop Time": "stop_time", "ended_at": "stop_time",
    "start station id": "start_station_id", "Start Station ID": "start_station_id", "start_station_id": "start_station_id",
    "start station name": "start_station_name", "Start Station Name": "start_station_name", "start_station_name": "start_station_name",
    "start station latitude": "start_station_latitude", "Start Station Latitude": "start_station_latitude", "start_lat": "start_station_latitude",
    "start station longitude": "start_station_longitude", "Start Station Longitude": "start_station_longitude", "start_lng": "start_station_longitude",
    "end station id": "end_station_id", "End Station ID": "end_station_id", "end_station_id": "end_station_id",
    "end station name": "end_station_name", "End Station Name": "end_station_name", "end_station_name": "end_station_name",
    "end station latitude": "end_station_latitude", "End Station Latitude": "end_station_latitude", "end_lat": "end_station_latitude",
    "end station longitude": "end_station_longitude", "End Station Longitude": "end_station_longitude", "end_lng": "end_station_longitude",
    "usertype": "user_type", "User Type": "user_type", "member_casual": "user_type",
}

DESIRED_COLUMNS = [
    "end_station_id", "start_station_id", "stop_time", "start_station_name",
    "start_station_latitude", "user_type", "start_time", "start_station_longitude",
    "end_station_name", "end_station_longitude", "end_station_latitude"
]

def list_s3_files():
    s3_client = boto3.client("s3")
    response = s3_client.list_objects_v2(Bucket=BUCKET_NAME)
    
    s3_citibike_files = []
    if "Contents" in response:
        for obj in response["Contents"]:
            file_url = f"s3://{BUCKET_NAME}/{obj['Key']}"
            s3_citibike_files.append(file_url)
            file_size = obj["Size"]
            print(f"File: {file_url.replace(BASE_URL, '')} ({file_size / 10**6:.2f} MB)")
    else:
        print("No files found.")
    
    return sorted([file for file in s3_citibike_files if file.endswith(".csv.zip") and "JC" in file])[:-14]

def process_zip_file(s3_file_path, tmpdirname):
    """Process a single ZIP file"""
    fs = s3fs.S3FileSystem(anon=True)
    
    try:
        input_zip = ZipFile(io.BytesIO(fs.cat(s3_file_path)))
        csv_files = [name for name in input_zip.filelist if name.filename.endswith(".csv")]
        dfs_year = []

        for csv in csv_files:
            csv_path = os.path.join(tmpdirname, os.path.basename(csv.filename))
            with input_zip.open(csv.filename) as csv_file, open(csv_path, "wb") as f:
                f.write(csv_file.read())

            try:
                df_dask = dd.read_csv(csv_path, blocksize=25e6, assume_missing=True)
                df_dask = df_dask.rename(columns=COLUMN_MAPPING)
                df_dask = df_dask.astype({"end_station_id": "object", "start_station_id": "object"})
                df_dask = df_dask[DESIRED_COLUMNS]
                dfs_year.append(df_dask)
                print(f"Processed {csv.filename}")
            except Exception as e:
                print(f"Error processing {csv.filename}: {e}")
                continue

        return dfs_year

    except Exception as e:
        print(f"Error processing {s3_file_path}: {e}")
        return None

def fetch_positions(latitude, longitude, index):
    if index % 10 == 0:
        print(index)
    return fetch_geocode_coordinates(latitude, longitude)

def async_geocode_fetch(latitudes, longitudes):
    """Fetch geocode data for multiple coordinates in async."""
    addresses = []
    with concurrent.futures.ThreadPoolExecutor() as executor:
        results = [executor.submit(fetch_positions, latitudes[i], longitudes[i], i) for i in range(len(latitudes))]
        for f in concurrent.futures.as_completed(results):
            addresses.append(f.result())
    return addresses

def process_ride_data():
    """Process and store Citi Bike ride data from S3 to psql database."""
    s3_citibike_file_urls = list_s3_files()
    
    with tempfile.TemporaryDirectory() as tmpdirname:
        with ThreadPoolExecutor() as executor:
            futures = list(tqdm(
                executor.map(lambda x: process_zip_file(x, tmpdirname), s3_citibike_file_urls),
                total=len(s3_citibike_file_urls),
                desc="Processing ZIP files"
            ))

        dfs_yield = [df for df in futures if df is not None]
        
        df_year_concat = []
        for i, df_year in enumerate(dfs_yield):
            print(f"Processing year {i}")
            for j in range(len(df_year)):
                df_year[j] = df_year[j].reset_index(drop=True)
            df_year_concat.append(dd.concat(df_year, axis=0)) if df_year else None

        dfs_all = dd.concat(df_year_concat, axis=0)
        
        dfs_all.to_sql("citibike_ride_history_full", psql_conn, if_exists="replace", index=False, chunksize=1_000_000, method="multi")

def geocode_stations():
    """Geocode Citi Bike stations and store results in postgresql database."""
    stations = pd.read_sql("SELECT * FROM citibike_stations", psql_conn)
    lat = list(stations['latitude'])
    lng = list(stations['longitude'])
    addresses = async_geocode_fetch(lat, lng)

    geo_data = []
    for address in addresses:
        all_dict = {}
        all_dict.update(extract_data(address, "2020 Census Blocks", ["BLOCK", "CENTLAT", "CENTLON", "AREALAND", "TRACT"]))
        all_dict.update(extract_data(address, "2020 Census Blocks", ["TRACT"]))
        geo_data.append(all_dict)

    geo_data_df = pd.DataFrame(geo_data)

    stations_con = stations.copy().reset_index()
    geo_stations = pd.concat([stations_con, geo_data_df], axis=1)

    geo_stations.drop(columns=['CENTLAT', 'CENTLON', 'AREALAND'], inplace=True)
    
    geo_stations.to_sql("citibike_stations_geocoded", psql_conn, if_exists="replace", index=False)

def process_and_geocode_data():
    """Process ride data, merge with geocoded station data, and store results."""
    df = pd.read_sql("SELECT * FROM citibike_ride_history_full", psql_conn)
    
    df['start_station_id'] = pd.to_numeric(df['start_station_id'], errors='coerce')
    df['end_station_id'] = pd.to_numeric(df['end_station_id'], errors='coerce')
    df = df[df['start_station_id'].notna()].reset_index(drop=True)
    df = df[df['end_station_id'].notna()].reset_index(drop=True)
    df['start_station_id'] = df['start_station_id'].astype(int)
    df['end_station_id'] = df['end_station_id'].astype(int)
    
    stations = pd.read_sql("SELECT * FROM citibike_stations_geocoded", psql_conn)
    
    geocoded_data = pd.merge(df, stations[['end_station_id', 'BLOCK', 'TRACT']], 
                             left_on='end_station_id', right_on='end_station_id', 
                             how='left', validate='many_to_one')
    
    geocoded_data.to_sql("citibike_rides_geocoded", psql_conn, if_exists="replace", index=False, chunksize=1_000_000, method="multi")

process_ride_data()
geocode_stations()
process_and_geocode_data()