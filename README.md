

# Urban Dynamics and Real Estate: Improving Market Forecasts with Non-Traditional Data


**All code authored by:** Darien Nouri

The associated report pdf can be found at this [link](https://general-scratch.s3.amazonaws.com/Improving_Market_Forecasts_with_Nontraditional_Data.pdf) or in this repository as [Improving Market Forecasts with Non-Traditional Data](Improving_Market_Forecasts_with_Nontraditional_Data.pdf). The report is pending submission.

## Project Overview

This repo contains relevant project based code with respect to data collection, exploratory analysis, and modeling. The central goal of the related project was to identify and evaluate the efficacy of alternative data sources in predicting real estate market trends.

### Data Collection

- [01_web_scraper/scraper](01_web_scraper/scraper):  Contains a web scraper for Zillow listings, capable of scraping 1M+ listings per day with the right rotating proxy service. Loads data to MongoDB.

- [aggregate_and_merge_all_sources_to_db.py](02_data_collection/aggregate_and_merge_all_sources_to_db.py):  Preprocesses and aggregates alternative data sources. Loads data from S3, standardizes date formats, groups by census tract and time periods, and merges with sales data. Outputs to PostgreSQL.

- [citibike_ride_data_collection_and_geocoding.py](02_data_collection/citibike_ride_data_collection_and_geocoding.py):  Obtains, preprocesses, and geocodes Citi Bike historical ride data (totaling 500m+ rows) from a public S3 bucket containing zip files with monthly CSVs. Uses Dask to read the CSVs async. Then obtains census-level geocoding using Google Maps API and Census API.

- [nyc_property_sales_etl_script.py](02_data_collection/nyc_property_sales_etl_script.py):  Processes and geocodes NYC property sales data from Excel files, combining data from multiple boroughs and years.


### SQL Queries

- [nyc_alt_data_analysis.sql](sql_queries/nyc_alt_data_analysis.sql):Contains SQL queries related to SQL-side eda and feature engineering.

- [create_nyc_alt_data_daily_mv.sql](sql_queries/create_nyc_alt_data_daily_mv.sql):  Creates a materialized view `nyc_alt_data_daily` by joining the alternative data sources at the daily frequency.

- [join_datasets_daily.sql](sql_queries/join_datasets_daily.sql) and [join_datasets_weekly.sql](sql_queries/join_datasets_weekly.sql):  Queries for joining the alternative data sources, then upsampling to daily and weekly frequencies.

- [date_field_optimization_and_indexing.sql](sql_queries/date_field_optimization_and_indexing.sql):  Creates indices across the date fields in the alternative data sources for optimized joins.

- [reit_etf_daily_datastream_from_wrds.sql](sql_queries/reit_etf_daily_datastream_from_wrds.sql):  Queries data related to REITs from the Wharton Research Data Services (WRDS) database.


### Exploratory Analysis

- [alt_data_census_level_analysis.py](03_exploratory_analysis/alt_data_census_level_analysis.py):  Analyzes alternative data sources (complaints, evictions, restaurants, operating businesses) across census tracts. Not viable, sparsity issues across census tracts.

- [alt_data_market_level_analysis.ipynb](03_exploratory_analysis/alt_data_market_level_analysis.ipynb):  Analyzes the efficacy of alternative data sources in predicting real estate index prices (average home values). Bulk of analysis.
  - Web app render: [feature_exploration_streamlit_app.pdf](03_exploratory_analysis/feature_exploration_streamlit_app.pdf)

- [feature_exploration_streamlit_app.py](03_exploratory_analysis/feature_exploration_streamlit_app.py):A Streamlit app for exploring and visualizing models/features related to the web-scraped Zillow listing data.


### Modeling
- [preliminary_modeling_nyc_zillow_scraped_data.ipynb](04_modeling/preliminary_modeling_nyc_zillow_scraped_data.ipynb):
Initial eda and attempt to reverse engineer Zillow's 'Zestimate' and 'sellingSoon' parameters, both of which have an associated 'model_id' and associated probability in the payload of each listing site.

- [alt_data_predictive_efficacy_on_reits.py](03_exploratory_analysis/alt_data_predictive_efficacy_on_reits.py):
Trains and compares LSTM models with and without alternative data for forecasting reits evaluating the impact of alternative data on model performance. in hindsight: Poor approach, no reason to use LSTM for this task.


``` 
├── 01_web_scraper
│   ├── scraper  
│   │   ├── __init__.py  
│   │   ├── crawler.py  
│   │   ├── data_model_entities.py  
│   │   └── listing_parser.py  
│   ├── run_scraper.py  
│   └── sample_mongo_web_scrape_item.json  
├── 02_data_collection  
│   ├── aggregate_and_merge_all_sources_to_db.py  
│   ├── citibike_ride_data_collection_and_geocoding.py  
│   ├── load_mongodb_scraped_data.ipynb  
│   └── nyc_property_sales_etl_script.py  
├── 03_exploratory_analysis  
│   ├── alt_data_census_level_analysis.py  
│   ├── alt_data_predictive_efficacy_on_re_market.ipynb  
│   ├── alt_data_predictive_efficacy_on_reits.py  
│   ├── feature_exploration_streamlit_app.pdf  
│   └── feature_exploration_streamlit_app.py  
├── 04_modeling  
│   ├── preliminary_modeling_nyc_zillow_scraped_data.ipynb  
│   └── zillow_real_estate_prediction_models.py  
├── sql_queries  
│   ├── create_nyc_alt_data_daily_mv.sql  
│   ├── date_field_optimization_and_indexing.sql  
│   ├── join_datasets_daily.sql  
│   ├── join_datasets_weekly.sql  
│   ├── nyc_alt_data_analysis.sql  
│   └── reit_etf_daily_datastream_from_wrds.sql  
├── utils  
│   ├── __init__.py  
│   ├── census_geocode_api.py  
│   ├── column_enums.py  
│   ├── db_utils.py  
│   ├── jupyter_dash.py  
│   ├── python_query_defs.py  
│   ├── time_series_utils.py  
│   └── visualizations.py  
└── README.md
```
