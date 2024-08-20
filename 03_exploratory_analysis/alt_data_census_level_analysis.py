# alt_data_census_level_analysis.py
"""
Purpose: Analyze alternative data sources (complaints, evictions, restaurants, operating businesses) 
across census tracts. 
"""

# %%
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from statsmodels.tsa.stattools import grangercausalitytests

import seaborn as sns
import matplotlib.pyplot as plt
from sklearn.preprocessing import MinMaxScaler

pd.set_option('display.max_columns', None)
px.defaults.template = "plotly_dark"

# %% [markdown]
# # Load and Preprocess Data j
# 
# note: pull alt data sources, group by census tract, merge w/ sales data

# %%
def load_and_preprocess(file_path):
    df = pd.read_csv(file_path)
    df['Date'] = pd.to_datetime(df['Date'])
    df['Year'] = df['Date'].dt.year
    df['yr-month'] = df['Year'].astype(str) + '-' + df['Date'].dt.month.astype(str)
    df['tract_1000_grp'] = pd.cut(df['tract'], bins=range(0, 303000, 10000), right=True, labels=False) + 1
    return df
# Load data from S3
DATA_DIR = 's3://general-scratch/alt_data'

sales = pd.read_csv(f'{DATA_DIR}/All_Boroughs_geocoded_With_2023.csv')
complaints = pd.read_csv(f"{DATA_DIR}/DOB_Complaints_Geocoded.csv")
operating_businesses = pd.read_csv('{DATA_DIR}/Businesses_Operating_Geocoded.csv')
evictions = pd.read_csv(f'{DATA_DIR}/Evictions_Geocoded.csv')
restaurants = pd.read_csv(f'{DATA_DIR}/Restaurants_Geocoded.csv')
health_inspections = pd.read_csv(f'{DATA_DIR}/DOHMH_New_York_City_Restaurant_Inspection_Results.csv')


# %%

def group_by_month_tract(df):
    return df.groupby(['yr-month', 'tract_1000_grp'], as_index=False).agg({'tract': 'count'}).sort_values(by=['yr-month'])

sales_month_grp = group_by_month_tract(sales)
complaints_month_grp = group_by_month_tract(complaints)
evictions_month_grp = group_by_month_tract(evictions)
restaurants_month_grp = group_by_month_tract(restaurants)
operating_businesses_month_grp = group_by_month_tract(operating_businesses)

# %%
def merge_with_sales(alt_data, sales_data):
    return pd.merge(sales_data, alt_data, how='left', on=['yr-month', 'tract_1000_grp'])

sales_complaints = merge_with_sales(complaints_month_grp, sales_month_grp)
sales_evictions = merge_with_sales(evictions_month_grp, sales_month_grp)
sales_restaurants = merge_with_sales(restaurants_month_grp, sales_month_grp)
sales_operating_businesses = merge_with_sales(operating_businesses_month_grp, sales_month_grp)

# %% [markdown]
# # Exploratory Data Analysis
# 
# note: visualize relationships btwn alt data and sales across tracts

# %%
def plot_time_series(data, dataset_name):
    fig, axes = plt.subplots(3, 2, figsize=(15, 15))
    fig.suptitle(f'{dataset_name} Count and Average Sale Values per Census Tract Time Series')
    
    for i, ax in enumerate(axes.flatten()):
        tract = i + 1
        tract_data = data[data['tract_1000_grp'] == float(tract)]
        tract_data = (tract_data - tract_data.mean()) / tract_data.std()
        
        sns.lineplot(x=tract_data['yr-month'], y=tract_data['tract'], color="red", ax=ax, label=dataset_name)
        sns.lineplot(x=tract_data['yr-month'], y=tract_data['SALE_PRICE'], color="green", ax=ax, label="Avg Market Sale Price")
        
        ax.set_title(f"Tract Group {tract}")
        ax.set_ylabel("Normalized Value")
        ax.set_xlabel("")
        ax.tick_params(axis='x', rotation=45)
    
    plt.tight_layout()
    plt.show()

plot_time_series(sales_complaints, "Complaints")
plot_time_series(sales_evictions, "Evictions")
plot_time_series(sales_restaurants, "Restaurants")
plot_time_series(sales_operating_businesses, "Operating Businesses")

# %% [markdown]
# # Granger Causality Analysis
# 
# note: check if alt data helps predict sales, look at diff lags

# %%
def run_granger_tests(data, max_lag=10):
    results = {}
    for tract in range(1, 7):
        tract_data = data[data['tract_1000_grp'] == float(tract)][['tract', 'SALE_PRICE']]
        tract_data = (tract_data - tract_data.mean()) / tract_data.std()
        
        try:
            granger_results = grangercausalitytests(tract_data, maxlag=max_lag, verbose=False)
            results[tract] = [granger_results[i+1][0]['ssr_ftest'][1] for i in range(max_lag)]
        except:
            results[tract] = [np.nan] * max_lag
    
    return pd.DataFrame(results, index=[f'Lag {i}' for i in range(1, max_lag+1)]).T

complaints_granger = run_granger_tests(sales_complaints)
evictions_granger = run_granger_tests(sales_evictions)
restaurants_granger = run_granger_tests(sales_restaurants)
businesses_granger = run_granger_tests(sales_operating_businesses)

# %%
def display_granger_results(results, title):
    styled = results.style.applymap(lambda x: 'background-color: yellow' if x < 0.05 else '')
    styled.set_caption(f'Granger Causality P-Values for {title} and Sales')
    display(styled)

display_granger_results(complaints_granger, "Complaints")
display_granger_results(evictions_granger, "Evictions")
display_granger_results(restaurants_granger, "Restaurants")
display_granger_results(businesses_granger, "Operating Businesses")

# %% [markdown]
# # Causal Impact Analysis
# 
# note: deeper look at causal effects, use pre/post periods

# %%
def run_causal_impact(data, intervention_point=0.7, tract=1.0):
    tract_data = data[data['tract_1000_grp'] == float(tract)][['tract', 'SALE_PRICE']]
    tract_data = (tract_data - tract_data.mean()) / tract_data.std()
    tract_data.index = pd.to_datetime(data['yr-month'])
    
    pre_period = [tract_data.index[0], tract_data.index[int(len(tract_data) * intervention_point)]]
    post_period = [tract_data.index[int(len(tract_data) * intervention_point) + 1], tract_data.index[-1]]
    
    ci = CausalImpact(tract_data, pre_period, post_period)
    print(ci.summary())
    ci.plot()

run_causal_impact(sales_complaints, tract=3.0)
run_causal_impact(sales_evictions, tract=3.0)
run_causal_impact(sales_restaurants, tract=3.0)
run_causal_impact(sales_operating_businesses, tract=3.0)

# %% [markdown]
# # forecasting
# 
# note: try basic forecasting, maybe arima or fbProphet

# %%
from statsmodels.tsa.stattools import adfuller
from statsmodels.tsa.arima.model import ARIMA

def test_stationarity(data):
    result = adfuller(data)
    print(f'ADF Statistic: {result[0]}')
    print(f'p-value: {result[1]}')

def fit_arima(data):
    model = ARIMA(data, order=(1,1,1))
    results = model.fit()
    print(results.summary())
    
    forecast = results.forecast(steps=12)
    plt.figure(figsize=(10,5))
    plt.plot(data.index, data, label='Observed')
    plt.plot(forecast.index, forecast, color='r', label='Forecast')
    plt.legend()
    plt.show()

# Example for complaints data
complaints_data = sales_complaints[sales_complaints['tract_1000_grp'] == 1.0].set_index('yr-month')['SALE_PRICE']
test_stationarity(complaints_data)
fit_arima(complaints_data)
# %%
