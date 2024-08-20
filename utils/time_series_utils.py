"""
Purpose: Custom models and utility functions for ts analysis and preprocessing
"""

import itertools
import numpy as np
import pandas as pd
import xgboost as xgb
import seaborn as sns
import plotly.express as px
import matplotlib.pyplot as plt
from typing import Dict, List, Tuple, Union
from sklearn.preprocessing import MinMaxScaler
from sklearn.model_selection import train_test_split
from statsmodels.tsa.stattools import adfuller, grangercausalitytests
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score



def set_range_nan(df: pd.DataFrame, start: str, end: str, col: str) -> None:
    if col not in df.columns:
        return
    df.loc[(df['date'] >= start) & (df['date'] <= end), col] = np.nan

def nullify_ranges_with_variability(df: pd.DataFrame) -> pd.DataFrame:
    set_range_nan(df, '2019-11-01', '2022-01-01', 'evictions')
    set_range_nan(df, '2020-10-01', '2021-08-01', 'avg_health_inspection')
    set_range_nan(df, '2020-10-01', '2021-08-01', 'total_inspections')
    set_range_nan(df, '2020-10-01', '2020-11-01', 'new_businesses')
    set_range_nan(df, '2021-10-01', '2022-02-01', 'complaints')
    set_range_nan(df, '2022-05-01', '2022-09-01', 'citibike_2nd_diff')
    set_range_nan(df, '2021-01-15', '2021-05-01', 'citibike_2nd_diff')
    set_range_nan(df, '2020-01-01', '2022-05-01', 'num_arrests')
    
    df.loc[df['date'] < '2020-11', 'new_restaurants'] = np.nan
    return df

def normalize_df(df: pd.DataFrame, start_date: str = '2018-01', date_col: str = 'date') -> pd.DataFrame:
    df_norm = df[df[date_col] > start_date].set_index(date_col)
    df_norm = (df_norm - df_norm.mean()) / df_norm.std()
    return df_norm.reset_index()

def preprocess_df(df: pd.DataFrame, start_date: str = '2016-01', RESAMPLE_FREQ: str = None, verbose: bool = True) -> pd.DataFrame:
    df = nullify_ranges_with_variability(df)
    if RESAMPLE_FREQ:
        df = resample_ts(df, freq=RESAMPLE_FREQ)
    df = make_columns_stationary(df, verbose=False)
    df = normalize_df(df, start_date=start_date, date_col='date')
    if verbose: 
        print(df.head(2))
    return df

def upsample_ts(df: pd.DataFrame, freq: str = 'D', date_col: str = 'date') -> pd.DataFrame:
    return df.set_index(date_col).resample(freq).mean().reset_index()

def resample_ts(df: pd.DataFrame, freq: str = 'D', date_col: str = 'date') -> pd.DataFrame:
    return df.set_index(date_col).resample(freq).mean().reset_index()

def plotall(data: pd.DataFrame, date_col: str = 'date', verbose: bool = True, **kwargs):
    if data.index.dtype != 'datetime64[ns]':
        data = data.set_index(date_col)
    if not data.apply(lambda x: x.min() >= 0 and x.max() <= 1).all():
        scaler = MinMaxScaler()
        data_normalized = pd.DataFrame(scaler.fit_transform(data), columns=data.columns)
    else:
        data_normalized = data
    
    fig = px.line(data_normalized, x=data.index, y=data_normalized.columns, **kwargs)
    fig.update_traces(selector=dict(name='avg_price'), line=dict(dash='solid', width=1.8, color='black'), opacity=.8)
    fig.for_each_trace(lambda t: t.update(visible="legendonly") if t.name not in list(data_normalized.columns)[:2] else ())
    fig.for_each_trace(lambda t: t.update(opacity=.9, line=dict(width=1.8)) if t.name not in list(data_normalized.columns)[:1] else ())
    fig.update_layout(template='ggplot2', xaxis=dict(showgrid=True, gridwidth=1, dtick=.5))
    
    fig.show()
    return fig if verbose else None

def make_columns_stationary(df: pd.DataFrame, verbose: bool = True) -> pd.DataFrame:
    df = df.copy()
    if 'citibike_rides' in df.columns:
        df['citibike_rides'] = df['citibike_rides'].diff()
    stationary_df = df.copy()
    for column in df.columns:
        if column != 'date':
            series = df[column]
            is_stationary = adfuller(series.dropna(inplace=False))[1] < 0.05

            if not is_stationary:
                if verbose:
                    print(f'{column} is not stationary')
                order = 1
                while not is_stationary:
                    if verbose:
                        print(f'{column} requires order {order} differencing')
                    series = series.diff()
                    is_stationary = adfuller(series.dropna(inplace=False))[1] < 0.05
                    order += 1
                
            stationary_df[column] = series

    return stationary_df

def granger_causality_test(data: pd.DataFrame, max_lag: int, target_col: str = 'avg_price', 
                           date_col: str = 'date', agg_lags: int = 1, 
                           alpha: float = 0.05,
                           verbose: bool = True, return_df: bool = False, debug: bool = False, **kwargs) -> Union[pd.DataFrame, Tuple[pd.DataFrame, pd.DataFrame]]:
    """Perform Granger causality test"""
    columns = [col for col in list(data.columns) if col != date_col]
    results = {}
    
    for col in [col for col in columns if col != target_col]:
        df_temp = data[[col, target_col]].dropna()
        df_temp = df_temp.groupby(np.arange(len(df_temp))//agg_lags).mean()
        
        try:
            granger_test = grangercausalitytests(df_temp, maxlag=max_lag+1, verbose=False)
        except Exception as e:
            if debug:
                print(f'Error with {col}, {e}')
            columns.remove(col)
            continue
        
        results[col] = [granger_test[i][0]['ssr_ftest'][1] for i in range(1, max_lag+1)]
    
    causality_results = pd.DataFrame.from_dict(results, orient='index', columns=[f'Lag {i*agg_lags}' for i in range(1, max_lag+1)])
    if debug:
        a = set(causality_results.index)
        b = set(columns)
        print((a - b).union(b - a))
        print(causality_results.index)
        print(columns)
    columns.remove(target_col)
    causality_results.index = columns
    causality_results = causality_results.round(6)
    
    if verbose:
        causality_results_style = causality_results.style.apply(lambda x: ["background: red" if v < alpha else "" for v in x], axis=1).set_caption('Granger Causality Test Results')
    
    return causality_results_style, causality_results if return_df else causality_results_style

def process_sliding_window(df: pd.DataFrame, RESAMPLE_FREQ: str, START_DATE: str, MAX_LAG: int, verbose: bool = True) -> Tuple[pd.DataFrame, Tuple[Dict[str, pd.Timestamp], pd.DataFrame]]:
    """Process sliding window analysis using Granger causality test"""
    df_resampled = preprocess_df(df.copy(), start_date=START_DATE, RESAMPLE_FREQ=RESAMPLE_FREQ, verbose=False)
    
    raw_sliding_window_results = []
    sliding_window_results_above_95 = []
    optimal_start_time_per_feature = {}
    optimal_results = []
    
    for time_step in df_resampled['date']:
        df_temp = df_resampled[df_resampled['date'] >= time_step]
        
        granger_results = granger_causality_test(df_temp, max_lag=MAX_LAG, target_col='avg_price', verbose=True, return_df=True)
        
        raw_sliding_window_results.append(granger_results[1].iloc[:,:MAX_LAG].mean(axis=1).to_dict())
        sliding_window_results_above_95.append((granger_results[1].iloc[:,:MAX_LAG] < 0.05).sum(axis=1).to_dict())
        
    optimal_start_time_per_feature = pd.DataFrame(raw_sliding_window_results, index=df_resampled['date']).idxmin(axis=0, skipna=True).to_dict()
        
    for feature, start_time in optimal_start_time_per_feature.items():
        df_temp = df_resampled[df_resampled['date'] >= start_time]
        granger_results = granger_causality_test(df_temp, max_lag=MAX_LAG, target_col='avg_price', verbose=True, return_df=True)
        optimal_results.append(granger_results[1].loc[feature])
    
    df_optimal_results = pd.DataFrame(optimal_results, index=optimal_start_time_per_feature.keys())
    
    if verbose:
        df_results_style = df_optimal_results.style.apply(lambda x: ["background: red" if v <= 0.06 else "" for v in x], axis=1).set_caption('Granger Causality Test Results')
        print(df_results_style)
        fig = px.line(df_optimal_results.T, title=f'Granger Results of Optimal Date Range Per Feature ({RESAMPLE_FREQ} Lag Interval)')
        fig.update_layout(yaxis_title='p-value', xaxis_title='Lag')
        fig.show()
        plotall(df_resampled, title=f'Normalized Data ({RESAMPLE_FREQ} Lag Interval)')
    
    return df_resampled, (optimal_start_time_per_feature, df_optimal_results)


def optimize_shifts(df, shifts, train_model, evaluate):
    cols = ["target_ci", "target_citi", "target_op", "target_ev", "target_hi"]
    best_mae = np.inf
    best_shift = None
    shift_values = range(1, 8)
    all_shifts = list(itertools.product(shift_values, repeat=4))
    for shift in all_shifts:
        shift = list(shift)
        shift.append(1)
        shifts = dict(zip(shifts.keys(), shift))
        data = df.copy()
        for col, shift in shifts.items():
            data[col] = data[col].shift(shift)
        data = data.dropna()
        data = data.drop(columns=["sales_count"])
        X_test, y_test, y_pred, model = train_model(data)
        mse, mae, r2 = evaluate_model(y_test, y_pred)

        if mae < best_mae:

            best_mae = mae
            best_shift = shifts
        print("Best Shift", best_shift, best_mae)
    print("Best Shifts: ", best_shift)

    return best_shift


def train_model(df):
    X = df.drop(["avg_sales"], axis=1)
    y = df["avg_sales"]
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )
    model = xgb.XGBRegressor(
        objective="reg:squarederror", learning_rate=0.1, max_depth=5, n_estimators=100
    )
    model.fit(X_train, y_train)
    y_pred = model.predict(X_test)
    return X_test, y_test, y_pred, model


def evaluate_model(y_test, y_pred):
    mse = mean_squared_error(y_test, y_pred)
    mae = mean_absolute_error(y_test, y_pred)
    r2 = r2_score(y_test, y_pred)
    mape = np.mean(np.abs((y_test - y_pred) / y_test)) * 100
    print(f"\nMAE: {mae:.3f}\nMAPE: {mape:.3f}\nR2: {r2:.3f}")

    return mse, mae, r2


def plot_data(X_test, y_test, y_pred):
    plt.figure(figsize=(10, 6))
    plt.scatter(X_test.index, y_test, color="blue", label="Actual")
    plt.scatter(X_test.index, y_pred, color="red", label="Predicted")
    plt.title("Actual vs Predicted Values")
    plt.xlabel("Index")
    plt.ylabel("Sales")
    plt.legend()
    plt.show()


def plot_importance(model, feature_names):
    xgb.plot_importance(model)
    plt.title("Feature Importance Group")
    plt.show()
    

def plot_importance_enhanced(model):

    importance_df = feature_importances(model)
    fig, ax = plt.subplots(figsize=(8, 6))
    sns.barplot(
        x="Importance", y="Feature", data=importance_df, ax=ax, palette="viridis"
    )

    for i, importance in enumerate(importance_df["Importance"]):
        ax.annotate(
            f"{importance:.2f}",
            xy=(importance, i),
            va="center",
            ha="left",
            fontweight="bold",
        )

    ax.set_xlabel("Importance")
    sns.despine(left=True, bottom=True)

    ax.grid(False)
    ax.spines["left"].set_visible(False)
    ax.spines["bottom"].set_visible(False)

    ax.set_title("Feature Importance", size=20)
    fig.tight_layout()
    plt.show()


def feature_importances(model):
    feature_important = model.get_booster().get_score(importance_type="weight")
    names = list(feature_important.keys())
    values = list(feature_important.values())

    feature_importance_df = pd.DataFrame(
        {"Feature": names, "Importance": values}
    ).sort_values(by="Importance", ascending=False)

    feature_importance_df
    return feature_importance_df
