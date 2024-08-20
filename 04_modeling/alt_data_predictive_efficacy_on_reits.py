"""
Purpose: Train and compare LSTM models with and without alt data
in forecasting reit etfs, evaluating their impact on model performance.
"""

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.preprocessing import MinMaxScaler
from sklearn.metrics import mean_squared_error, mean_absolute_error
from keras.models import Sequential
from keras.layers import LSTM, Dense, Dropout
from keras.optimizers import Adam
from keras.callbacks import EarlyStopping

class ETFPredictionModel:
    def __init__(self, df, alt_features, base_features, etfs, look_back=1, epochs=100, batch_size=32, split_ratio=0.8):
        self.df = df
        self.alt_features = alt_features
        self.base_features = base_features
        self.etfs = etfs
        self.look_back = look_back
        self.epochs = epochs
        self.batch_size = batch_size
        self.split_ratio = split_ratio

    def prepare_data(self, features, target):
        dataset = self.df[features + [target]].values
        scaler = MinMaxScaler(feature_range=(0, 1))
        dataset = scaler.fit_transform(dataset)
        
        X, y = [], []
        for i in range(len(dataset) - self.look_back):
            X.append(dataset[i:(i + self.look_back), :-1])
            y.append(dataset[i + self.look_back, -1])
        return np.array(X), np.array(y), scaler

    def create_lstm_model(self, input_shape):
        model = Sequential([
            LSTM(64, input_shape=input_shape, return_sequences=True),
            Dropout(0.2),
            LSTM(32),
            Dropout(0.2),
            Dense(1)
        ])
        model.compile(optimizer=Adam(learning_rate=0.001), loss='mean_squared_error')
        return model

    def train_lstm(self, X, y):
        train_size = int(len(X) * self.split_ratio)
        X_train, X_test = X[:train_size], X[train_size:]
        y_train, y_test = y[:train_size], y[train_size:]
        
        model = self.create_lstm_model((X.shape[1], X.shape[2]))
        early_stop = EarlyStopping(monitor='val_loss', patience=10, restore_best_weights=True)
        
        model.fit(X_train, y_train, validation_split=0.2, epochs=self.epochs, 
                  batch_size=self.batch_size, callbacks=[early_stop], verbose=0)
        
        train_predict = model.predict(X_train)
        test_predict = model.predict(X_test)
        
        return train_predict, test_predict, y_train, y_test

    def evaluate_model(self, y_true, y_pred):
        mse = mean_squared_error(y_true, y_pred)
        rmse = np.sqrt(mse)
        mae = mean_absolute_error(y_true, y_pred)
        smape = 100/len(y_true) * np.sum(2 * np.abs(y_pred - y_true) / (np.abs(y_true) + np.abs(y_pred)))
        
        return {
            "MSE": mse,
            "RMSE": rmse,
            "MAE": mae,
            "SMAPE": smape
        }

    def get_model_predictions(self, dates, y_true, y_pred, train_size):
        df = pd.DataFrame({
            'Date': dates,
            'Actual': y_true,
            'Predicted': y_pred
        })
        df.set_index('Date', inplace=True)
        df['Train'] = df['Predicted'][:train_size]
        df['Test'] = df['Predicted'][train_size:]
        return df

    def run_experiment(self):
        """Run experiment comparing models with and without alternative data."""
        results_treatment = []
        results_control = []
        model_predictions = []

        for etf in self.etfs:
            print(f"Processing ETF: {etf}")
            
            # Model with alternative data
            X, y, scaler = self.prepare_data(self.alt_features + [etf], etf)
            train_pred, test_pred, y_train, y_test = self.train_lstm(X, y)
            y_pred = np.concatenate([train_pred, test_pred])
            y_true = scaler.inverse_transform(np.concatenate([y_train, y_test]).reshape(-1, 1))[:, 0]
            y_pred = scaler.inverse_transform(y_pred)[:, 0]
            results = self.evaluate_model(y_true, y_pred)
            results_treatment.append(results)
            model_predictions.append(self.get_model_predictions(self.df['date'], y_true, y_pred, len(train_pred)))
            
            # Model without alternative data
            X, y, scaler = self.prepare_data(self.base_features + [etf], etf)
            train_pred, test_pred, y_train, y_test = self.train_lstm(X, y)
            y_pred = np.concatenate([train_pred, test_pred])
            y_true = scaler.inverse_transform(np.concatenate([y_train, y_test]).reshape(-1, 1))[:, 0]
            y_pred = scaler.inverse_transform(y_pred)[:, 0]
            results = self.evaluate_model(y_true, y_pred)
            results_control.append(results)

        return results_treatment, results_control, model_predictions

def plot_results(control):
    fig, ax = plt.subplots(figsize=(12, 6))
    ax.bar(control.index, control['Difference'], color='#DC143C')
    ax.set_title('SMAPE % Difference Between Base and Full Model', size=18)
    ax.set_ylabel('SMAPE % Difference', size=14)
    ax.set_xlabel('ETF', size=14)
    ax.set_yticklabels([f'{i}%' for i in ax.get_yticks()], size=12)
    fig.tight_layout()
    plt.show()

# Load data and run experiment
df = pd.read_csv('data_with_etfs.csv')
alt_features = ['avg_sales', 'sales_count', 'target_ci', 'target_citi', 'target_op', 'target_ev', 'target_hi']
base_features = ['avg_sales']
etfs = ['VNQ', 'MORT', 'REM', 'KBWY', 'RWR', 'ICF', 'SCHH', 'IYR', 'USRT', 'REET']

model = ETFPredictionModel(df, alt_features, base_features, etfs)
results_treatment, results_control, model_predictions = model.run_experiment()

# Create comparison df
etf_lstm_results_treatment = [{etf: results['SMAPE']} for etf, results in zip(etfs, results_treatment)]
etf_lstm_results_control = [{etf: results['SMAPE']} for etf, results in zip(etfs, results_control)]

etf_lstm_results_treatment = {k: v for d in etf_lstm_results_treatment for k, v in d.items()}
etf_lstm_results_control = {k: v for d in etf_lstm_results_control for k, v in d.items()}


control = pd.DataFrame.from_dict(etf_lstm_results_control, orient='index', columns=['Control'])
control['Treatment'] = pd.DataFrame.from_dict(etf_lstm_results_treatment, orient='index', columns=['SMAPE'])
control['Difference'] = control['Treatment'] - control['Control']
control = control.sort_values(by='Difference', ascending=False)

plot_results(control)
print(control)


base_mae = [d['MAE'] for d in results_control]
treatment_mae = [d['MAE'] for d in results_treatment]
compare = pd.DataFrame([base_mae, treatment_mae], index=['control', 'treatment']).T
compare['difference'] = compare['control'] - compare['treatment']
print(compare)

# Calculate average difference and percent difference
avg_difference = compare['difference'].mean()
tmean = compare['treatment'].mean()
cmean = compare['control'].mean()
percent_difference = (cmean - tmean) / cmean * 100

print(f"Average MAE difference: {avg_difference}")
print(f"Average percent difference: {percent_difference}%")
print(f"Treatment mean: {tmean}, Control mean: {cmean}")