"""
Model & Feature Exploration Streamlit App for web scraped Zillow listing data
"""

import numpy as np
import pandas as pd
from sklearn.metrics import r2_score
import matplotlib.pyplot as plt
import plotly.graph_objects as go
import plotly.express as px
import streamlit as st
from pycaret.regression import *
from explainerdashboard import RegressionExplainer, ExplainerDashboard, ExplainerHub
import time
import socket
import os

from dotenv import load_dotenv
load_dotenv()
from utils.census_geocode_api import fetch_geocode_coordinates, extract_data
GOOGLE_MAPS_API_KEY = os.getenv('GOOGLE_MAPS_API_KEY')

from utils import db_utils
psql_conn = db_utils.get_postgres_conn()

st.set_page_config(layout="wide", page_title="ML Feature Exploration", page_icon="🤖")
st.title("PropertizeAI: ML Feature Exploration")

class ModelHistory:
    def __init__(self, model_hist_save_name='model_hist.csv'):
        self.model_hist_df = pd.DataFrame(columns=['Target', 'R2', 'MSE', 'Excluded Features', 'Model'])
        self.model_hist_save_name = model_hist_save_name
        self.displayDashboardPort = 8088
        self.db = None
        self.db2 = None
    
    def append_row_to_csv(self, dfRowToConcat):
        dfRowToConcat.to_csv(self.model_hist_save_name, header=False, index=False, mode='a')
        self.model_hist_df = pd.concat([self.model_hist_df, dfRowToConcat], ignore_index=True)
                    
    def update_model_hist_df(self, dfRowToConcat):
        self.model_hist_df = pd.concat([self.model_hist_df, dfRowToConcat], ignore_index=True)
        return self.model_hist_df

    def read_model_hist_csv(self):
        return pd.read_csv(self.model_hist_save_name)

modelHistory = ModelHistory()

class ExplainerHub_Class:
    def __init__(self):
        currentMinute = time.strftime("%M")
        if int(currentMinute) > 50: currentMinute = int(currentMinute) + 30
        self.displayDashboardPort = int('80' + str(currentMinute))
        self.portIncrease = 9
        self.hub = None
        self.first_run = True
    
    def is_port_in_use(self, port: int) -> bool:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            return s.connect_ex(('localhost', port)) == 0
        
    def assing_port(self):
        assignPort = self.displayDashboardPort
        while(self.is_port_in_use(assignPort)):
            assignPort += 1
        self.displayDashboardPort = assignPort
    
    def read_dashboard_yaml(self):
        with open("dashboardLaunch.yaml", 'r') as f:
            yaml = f.read()
        return ExplainerDashboard.from_config("explainer.joblib", "dashboardLaunch.yaml")
    
    def create_explainerHub(self):
        db1 = self.read_dashboard_yaml()
        self.hub = ExplainerHub([db1], title='Model Comparison', port=self.displayDashboardPort)
    
    def add_explainer(self, explainer):
        self.hub.add_dashboard(explainer)

    def run_explainerHub(self):
        self.first_run = False
        self.hubRun = self.hub.run(port=self.displayDashboardPort+self.portIncrease)

@st.cache_resource
def initialize_explainerHub():
    explainerHub = ExplainerHub_Class()
    explainerHub.create_explainerHub()
    return explainerHub

explainerHub = initialize_explainerHub()

preprocessed_df = pd.read_csv('preprocessed-NYC-sortedCols.csv')
features = [
    'price', 'zestimate', 'sellingSoon', 'avgPriceChange', 'propertyTaxRateEncoded',
    'arm5BucketRate', 'favoriteCount', 'pageViewCount', 'daysOnZillow', 'hoaFee',
    'yearBuilt', 'homeType', 'bedrooms', 'bathroomsFloat', 'parkingCapacity',
    'avgSchoolRating', 'hasHeating', 'hasGarage', 'zipcodeEncoded', 'latitude', 'longitude'
]

@st.cache_data
def train_multi_model_fn(X_features1, X_features2, predict_col):
    X_features = [feature for feature, selected in {**X_features1, **X_features2}.items() if selected]
    excluded_features = [i for i in features if i not in X_features]
    df_regress = preprocessed_df[X_features]
    reg = setup(data=df_regress, target=predict_col, session_id=123, verbose=False)
    all_models = compare_models(n_select=11)
    results_df = pull()
    best = results_df.iloc[0].to_dict()
    addToModelHist = pd.DataFrame({
        'Target': [predict_col],
        'R2': [best['R2']],
        'MSE': [best['MSE']],
        'Excluded Features': [excluded_features],
        'Model': [best['Model']],
    })
    modelHistory.update_model_hist_df(addToModelHist)
    model_id = f'{predict_col}: R2-{round(best["R2"],3)}:Len-{len(excluded_features)}'
    return results_df, all_models, X_features, addToModelHist, model_id

@st.cache_data
def train_model_fn(X_features1, X_features2, predict_col, model_select):
    X_features = [feature for feature, selected in {**X_features1, **X_features2}.items() if selected]
    df_regress = preprocessed_df[X_features]
    reg = setup(data=df_regress, target=predict_col, session_id=123, verbose=False)
    model = create_model(model_select)
    model.predict_col_ = predict_col
    save_model(model, 'model')
    return model

def plot_model_fn(model, plot_options, plot_select):
    return plot_model(model, plot=plot_options[plot_select], display_format='streamlit')

def plot_prediction_error(model, X_test, y_test):
    fig, ax = plt.subplots()
    y_pred = model.predict(X_test)
    ax.scatter(y_test, y_pred, edgecolors=(0, 0, 0))
    ax.plot([y_test.min(), y_test.max()], [y_test.min(), y_test.max()], 'k--', lw=4)
    ax.set_xlabel('Measured')
    ax.set_ylabel('Predicted')
    ax.set_xlim([y_test.min(), y_test.max()])
    ax.set_ylim([y_test.min(), y_test.max()])
    ax.set_title('Predicted Error')
    r2 = r2_score(y_test, y_pred)
    ax.legend([f'Predicted Error, R2: {r2:.2f}'], loc='upper left')
    return fig

def plot_prediction_error_plotly_dark(model, X_test, y_test):
    import plotly.io as pio
    pio.templates.default = "plotly_dark"
    y_pred = model.predict(X_test)
    fig = px.scatter(x=y_test, y=y_pred, trendline="ols", trendline_color_override="red")
    fig.update_layout(
        title="Predicted Error",
        xaxis_title="True Values",
        yaxis_title="Predictions",
        title_x=0.5,
        autosize=True, 
        margin=dict(l=50, r=50, b=100, t=100, pad=4),
    )
    return fig

st.sidebar.header("Select Features")

with st.sidebar.form("select_features"):
    colSide1, colSide2 = st.columns(2)
    with colSide1:
        X_features1 = {feature: colSide1.checkbox(f"{feature}", value=True) for feature in features[:len(features)//2]}   
    with colSide2:
        X_features2 = {feature: colSide2.checkbox(f"{feature}", True) for feature in features[len(features)//2:]}   
    predict_col = st.selectbox("Select target parameter", features, 2, key='predict_col')
    st.form_submit_button("Submit")

if 'predict_col' not in st.session_state:
    st.session_state['predict_col'] = predict_col
    
st.write(st.session_state['predict_col'])
multi_model_results, all_models, X_features, addToModelHist, model_id = train_multi_model_fn(X_features1, X_features2, predict_col)
save_model(all_models, 'multi_model_results')

if 'X_features' not in st.session_state:
    st.session_state['X_features'] = X_features

multi_model_results = multi_model_results[['Model','R2', 'MAE', 'MSE', 'RMSE', 'RMSLE', 'MAPE', 'TT (Sec)']]
multi_model_results.style.apply(lambda x: ['background: yellow' if x.name == 0 else '' for i in x], axis=1)
st.table(multi_model_results.reset_index(drop=True).head(10))

def change_selected_model(model_select, X_features, predict_col):
    selected_model = create_model(model_select)
    try:
        feature_importances = zip(selected_model.feature_names_in_, selected_model.feature_importances_)
    except: 
        feature_importances = zip(selected_model.feature_name_, selected_model.feature_importances_)
    else: 
        feature_importances = zip(X_features, selected_model.feature_importances_)
    finally:
        feature_importances = sorted(feature_importances, key=lambda x: x[1], reverse=True)
        selected_model.feature_importances_list = feature_importances
    selected_model.predict_col_ = predict_col
    save_model(selected_model, 'currentModel')
    return selected_model

all_models_dict = dict(zip(multi_model_results['Model'], multi_model_results.index))
all_models_dict_nums = dict(enumerate(multi_model_results['Model']))
all_models_dict_nums = {v: k for k, v in all_models_dict_nums.items()}

dropdown_model_select = st.selectbox("Select model to train", list(all_models_dict.keys()), 0 )

selected_model = change_selected_model(all_models_dict[dropdown_model_select], X_features, predict_col)

plot_options = {
    '- Schematic drawing of the preprocessing pipeline': 'pipeline',
    '- Interactive Residual plots': 'residuals_interactive',
    '- Residuals Plot': 'residuals',
    '- Prediction Error Plot': 'error',
    '- Cooks Distance Plot': 'cooks',
    '- Recursive Feat. Selection': 'rfe',
    '- Learning Curve': 'learning',
    '- Validation Curve': 'vc',
    '- Manifold Learning': 'manifold',
    '- Feature Importance': 'feature',
    '- Feature Importance (All)': 'feature_all',
    '- Model Hyperparameter': 'parameter',
    '- Decision Tree': 'tree'
}

model = load_model('model')

def save_model_proc():
    user_input = st.sidebar.text_input('Please insert the a save name for the model', key='text_key', value='Save Name')
    if user_input is not None:
        if st.sidebar.button('Save Current Model'):
            save_model(model, user_input)
            st.sidebar.success('Model Saved')
        
save_model_proc() 

def plot_feature_importances_plotly(model):
    feature_importance_list = model.feature_importances_list
    X = [x[1] for x in feature_importance_list][::-1]
    y = [x[0] for x in feature_importance_list][::-1]
    fig = go.Figure(go.Bar(x=X, y=y, orientation='h'))
    fig.update_layout(title="Model Feature Weights", autosize=True)
    return fig

X_test = get_config('X_test')
y_test = get_config('y_test')

col1, col2 = st.columns(2)
with col1:
    plot_select1 = st.selectbox("Select plot 1", list(plot_options.keys()), 3)
    if list(plot_options.keys()).index(plot_select1) == 3:
        st.write(plot_prediction_error(selected_model, X_test, y_test))
    else:
        plot_model_fn(selected_model, plot_options, plot_select1)
with col2:
    plot_select2 = st.selectbox("Select plot 2", list(plot_options.keys()), 4)
    plot_model_fn(selected_model, plot_options, plot_select2)

col1, col2 = st.columns(2)
with col1:
    plot_select3 = st.selectbox("Select plot 3", list(plot_options.keys()), 7)
    plot_model_fn(selected_model, plot_options, plot_select3)
with col2:
    plot_select4 = st.selectbox("Select plot 4", list(plot_options.keys()), 6)
    plot_model_fn(selected_model,plot_options, plot_select4)

allowed_interpret = ['lightgbm','rf','dt']
allow_interpret = all_models_dict[dropdown_model_select] in allowed_interpret

feature_importance_plot = plot_feature_importances_plotly(selected_model)

col1, col2 = st.columns(2, gap='small')
with col1:
    st.subheader("Residuals Distribution Plot")
    plot_model_fn(selected_model, plot_options, '- Residuals Plot')
with col2:
    st.subheader("Feature Correlation Plot")
    if allow_interpret:
        try: 
            st.pyplot(interpret_model(selected_model, plot='correlation'), clear_figure=True)
        except: 
            pass

col1, col2 = st.columns(2)
with col1:
    st.subheader("Feature Weights/Explained Variance")
    if allow_interpret:
        try:
            shap_msa = interpret_model(selected_model, plot='msa')
            st.plotly_chart(feature_importance_plot, clear_figure=True)
            st.plotly_chart(shap_msa, clear_figure=True)
        except:
            st.plotly_chart(feature_importance_plot, clear_figure=True)
with col2:
    st.subheader('SHAP Summary Plot')
    
if allow_interpret:
        st.pyplot(interpret_model(selected_model), clear_figure=True)
    else:
        st.plotly_chart(plot_feature_importances_plotly(selected_model), clear_figure=True)

with st.expander("Model History"):
    try:
        st.table(modelHistory.read_model_hist_csv())
    except:
        st.write('No model history to display')

X_test = get_config('X_test')
y_test = get_config('y_test')

def update_explainerDashboard(selected_model, X_test, y_test, title):
    try:
        modelHistory.db2.terminate(port=modelHistory.displayDashboardPort)
        modelHistory.db.terminate(port=modelHistory.displayDashboardPort)
    except:
        pass
    explainer = RegressionExplainer(selected_model, X_test, y_test)
    explainer.dump("explainer.joblib")
    ExplainerDashboard(explainer, title='Dashboard Title', mode='dash', shap_interaction=False).to_yaml("dashboardLaunch.yaml", explainerfile='explainer.joblib')
    
    db = ExplainerDashboard(explainer, title=title, mode='inline', shap_interaction=False)
    explainerHub.add_explainer(db)
    st.write('stored')
    return db

title = st.text_input('Enter a title for the dashboard', key='title', placeholder='DashboardTitle')

def firstTimeUpload(_selected_model, X_test, y_test, title):
    if explainerHub.first_run:
        update_explainerDashboard(_selected_model, X_test, y_test, title=title)
        dashboardurl = f'http://127.0.0.1:{explainerHub.displayDashboardPort+explainerHub.portIncrease}/'
        st.components.v1.iframe(dashboardurl, width=None, height=900, scrolling=True)
        explainerHub.run_explainerHub()

@st.cache_resource
def display_dashboard_Hub():
    dashboardurl = f'http://127.0.0.1:{explainerHub.displayDashboardPort+explainerHub.portIncrease}/'
    st.components.v1.iframe(dashboardurl, width=None, height=900, scrolling=True)

if st.button("Add Model to Hub"):
    modelHistory.append_row_to_csv(addToModelHist)
    if title == '':
        title = 'model'
    update_explainerDashboard(selected_model, X_test, y_test, title=title)
    if not explainerHub.first_run:
        display_dashboard_Hub()
    st.success('Dashboard Added')

firstTimeUpload(selected_model, X_test, y_test, title=model_id)
if not explainerHub.first_run:
    display_dashboard_Hub()