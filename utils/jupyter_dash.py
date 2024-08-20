import plotly.graph_objects as go
import dash
import dash_core_components as dcc
import dash_html_components as html
from jupyter_dash import JupyterDash

def run_dash_server(app):

    app.run_server(mode='external', port=8050)
