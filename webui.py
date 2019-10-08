import pandas as pd
import dash
import plotly.graph_objs as go
import dash_core_components as dcc
import dash_html_components as html
import dash_table_experiments as dt
from plotly.graph_objs import *
from dash.dependencies import Input, Output, State
import pandas.io.sql as psql
import psycopg2 as pg
import dash_table as dt
import plotly.graph_objects as gb
import dash_table

connection = pg.connect(
            database='database',
            user = "user",
            password = "password",
            host = "ip",
            port = 'port no'
             )
# sets dash app
external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
app = dash.Dash(__name__, external_stylesheets=external_stylesheets)
server=app.server
mapbox_access_token='token'
# cretes color theme
theme={
        'font-family': 'Raleway',
        'background-color':'#787878',
        }
colorscale_mag = [
        [0, '#ffffb2'],
        [0.25, '#fecc5c'],
        ]
# sql querie to get data from  Postgres
def get_year_data():
    query = """ SELECT DISTINCT "FIRE_YEAR" FROM fires; """
    return createDataframe(query)

def get_county_data():
    query =""" SELECT DISTINCT plot_county_code FROM plot_condition; """
    return createDataframe(query)


def get_fire_data(selected_year):
    query= """ SELECT "FIRE_NAME", "FIRE_YEAR", "FIRE_SIZE_CLASS", "LONGITUDE", "LATITUDE"
                            FROM fires
                                WHERE "FIRE_SIZE_CLASS"='G'; """
    return createDataframe(query)

def get_plot_condition_data(selected_year):
    query = """ SELECT plot_county_code, plot_inventory_year, forest_type_code_name, latitude, longitude
                                FROM  plot_condition;"""
    return createDataframe(query)

def createDataframe(query):
    # Create connection to postgres and create data frame
    dataframe = pd.read_sql(query, connection)
    connection.close()
    return dataframe
# creates a distinct years for dropdown
year_options = []
dfyear=get_year_data()
dfyear = dfyear.sort_values("FIRE_YEAR",ascending=False)

for year in dfyear['FIRE_YEAR'].unique():
    year_options.append({'label':year, 'value':int(year)})

#print(year_options)
# Creates application with dropdown, table and a graph
app.layout=html.Div(children=[
            html.H1('Wildfire Risk Management Platform'),
            html.H4('Select an year from the drop down'),
            html.Div([
                dcc.Dropdown(id='select_year', options=year_options,
                value=dfyear['FIRE_YEAR'].max())
                ], style={'width':'48%', 'display':'inline-block'}),

            dcc.Graph(id='graph'),
            #style={'padding':10}
            html.Div(id='fire_table',
                #page_size=5,
                children= dash_table.DataTable(
                    id = 'fire_tbl',
                    style_table={'width': '40%', 'height':'40%'},
                    #style_data={'whiteSpace': 'normal'},
                    #content_style='grow',
                        )
                    )
                ])

# call back functins for input and output for the figure
@app.callback(Output('graph', 'figure'),
                [Input('select_year', 'value')])
                    #Input('select_county', 'value')])

def update_figure(selected_year):

    #print(selected_year, selected_county)
    df = get_fire_data(selected_year)
    df2 = get_plot_condition_data(selected_year)
    #print(df2)
    filtered_df = df[df["FIRE_YEAR"]==selected_year]
    filtered_df2 = df2[df2['plot_inventory_year']==selected_year]
    min_lat = filtered_df["LATITUDE"].min()
    max_lat = filtered_df["LATITUDE"].max()
    min_lon = filtered_df["LONGITUDE"].min()
    max_lon = filtered_df["LONGITUDE"].max()
# layout for creating scattermapbox

    layout = go.Layout(title='Wildfires and Vegetation',
                    autosize=False,
                    hovermode='closest',
                    height=1500,
                    width=1500,
                    margin=go.Margin(l=20, r=10, t=0, b=50, pad=4),
                    mapbox=dict(
                        accesstoken=mapbox_access_token,
                            bearing=0,
                            center=dict(
                                lat=float(min_lat),
                                lon=float(max_lon),
                                                ),
                            pitch=0,
                            #style=map_style,
                            zoom = 5.5,
                            #style=map_style,
                            ),
                        )
    data=go.Data([
                 go.Scattermapbox(
                    lon = filtered_df["LONGITUDE"],
                    lat = filtered_df["LATITUDE"],
                    text=filtered_df["FIRE_NAME"],
                    name='',
                    mode='markers',
                    marker=go.Marker(
                        #symbol='diamond-tall-dot',
                        size=30,
                        color='#FF0000',
                        opacity=1,
                        #symbol='diamond-tall-dot',
                       # "symbol": "circle-dot"
                        ),
                        #showlegend=False,
                    ),

                go.Scattermapbox(
                    lon = filtered_df2["longitude"],
                    lat = filtered_df2["latitude"],
                    text = filtered_df2["forest_type_code_name"],
                    name='',
                    mode='markers',
                    marker = dict(
                        #symbol='triangle-up-open',
                        size = 12,
                        opacity=0.7,
                        color='#006400',
                        #symbol='triangle-up-open',
                             ),
                            showlegend=False
                        ),
                    ])

    fig=go.Figure(data=data, layout=layout)

    return(fig)

# Call back for creating table with fire data
@app.callback(Output('fire_table', 'children'),
                [Input('select_year', 'value')])

def update_fire_table(selected_year):

    #print(selected_year, selected_county)
    df4 = get_fire_data(selected_year)
    filtered_fire_df = df4[df4["FIRE_YEAR"]==selected_year]
    fire_columns=[{'name':i, 'id':i} for i in filtered_fire_df.columns]
    return html.Div([
            dash_table.DataTable(
                id='tbl_fire',
                style_header={'backgroundColor':'#CD853F'},
            columns=fire_columns,
            data=filtered_fire_df.to_dict("row"))
            ])

if __name__=='__main__':
    app.run_server(host='0.0.0.0', port='8080', debug=True)
