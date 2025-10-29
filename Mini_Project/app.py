import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import plotly.express as px
import pymongo
from sqlalchemy import create_engine
import pandas as pd
from datetime import datetime

# --- CONFIGURATION ---

# --- 1. PostgreSQL Settings ---
DB_SETTINGS = {
    "dbname": "cities",
    "user": "postgres",
    "password": "Root",
    "host": "localhost",
    "port": "5432"
}

# --- 2. MongoDB Settings ---
MONGO_CLIENT_URL = "mongodb://localhost:27017/"
MONGO_DB_NAME = "geoweather_db"
MONGO_COLLECTION_NAME = "weather_reports"

# --- HELPER FUNCTIONS ---

def get_tracked_cities_from_mongo():
    """Fetches the list of cities we ACTUALLY have weather for from MongoDB."""
    try:
        client = pymongo.MongoClient(MONGO_CLIENT_URL)
        db = client[MONGO_DB_NAME]
        collection = db[MONGO_COLLECTION_NAME]
        
        # Get a distinct list of city names from all the reports
        city_list = collection.distinct("city_name_clean")
        client.close()
        
        city_list.sort() # Alphabetize
        
        # Convert to a list of dictionaries for the Dropdown
        print(f"Found {len(city_list)} tracked cities for dropdown.")
        return [{'label': city, 'value': city} for city in city_list]
    
    except Exception as e:
        print(f"Error fetching tracked cities from MongoDB: {e}")
        return []

def get_latest_weather_from_mongo():
    """Fetches the MOST RECENT weather report for ALL cities from MongoDB."""
    client = pymongo.MongoClient(MONGO_CLIENT_URL)
    db = client[MONGO_DB_NAME]
    collection = db[MONGO_COLLECTION_NAME]
    
    # This is an "Aggregation Pipeline"
    # 1. Sort by timestamp (newest first)
    # 2. Group by the clean city name, taking the FIRST document (which is the newest)
    pipeline = [
        {"$sort": {"fetch_timestamp": -1}},
        {"$group": {
            "_id": "$city_name_clean",
            "latest_report": {"$first": "$$ROOT"}
        }}
    ]
    
    reports = list(collection.aggregate(pipeline))
    client.close()
    
    # --- Prepare data for Plotly ---
    # We flatten the nested JSON into a clean list for pandas
    data_for_df = []
    for report in reports:
        doc = report['latest_report']
        data_for_df.append({
            "city_name": doc.get('city_name_clean', 'Unknown'),
            "temperature": doc.get('main', {}).get('temp', None),
            "condition": doc.get('weather', [{}])[0].get('description', 'N/A'),
            "lat": doc.get('coord', {}).get('lat', None),
            "lon": doc.get('coord', {}).get('lon', None),
            "timestamp": doc.get('fetch_timestamp')
        })
    
    return pd.DataFrame(data_for_df)

def get_city_history_from_mongo(selected_city):
    """Fetches ALL historical weather reports for ONE selected city."""
    client = pymongo.MongoClient(MONGO_CLIENT_URL)
    db = client[MONGO_DB_NAME]
    collection = db[MONGO_COLLECTION_NAME]
    
    # Find all documents matching the city, sort by time
    query = {"city_name_clean": selected_city}
    reports = list(collection.find(query).sort("fetch_timestamp", 1))
    client.close()
    
    # --- Prepare data for Plotly ---
    data_for_df = []
    for doc in reports:
        data_for_df.append({
            "timestamp": doc.get('fetch_timestamp'),
            "temperature": doc.get('main', {}).get('temp', None),
        })
        
    return pd.DataFrame(data_for_df)

# --- LOAD INITIAL DATA ---
# Fetch the list of cities just once when the app starts
city_options = get_tracked_cities_from_mongo()

# --- INITIALIZE DASH APP ---
app = dash.Dash(__name__)
app.title = "Geo-Weather Tracker"

# --- APP LAYOUT ---
app.layout = html.Div(style={'fontFamily': 'Arial, sans-serif', 'backgroundColor': '#f4f4f4', 'padding': '20px'}, children=[
    
    # 1. Title
    html.H1(
        children='Geo-Indexed Weather Tracking Tool',
        style={'textAlign': 'center', 'color': '#333'}
    ),
    
    # 2. Map Section
    html.Div(className='map-container', style={'marginBottom': '20px', 'boxShadow': '0 4px 8px 0 rgba(0,0,0,0.2)', 'backgroundColor': 'white', 'padding': '15px', 'borderRadius': '5px'}, children=[
        html.H3("Live Weather Map", style={'color': '#555'}),
        html.P("Latest weather reports from all tracked locations."),
        # The Map component
        dcc.Graph(id='weather-map'),
        # Interval timer to refresh the map every 5 minutes (300,000 ms)
        dcc.Interval(
            id='map-interval-component',
            interval=300 * 1000,  # in milliseconds
            n_intervals=0
        )
    ]),
    
    # 3. History Section
    html.Div(className='history-container', style={'boxShadow': '0 4px 8px 0 rgba(0,0,0,0.2)', 'backgroundColor': 'white', 'padding': '15px', 'borderRadius': '5px'}, children=[
        html.H3("Historical Data Viewer", style={'color': '#555'}),
        
        # The Dropdown menu
        dcc.Dropdown(
            id='city-selector',
            options=city_options,
            value=city_options[0]['value'] if city_options else None, # Set default value
            style={'width': '50%', 'margin': 'auto'}
        ),
        
        # The Line Chart
        dcc.Graph(id='temp-history-chart')
    ])
])

# --- CALLBACKS (to make the app interactive) ---

# Callback 1: Update the LIVE MAP
@app.callback(
    Output('weather-map', 'figure'),
    Input('map-interval-component', 'n_intervals') # Triggered by the timer
)
def update_map(n):
    df = get_latest_weather_from_mongo()
    
    if df.empty:
        return px.scatter_mapbox(title="No data found.")

    # Create the map figure using Plotly Express
    fig = px.scatter_mapbox(
        df,
        lat="lat",
        lon="lon",
        color="temperature",
        size="temperature",
        hover_name="city_name",
        hover_data={"condition": True, "temperature": True, "lat": False, "lon": False},
        color_continuous_scale=px.colors.sequential.Viridis,
        size_max=15,
        zoom=1,
        mapbox_style="carto-positron",
        title=f"Live Weather Map (Last Updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')})"
    )
    fig.update_layout(margin={"r":0,"t":40,"l":0,"b":0})
    return fig

# Callback 2: Update the HISTORY CHART
@app.callback(
    Output('temp-history-chart', 'figure'),
    Input('city-selector', 'value') # Triggered by the dropdown
)
def update_history_chart(selected_city):
    if not selected_city:
        return px.line(title="Please select a city.")
        
    df = get_city_history_from_mongo(selected_city)
    
    if df.empty:
        return px.line(title=f"No historical data found for {selected_city}.")

    # Create the line chart figure
    fig = px.line(
        df,
        x="timestamp",
        y="temperature",
        title=f"Temperature Trend for {selected_city}",
        markers=True
    )
    fig.update_layout(xaxis_title="Time", yaxis_title="Temperature (°C)")
    return fig

# --- RUN THE APP ---
if __name__ == '__main__':
    app.run(debug=True)