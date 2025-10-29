import psycopg2
import pymongo
import requests
import sys
from datetime import datetime

# --- CONFIGURATION ---

OPENWEATHER_API_KEY = "6b21e0795399bec23ea1b0354584e100" 

# !!! 2. UPDATE YOUR POSTGRESQL SETTINGS !!!
DB_SETTINGS = {
    "dbname": "cities",    
    "user": "postgres",
    "password": "Root",
    "host": "localhost",
    "port": "5432"
}

# --- MongoDB Settings ---
MONGO_CLIENT_URL = "mongodb://localhost:27017/"
MONGO_DB_NAME = "geoweather_db"
MONGO_COLLECTION_NAME = "weather_reports"

def get_locations_from_postgres():
    """Extract (E): Fetches the list of locations from PostgreSQL."""
    conn = None
    try:
        conn = psycopg2.connect(**DB_SETTINGS)
        cur = conn.cursor()
        
        # We only query for a small batch to avoid API limits on the free plan
        # You can remove 'LIMIT 100' if you have a paid key
        cur.execute("SELECT city_name, latitude, longitude FROM locations LIMIT 100;")
        
        locations = cur.fetchall()
        print(f"Fetched {len(locations)} locations from PostgreSQL.")
        # Returns a list of tuples: [('New York', 40.7128, -74.0060), ...]
        return locations
    
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error fetching from PostgreSQL: {error}", file=sys.stderr)
        return [] # Return empty list on error
    finally:
        if conn:
            conn.close()

def fetch_weather_data(lat, lon):
    """Fetches weather data from OpenWeatherMap API."""
    api_url = f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={OPENWEATHER_API_KEY}&units=metric"
    
    try:
        response = requests.get(api_url)
        response.raise_for_status() 
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching API data for ({lat}, {lon}): {e}", file=sys.stderr)
        return None

def transform_and_load_to_mongo(data, city_name):
    """Transform (T) and Load (L): Adds metadata and inserts into MongoDB."""
    if data is None:
        return False

    # --- Transform (T) ---
    # 1. Add our own timestamp
    data['fetch_timestamp'] = datetime.utcnow()
    
    # 2. Add the clean city_name from our PostgresDB
    data['city_name_clean'] = city_name
    
    # 3. Add a GeoJSON object for MongoDB's geospatial queries
    #    Note: MongoDB uses [longitude, latitude] order!
    if 'coord' in data:
        data['geojson_location'] = {
            'type': 'Point',
            'coordinates': [data['coord']['lon'], data['coord']['lat']]
        }

    # --- Load (L) ---
    try:
        client = pymongo.MongoClient(MONGO_CLIENT_URL)
        db = client[MONGO_DB_NAME]
        collection = db[MONGO_COLLECTION_NAME]
        
        # Insert the transformed JSON document
        collection.insert_one(data)
        return True
        
    except (Exception, pymongo.errors.PyMongoError) as e:
        print(f"Error loading to MongoDB: {e}", file=sys.stderr)
        return False
    finally:
        if 'client' in locals() and client:
            client.close()

def main():
    """Main ETL function for Pipeline 2."""
    
    # 1. Extract locations
    locations = get_locations_from_postgres()
    
    if not locations:
        print("No locations found in PostgreSQL. Aborting.")
        return

    print("--- Starting Weather Fetching Pipeline ---")
    success_count = 0
    fail_count = 0

    # 2. Loop through locations
    for location in locations:
        city_name, lat, lon = location
        
        # 3. Fetch data from API
        print(f"Fetching weather for {city_name}...", end=' ')
        weather_data = fetch_weather_data(lat, lon)
        
        # 4. Transform and Load
        if weather_data:
            if transform_and_load_to_mongo(weather_data, city_name):
                print("Success.")
                success_count += 1
            else:
                print("Failed to load to Mongo.")
                fail_count += 1
        else:
            print("Failed to fetch from API.")
            fail_count += 1
            
    print("--- Pipeline Run Complete ---")
    print(f"Successfully loaded: {success_count} reports")
    print(f"Failed: {fail_count} reports")
    
    if success_count > 0:
        print(f"\nCheck your MongoDB '{MONGO_DB_NAME}' database in MongoDB Compass!")
        print(f"You should see a new collection: '{MONGO_COLLECTION_NAME}'")


if __name__ == "__main__":
    main()