import pandas as pd
import psycopg2
import sys

# --- DATABASE CONNECTION PARAMETERS ---
DB_SETTINGS = {
    "dbname": "cities",   
    "user": "postgres",    
    "password": "Root",
    "host": "localhost",
    "port": "5432" 
}

# --- CSV FILE CONFIGURATION ---
CSV_FILE = 'cities.csv'
# Columns we will use from the CSV
COLUMNS_TO_USE = ['AccentCity', 'Latitude', 'Longitude']
# How we will rename them to match our database table
RENAMED_COLUMNS = {
    'AccentCity': 'city_name',
    'Latitude': 'latitude',
    'Longitude': 'longitude'
}

def transform_data(df):
    """Performs the Transform (T) step, tailored to the Kaggle cities.csv file."""
    print("Transforming data from cities.csv...")
    
    # 1. Select only the columns we need
    df = df[COLUMNS_TO_USE]
    
    # 2. Rename columns for clarity (e.g., 'AccentCity' -> 'city_name')
    df = df.rename(columns=RENAMED_COLUMNS)
    
    # 3. Drop rows with any missing essential values (city_name, lat, or lon)
    df.dropna(subset=['city_name', 'latitude', 'longitude'], inplace=True)
    
    # 4. Ensure correct data types (pandas can misread numbers)
    try:
        df['latitude'] = pd.to_numeric(df['latitude'])
        df['longitude'] = pd.to_numeric(df['longitude'])
    except Exception as e:
        print(f"Error converting lat/lon to numeric: {e}", file=sys.stderr)
        return pd.DataFrame() # Return empty dataframe on failure
            
    # 5. Drop duplicate cities 
    # keep the first entry for any given city name
    original_count = len(df)
    df.drop_duplicates(subset=['city_name'], inplace=True, keep='first')
    new_count = len(df)
    
    print(f"Cleaning complete. Filtered {original_count} rows down to {new_count} unique locations.")
    return df

def load_data(df):
    """Performs the Load (L) step into PostgreSQL."""
    conn = None
    cur = None
    try:
        # --- Connect to PostgreSQL ---
        print("Connecting to PostgreSQL database...")
        conn = psycopg2.connect(**DB_SETTINGS)
        cur = conn.cursor()

        # --- Define the INSERT query ---
        # This query uses the PostGIS function ST_SetSRID(ST_MakePoint(lon, lat), 4326)
        # to create the 'geom' point from our lat/lon columns.
        insert_query = """
        INSERT INTO locations (city_name, latitude, longitude, geom)
        VALUES (%s, %s, %s, ST_SetSRID(ST_MakePoint(%s, %s), 4326));
        """

        print(f"Loading {len(df)} records into 'locations' table... This may take a moment.")
        
        # --- Iterate and Insert ---
        for index, row in df.iterrows():
            cur.execute(insert_query, (
                row['city_name'],
                row['latitude'],
                row['longitude'],
                row['longitude'],  # Note: ST_MakePoint takes (lon, lat)
                row['latitude']
            ))
        
        # --- Commit transactions ---
        conn.commit()
        print(f"Successfully loaded {len(df)} records into PostgreSQL.")

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error connecting or loading data: {error}", file=sys.stderr)
        if conn:
            conn.rollback() # Roll back all changes on error
    finally:
        # --- Close connection ---
        if cur:
            cur.close()
        if conn:
            conn.close()
            print("PostgreSQL connection closed.")

def main():
    """Main ETL function for Pipeline 1."""
    
    # --- Extract (E) ---
    try:
        print(f"Extracting data from {CSV_FILE}...")
        # Read all data as string (dtype=str) first to avoid pandas
        # guessing data types incorrectly, especially for lat/lon.
        df = pd.read_csv(CSV_FILE, dtype=str) 
    except FileNotFoundError:
        print(f"Error: '{CSV_FILE}' not found.", file=sys.stderr)
        print("Please make sure it's in the same folder as this script.", file=sys.stderr)
        return
    except Exception as e:
        print(f"Error reading CSV: {e}", file=sys.stderr)
        return

    # --- Transform (T) ---
    df_cleaned = transform_data(df)

    # --- Load (L) ---
    if not df_cleaned.empty:
        load_data(df_cleaned)
    else:
        print("No data to load after cleaning. Check CSV or transform logic.")

if __name__ == "__main__":
    main()