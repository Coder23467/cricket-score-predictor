import pandas as pd
import numpy as np
import subprocess
import sys
import time
import os

# --- IMPORTANT: We only import 'requests' inside the main execution block
# after the installation function is called, to prevent the crash.
# However, for the initial import line at the top, we must assume it's there
# or move the entire function. The safest approach is to move the logic. ---

def install_excel_lib():
    """Installs the necessary libraries (requests, openpyxl)."""
    # This function is now only called once, at the start of __main__
    try:
        # We need the 'requests' library to make HTTP calls to the API
        print("Installing required libraries (requests, openpyxl)...")
        # Install both libraries in one command
        subprocess.check_call([sys.executable, "-m", "pip", "install", "openpyxl", "requests"])
        print("\nSuccessfully installed openpyxl and requests.")
    except subprocess.CalledProcessError:
        print("\nFailed to install required libraries. Please install them manually with 'pip install openpyxl requests'.")

def get_historical_weather(lat, lon, dt):
    """
    Fetches historical weather data for a specific location and time using the OpenWeatherMap API.
    
    NOTE: 'requests' must be imported here to use it. We assume successful installation.
    
    Parameters:
    - lat (float): Latitude of the stadium.
    - lon (float): Longitude of the stadium.
    - dt (int): UNIX timestamp of the match start time.
    
    Returns:
    - dict: Dictionary containing key weather metrics (temp, humidity, wind_speed, dew_point).
    """
    import requests # Local import now possible after successful installation
    
    # --- IMPORTANT: SET YOUR API KEY HERE ---
    OPENWEATHERMAP_API_KEY = "YOUR_OPENWEATHERMAP_API_KEY" 
    OPENWEATHERMAP_URL = "http://api.openweathermap.org/data/2.5/onecall/timemachine"
    # ----------------------------------------
    
    if OPENWEATHERMAP_API_KEY == "YOUR_OPENWEATHERMAP_API_KEY":
        # Return dummy data if API key is not set (for testing purposes)
        return {
            'match_temp': 31.5,
            'match_humidity': 45,
            'match_wind_speed': 15,
            'match_dew_point': 12.5
        }

    params = {
        'lat': lat,
        'lon': lon,
        'dt': dt,
        'appid': OPENWEATHERMAP_API_KEY,
        'units': 'metric'
    }

    try:
        response = requests.get(OPENWEATHERMAP_URL, params=params, timeout=5)
        response.raise_for_status() # Raise exception for bad status codes (4xx or 5xx)
        data = response.json()
        
        # The API returns an hourly block (data['hourly'][0]) for the requested timestamp
        hourly_data = data['hourly'][0]

        return {
            'match_temp': hourly_data.get('temp'),
            'match_humidity': hourly_data.get('humidity'),
            'match_wind_speed': hourly_data.get('wind_speed'),
            'match_dew_point': hourly_data.get('dew_point')
        }
    except requests.exceptions.RequestException as e:
        print(f"API Error for timestamp {dt}: {e}. Returning average/dummy values.")
        # Return NaN or a fallback if the API call fails
        return {
            'match_temp': np.nan,
            'match_humidity': np.nan,
            'match_wind_speed': np.nan,
            'match_dew_point': np.nan
        }
    except (IndexError, KeyError):
        print(f"API returned incomplete data for timestamp {dt}. Returning dummy values.")
        return {
            'match_temp': np.nan,
            'match_humidity': np.nan,
            'match_wind_speed': np.nan,
            'match_dew_point': np.nan
        }


def merge_datasets(deliveries_path, matches_path, geo_data_path):
    """
    Loads, cleans, and merges the three provided datasets, adding custom engineered features.
    """
    try:
        # We need to import requests here to satisfy the linter/static checker, 
        # but in runtime, the __main__ block ensures it's installed first.
        import requests 
    except ImportError:
        # If running interactively, this might still fail, but running the 
        # __main__ block guarantees proper setup.
        pass 

    try:
        deliveries_df = pd.read_csv(deliveries_path)
        matches_df = pd.read_csv(matches_path)
        geo_df = pd.read_excel(geo_data_path)
        print("All datasets loaded successfully.")

    except FileNotFoundError as e:
        print(f"Error: {e}. Please ensure all files are in the correct directory.")
        return None

    # --- Step 1: Calculate Inning Scores and Match Prep ---
    print("\nCalculating inning scores...")
    inning_scores = deliveries_df.groupby(['match_id', 'inning'])['total_runs'].sum().reset_index()
    inning_scores.rename(columns={'total_runs': 'inning_score'}, inplace=True)
    
    merged_df = pd.merge(matches_df, inning_scores, left_on='id', right_on='match_id', how='inner')
    merged_df.drop_duplicates(subset=['id', 'inning'], keep='first', inplace=True)
    print("Inning scores merged with matches data.")

    # Convert date to datetime for feature engineering
    merged_df['date'] = pd.to_datetime(merged_df['date'], format='mixed', dayfirst=True)
    merged_df['Year'] = merged_df['date'].dt.year

    # --- Step 2: Feature Engineering (Pitch Degradation) ---
    print("Engineering dynamic pitch degradation features...")
    
    merged_df = merged_df.sort_values(['venue', 'date'])
    merged_df['last_match_date'] = merged_df.groupby('venue')['date'].shift(1)
    merged_df['Days Since Last Match'] = (merged_df['date'] - merged_df['last_match_date']).dt.days
    merged_df['Days Since Last Match'] = merged_df['Days Since Last Match'].fillna(365)
    merged_df['Matches This Season'] = merged_df.groupby(['season', 'venue']).cumcount() + 1
    print("Dynamic features ('Days Since Last Match', 'Matches This Season') added.")

    # --- Step 3: Standardize Venue Names and Merge Static Geo Data ---
    venue_mapping = {
        'Rajiv Gandhi International Stadium, Uppal': 'Rajiv Gandhi International Stadium', 'M. Chinnaswamy Stadium': 'M Chinnaswamy Stadium', 'Holkar Cricket Stadium': 'Holkar Cricket Stadium', 'Maharashtra Cricket Association Stadium': 'Maharashtra Cricket Association Stadium', 'Wankhede Stadium': 'Wankhede Stadium', 'Feroz Shah Kotla Ground': 'Feroz Shah Kotla Ground', 'Eden Gardens': 'Eden Gardens', 'Punjab Cricket Association Stadium, Mohali': 'IS Bindra Stadium', 'Feroz Shah Kotla': 'Feroz Shah Kotla Ground', 'M. A. Chidambaram Stadium': 'M. A. Chidambaram Stadium', 'Sardar Patel Stadium, Motera': 'Sardar Patel Stadium, Motera', 'Himachal Pradesh Cricket Association Stadium': 'Himachal Pradesh Cricket Association Stadium', 'Subrata Roy Sahara Stadium': 'Maharashtra Cricket Association Stadium', 'JSCA International Stadium Complex': 'JSCA International Stadium Complex', 'Barabati Stadium': 'Barabati Stadium', 'Saurashtra Cricket Association Stadium': 'Saurashtra Cricket Association Stadium', 'Shaheed Veer Narayan Singh International Stadium': 'Shaheed Veer Narayan Singh International Stadium', 'Dr. Y.S. Rajasekhara Reddy ACA-VDCA Cricket Stadium': 'Dr. Y.S. Rajasekhara Reddy ACA-VDCA Cricket Stadium', 'ACA-VDCA Stadium': 'Dr. Y.S. Rajasekhara Reddy ACA-VDCA Cricket Stadium', 'M. Chinnaswamy Stadium, Bengaluru': 'M Chinnaswamy Stadium', 'Wankhede Stadium, Mumbai': 'Wankhede Stadium', 'Sheikh Zayed Stadium': 'Sheikh Zayed Cricket Stadium'
    }
    merged_df['venue'] = merged_df['venue'].map(venue_mapping).fillna(merged_df['venue'])
    geo_df.rename(columns={'Stadium': 'venue'}, inplace=True)

    # Merge static geo data
    df = pd.merge(merged_df, geo_df[['venue', 'Year', 'Latitude', 'Longitude']], on=['venue', 'Year'], how='left')
    
    # --- Step 4: API Integration (Dynamic Weather) ---
    print("Integrating dynamic match-day weather data via simulated API...")
    
    # Drop duplicates for API calls (we only need one call per match)
    unique_matches = df[['id', 'date', 'Latitude', 'Longitude']].drop_duplicates(subset=['id']).copy()

    # Convert match date/time to UNIX timestamp (required by API)
    # We assume an evening start time (6 PM local time) for T20 matches
    unique_matches['dt'] = (unique_matches['date'] + pd.Timedelta(hours=18)).astype(int) // 10**9 

    # Prepare lists to collect results
    match_weather_list = []
    
    # Iterate through unique matches and call API
    for index, row in unique_matches.iterrows():
        weather_data = get_historical_weather(row['Latitude'], row['Longitude'], row['dt'])
        weather_data['id'] = row['id']
        match_weather_list.append(weather_data)
        # Use time.sleep(1) here to respect API rate limits in a real project!

    match_weather_df = pd.DataFrame(match_weather_list)
    
    # Merge the new dynamic weather data back into the main DataFrame
    final_df = pd.merge(df.drop(columns=['Latitude', 'Longitude']), match_weather_df, on='id', how='left')

    # Re-merge the rest of the static geo data that was initially excluded
    final_df = pd.merge(final_df, geo_df.drop(columns=['Latitude', 'Longitude']), on=['venue', 'Year'], how='left')
    
    print("Dynamic weather features added successfully.")

    # --- Step 5: Final Cleaning and Preparation ---
    final_df = final_df.drop(columns=['id', 'match_id', 'player_of_match', 'result', 
                                      'dl_applied', 'umpire1', 'umpire2', 'umpire3', 'last_match_date'])
    
    # Fill any remaining NaN values in numerical columns
    numeric_cols = final_df.select_dtypes(include=np.number).columns
    final_df[numeric_cols] = final_df[numeric_cols].fillna(final_df[numeric_cols].mean())

    print("\nFinal merged DataFrame shape:", final_df.shape)
    print("Final merged DataFrame head:")
    print(final_df.head())
    
    return final_df

if __name__ == '__main__':
    # **CRITICAL FIX: Install libraries before any external imports attempt to use them.**
    install_excel_lib()

    deliveries_file = 'deliveries.csv'
    matches_file = 'matches.csv'
    geo_file = 'cricket_stadiums_india_southafrica_uae_filled (1).xlsx'
    
    final_dataset = merge_datasets(deliveries_file, matches_file, geo_file)
    
    if final_dataset is not None:
        final_dataset.to_csv('final_cricket_dataset.csv', index=False)
        print("\nFinal dataset saved to 'final_cricket_dataset.csv'.")

