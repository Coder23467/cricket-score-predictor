import pandas as pd
import numpy as np
import subprocess
import sys

def install_excel_lib():
    """Installs the openpyxl library if it's not already installed."""
    try:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "openpyxl"])
        print("\nSuccessfully installed openpyxl.")
    except subprocess.CalledProcessError:
        print("\nFailed to install openpyxl. Please install it manually with 'pip install openpyxl'.")

def merge_datasets(deliveries_path, matches_path, geo_data_path):
    """
    Loads, cleans, and merges the three provided datasets into a single DataFrame.
    
    Parameters:
    - deliveries_path: Path to the ball-by-ball deliveries CSV file.
    - matches_path: Path to the match-level details CSV file.
    - geo_data_path: Path to the geographical data CSV file.
    
    Returns:
    - pd.DataFrame: A single, merged DataFrame ready for analysis.
    """
    try:
        # Load the datasets
        deliveries_df = pd.read_csv(deliveries_path)
        matches_df = pd.read_csv(matches_path)
        # Note: The geo_data file is an .xlsx, so we use read_excel
        geo_df = pd.read_excel(geo_data_path)
        print("All datasets loaded successfully.")

    except FileNotFoundError as e:
        print(f"Error: {e}. Please ensure all files are in the correct directory.")
        return None

    # --- Step 1: Calculate Inning Scores from Deliveries Data ---
    print("\nCalculating inning scores...")
    inning_scores = deliveries_df.groupby(['match_id', 'inning'])['total_runs'].sum().reset_index()
    inning_scores.rename(columns={'total_runs': 'inning_score'}, inplace=True)
    
    # Merge matches data with inning scores
    merged_df = pd.merge(matches_df, inning_scores, left_on='id', right_on='match_id', how='inner')
    print("Inning scores merged with matches data.")

    # --- Step 2: Prepare Matches Data for Merging with Geographical Data ---
    # Convert date to datetime to extract the year, which is a key for merging
    # Using dayfirst=True to handle formats like DD/MM/YY
    merged_df['date'] = pd.to_datetime(merged_df['date'], format='mixed', dayfirst=True)
    merged_df['Year'] = merged_df['date'].dt.year

    # Correct venue names to match the geographical data
    # This is a crucial step for accurate merging.
    venue_mapping = {
        'Rajiv Gandhi International Stadium, Uppal': 'Rajiv Gandhi International Stadium',
        'M. Chinnaswamy Stadium': 'M Chinnaswamy Stadium',
        'Holkar Cricket Stadium': 'Holkar Cricket Stadium',
        'Maharashtra Cricket Association Stadium': 'Maharashtra Cricket Association Stadium',
        'Wankhede Stadium': 'Wankhede Stadium',
        'Feroz Shah Kotla Ground': 'Feroz Shah Kotla Ground',
        'Eden Gardens': 'Eden Gardens',
        'Punjab Cricket Association Stadium, Mohali': 'IS Bindra Stadium',
        'Feroz Shah Kotla': 'Feroz Shah Kotla Ground',
        'M. A. Chidambaram Stadium': 'M. A. Chidambaram Stadium',
        'Sardar Patel Stadium, Motera': 'Sardar Patel Stadium, Motera',
        'Himachal Pradesh Cricket Association Stadium': 'Himachal Pradesh Cricket Association Stadium',
        'Subrata Roy Sahara Stadium': 'Maharashtra Cricket Association Stadium',
        'JSCA International Stadium Complex': 'JSCA International Stadium Complex',
        'Barabati Stadium': 'Barabati Stadium',
        'Saurashtra Cricket Association Stadium': 'Saurashtra Cricket Association Stadium',
        'Shaheed Veer Narayan Singh International Stadium': 'Shaheed Veer Narayan Singh International Stadium',
        'Dr. Y.S. Rajasekhara Reddy ACA-VDCA Cricket Stadium': 'Dr. Y.S. Rajasekhara Reddy ACA-VDCA Cricket Stadium',
        'ACA-VDCA Stadium': 'Dr. Y.S. Rajasekhara Reddy ACA-VDCA Cricket Stadium',
        'M. Chinnaswamy Stadium, Bengaluru': 'M Chinnaswamy Stadium',
        'Wankhede Stadium, Mumbai': 'Wankhede Stadium',
        'Sheikh Zayed Stadium': 'Sheikh Zayed Cricket Stadium'
    }
    merged_df['venue'] = merged_df['venue'].map(venue_mapping).fillna(merged_df['venue'])
    geo_df.rename(columns={'Stadium': 'venue'}, inplace=True)

    # --- Step 3: Merge with the Comprehensive Geographical Data ---
    # Merge on both venue and year for the most accurate join
    final_df = pd.merge(merged_df, geo_df, on=['venue', 'Year'], how='left')
    print("Geographical data merged with the dataset.")

    # --- Step 4: Final Cleaning and Preparation ---
    # Drop columns that are no longer needed
    final_df = final_df.drop(columns=['id', 'match_id', 'player_of_match', 'result', 
                                      'dl_applied', 'umpire1', 'umpire2', 'umpire3'])
    
    # Fill any remaining NaN values in numerical columns.
    # We use the mean here, but other strategies could be considered.
    numeric_cols = final_df.select_dtypes(include=np.number).columns
    final_df[numeric_cols] = final_df[numeric_cols].fillna(final_df[numeric_cols].mean())

    print("\nFinal merged DataFrame shape:", final_df.shape)
    print("Final merged DataFrame head:")
    print(final_df.head())
    
    return final_df

if __name__ == '__main__':
    # First, make sure the necessary library for Excel is installed
    install_excel_lib()

    # Define file paths
    deliveries_file = 'deliveries.csv'
    matches_file = 'matches.csv'
    geo_file = 'cricket_stadiums_india_southafrica_uae_filled (1).xlsx'
    
    # Run the merge function and get the final DataFrame
    final_dataset = merge_datasets(deliveries_file, matches_file, geo_file)
    
    if final_dataset is not None:
        # You can now save this final dataset for later use
        final_dataset.to_csv('final_cricket_dataset.csv', index=False)
        print("\nFinal dataset saved to 'final_cricket_dataset.csv'.")
