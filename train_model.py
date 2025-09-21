import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error
import numpy as np

def train_and_evaluate_model(file_path):
    """
    Loads the final dataset, trains a RandomForestRegressor model, and evaluates its performance.
    
    Parameters:
    - file_path: Path to the final merged dataset CSV file.
    """
    try:
        df = pd.read_csv(file_path)
        print("Final dataset loaded successfully.")
    except FileNotFoundError:
        print(f"Error: The file '{file_path}' was not found.")
        print("Please run merge_datasets.py first to create this file.")
        return
        
    # --- Step 1: Feature Engineering and Data Preparation ---
    # One-hot encode categorical features
    categorical_features = ['toss_winner', 'toss_decision', 'winner', 'venue', 'Country', 
                            'Pitch Soil Type', 'Wind Condition']
    df = pd.get_dummies(df, columns=categorical_features, drop_first=True)
    
    # Drop columns that are no longer needed for training, including 'city'
    # as it is already represented by 'venue'.
    df = df.drop(columns=['team1', 'team2', 'date', 'city'], errors='ignore')
    
    print("Categorical features have been one-hot encoded.")

    # --- Step 2: Define Features (X) and Target (y) ---
    X = df.drop('inning_score', axis=1)
    y = df['inning_score']

    # --- Filter out any remaining non-numerical columns from X before training ---
    X_numerical = X.select_dtypes(include=np.number)
    
    # --- Step 3: Split the Data ---
    X_train, X_test, y_train, y_test = train_test_split(X_numerical, y, test_size=0.2, random_state=42)
    print("Data split into training and testing sets.")

    # --- Step 4: Train the Model ---
    # We use RandomForestRegressor, a powerful model for this type of problem
    model = RandomForestRegressor(n_estimators=100, random_state=42)
    model.fit(X_train, y_train)
    print("Model training complete.")

    # --- Step 5: Evaluate the Model ---
    predictions = model.predict(X_test)
    
    mae = mean_absolute_error(y_test, predictions)
    rmse = np.sqrt(mean_squared_error(y_test, predictions))
    
    print("\n--- Model Evaluation ---")
    print(f"Mean Absolute Error (MAE): {mae:.2f}")
    print(f"Root Mean Squared Error (RMSE): {rmse:.2f}")

if __name__ == '__main__':
    train_and_evaluate_model('final_cricket_dataset.csv')


