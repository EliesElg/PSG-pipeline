import os
import json
import requests
from dotenv import load_dotenv

# 1. Load environment variables (to keep your API key safe)
load_dotenv()
API_KEY = os.getenv("API_KEY")

if not API_KEY:
    print("Error: API_KEY not found. Please check your .env file.")
    exit(1)

# 2. Define the API Endpoint
# PSG's ID in football-data.org is usually 524. 
# We will fetch their scheduled matches.
TEAM_ID = 524
URL = f"https://api.football-data.org/v4/teams/{TEAM_ID}/matches"

headers = {
    "X-Auth-Token": API_KEY
}

def extract_data():
    print(f"Fetching data for PSG (Team ID: {TEAM_ID})...")
    
    try:
        # 3. Make the HTTP Request
        response = requests.get(URL, headers=headers)
        response.raise_for_status() # Check for errors (like 401 Unauthorized or 404 Not Found)
        
        data = response.json()
        
        # 4. Save raw data to the Landing Zone
        output_path = "data/raw/psg_matches.json"
        
        # Ensure directory exists (just in case)
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4)
            
        print(f"Success! Data saved to {output_path}")
        print(f"Total matches fetched: {data.get('count', 0)}")
        
    except requests.exceptions.HTTPError as err:
        print(f"HTTP Error: {err}")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    extract_data()
