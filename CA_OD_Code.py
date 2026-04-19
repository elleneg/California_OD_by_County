import pandas as pd
import requests
import io
import os

# --- CONFIGURATION ---
API_KEY = os.getenv("DW_API_KEY")
CHART_ID = "bodZ9"

# Updated to the 2025-2026 Provisional Monthly Death Profiles
DATA_URL = "https://data.chhs.ca.gov/dataset/death-profiles-by-county/resource/2e546f88-bba8-4d77-846a-7fb77846cac6/download/provisional_deaths_by_month_by_county.csv"

def update_map():
    try:
        print("Downloading latest CHHS data...")
        response = requests.get(DATA_URL)
        df = pd.read_csv(io.StringIO(response.text))

        # NEW FILTER LOGIC: 
        # In the 2025/26 data, we look for 'Drug Overdose' in the Cause_Desc
        # We also want to sum the months to get a yearly total per county
        mask = df['Cause_Desc'].str.contains('Drug overdose', case=False, na=False)
        overdose_df = df[mask].copy()

        if overdose_df.empty:
            print("Warning: No data found matching 'Drug overdose'. Checking column names...")
            print(f"Columns available: {df.columns.tolist()}")
            return

        # Group by County and sum the 'Count' column
        # This ensures we get a total for the year-to-date
        map_data = overdose_df.groupby('County')['Count'].sum().reset_index()
        map_data.columns = ['County', 'Total Deaths']

        print(f"Uploading {len(map_data)} counties to Datawrapper...")
        
        headers = {
            "Authorization": f"Bearer {API_KEY}",
            "Content-Type": "text/csv"
        }
        
        csv_payload = map_data.to_csv(index=False)
        update_res = requests.put(
            f"https://api.datawrapper.de/v3/charts/{CHART_ID}/data",
            headers=headers,
            data=csv_payload
        )

        if update_res.status_code == 204:
            requests.post(f"https://api.datawrapper.de/v3/charts/{CHART_ID}/publish", headers=headers)
            print("Success! Data updated and chart re-published.")
        else:
            print(f"Failed to upload: {update_res.text}")

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    update_map()
