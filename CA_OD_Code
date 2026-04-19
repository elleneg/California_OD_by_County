import pandas as pd
import requests
import io

# --- CONFIGURATION ---
import os
API_KEY = os.getenv("GitCAL_OD_Map_API_KEY")
CHART_ID = "bodZ9"

# Link to the "Death Profiles by County" CSV on CHHS Portal
# We are using the 2025-2026 Provisional data for the most current weekly updates
DATA_URL = "https://data.chhs.ca.gov/dataset/death-profiles-by-county/resource/0dc57617-ddfa-4e90-a47d-264cf211730e/download/chsp_2024_odp.csv"

def update_map():
    try:
        # 1. Fetch the data
        print("Downloading latest California overdose data...")
        response = requests.get(DATA_URL)
        df = pd.read_csv(io.StringIO(response.text))

        # 2. Filter for "All Drug Overdose" and the most recent year/timeframe
        # Note: 'Indicator' names vary slightly; this filters for the primary 'All' category
        all_overdoses = df[df['Indicator'].str.contains('All Drug Overdose', case=False, na=False)]
        
        # 3. Clean and Format for Datawrapper
        # We need a 'County' column and a 'Value' column
        final_df = all_overdoses[['County', 'Rate/Percentage']].copy()
        final_df.columns = ['County', 'Death Rate']
        
        # 4. Upload to Datawrapper
        print(f"Uploading to Datawrapper chart {CHART_ID}...")
        headers = {
            "Authorization": f"Bearer {API_KEY}",
            "Content-Type": "text/csv"
        }
        
        payload = final_df.to_csv(index=False)
        update_res = requests.put(
            f"https://api.datawrapper.de/v3/charts/{CHART_ID}/data",
            headers=headers,
            data=payload
        )

        if update_res.status_code == 204:
            # 5. Publish the chart so changes are visible
            publish_res = requests.post(
                f"https://api.datawrapper.de/v3/charts/{CHART_ID}/publish", 
                headers=headers
            )
            print("Successfully updated and published the map!")
        else:
            print(f"Upload failed: {update_res.status_code} - {update_res.text}")

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    update_map()
