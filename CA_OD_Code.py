import pandas as pd
import requests
import io
import os

# --- CONFIGURATION ---
API_KEY = os.getenv("DW_API_KEY")
CHART_ID = "bodZ9"

# This is the DIRECT link to the 2026 Provisional CSV file
DATA_URL = "https://data.chhs.ca.gov/dataset/58619b69-b3cb-41a7-8bfc-fc3a524a9dd4/resource/2e546f88-bba8-4d77-846a-7fb77846cac6/download/2026-03_deaths_provisional_county_month_sup.csv"

def update_map():
    try:
        print("Fetching direct CSV from CHHS...")
        response = requests.get(DATA_URL)
        
        # Load data (using 'on_bad_lines' to handle any state disclaimer rows)
        df = pd.read_csv(io.StringIO(response.text), on_bad_lines='skip')
        print(f"Success! Found columns: {df.columns.tolist()}")

        # 1. Filter for Drug Overdose only
        # The column is 'Cause_Desc' and we want rows containing 'overdose'
        mask = df['Cause_Desc'].str.contains('overdose', case=False, na=False)
        overdose_df = df[mask].copy()

        if overdose_df.empty:
            print("❌ No rows found for 'Drug overdose'. Check Cause_Desc sample:")
            print(df['Cause_Desc'].unique()[:5])
            return

        # 2. Total the counts by County
        # This dataset has monthly rows; we sum them to get the year-to-date total
        map_data = overdose_df.groupby('County')['Count'].sum().reset_index()
        map_data.columns = ['County', 'Total Deaths']

        print(f"✅ Prepared data for {len(map_data)} counties.")

        # 3. Upload to Datawrapper
        headers = {
            "Authorization": f"Bearer {API_KEY}",
            "Content-Type": "text/csv"
        }
        
        upload_res = requests.put(
            f"https://api.datawrapper.de/v3/charts/{CHART_ID}/data",
            headers=headers,
            data=map_data.to_csv(index=False)
        )

        if upload_res.status_code == 204:
            # 4. Re-publish to make it live
            requests.post(f"https://api.datawrapper.de/v3/charts/{CHART_ID}/publish", headers=headers)
            print("🚀 Map successfully updated and published!")
        else:
            print(f"❌ Datawrapper Upload Failed: {upload_res.text}")

    except Exception as e:
        print(f"❌ Script Error: {e}")

if __name__ == "__main__":
    update_map()
