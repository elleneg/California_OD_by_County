import pandas as pd
import requests
import io
import os

# --- CONFIGURATION ---
API_KEY = os.getenv("DW_API_KEY")
CHART_ID = "bodZ9"
# This is the 2026 Provisional Monthly CSV
DATA_URL = "https://data.chhs.ca.gov/dataset/58619b69-b3cb-41a7-8bfc-fc3a524a9dd4/resource/2e546f88-bba8-4d77-846a-7fb77846cac6/download/2026-03_deaths_provisional_county_month_sup.csv"

def update_map():
    try:
        print("Fetching Provisional 2026 Data...")
        r = requests.get(DATA_URL)
        # Skip the HTML/Header noise if it exists
        df = pd.read_csv(io.StringIO(r.text), on_bad_lines='skip')

        # 1. FILTER: We want 'Accidents' and we EXCLUDE the 'California' total row
        mask = (df['Cause_Desc'].str.contains('Accidents', case=False, na=False)) & (df['County'] != 'California')
        filtered_df = df[mask].copy()

        # 2. CLEAN: Convert 'Count' to numbers (treats 'S' or suppressed data as 0)
        filtered_df['Count'] = pd.to_numeric(filtered_df['Count'], errors='coerce').fillna(0)

        # 3. SUM: Group by county to get Year-to-Date totals
        map_data = filtered_df.groupby('County')['Count'].sum().reset_index()
        map_data.columns = ['County', 'Accidental Deaths (Provisional)']

        print(f"✅ Success! Found {len(map_data)} counties.")

        # 4. UPLOAD & RE-TITLE
        headers = {"Authorization": f"Bearer {API_KEY}", "Content-Type": "application/json"}
        
        # Upload the CSV data
        requests.put(
            f"https://api.datawrapper.de/v3/charts/{CHART_ID}/data",
            headers={"Authorization": f"Bearer {API_KEY}", "Content-Type": "text/csv"},
            data=map_data.to_csv(index=False)
        )

        # Update metadata to show this is Provisional data
        metadata = {
            "title": "California Accidental & Unintentional Injury Deaths (2026 YTD)",
            "metadata": {
                "describe": {
                    "intro": "Data reflects provisional monthly death counts including overdoses, falls, and transport accidents."
                }
            }
        }
        requests.patch(f"https://api.datawrapper.de/v3/charts/{CHART_ID}", headers=headers, json=metadata)

        # 5. PUBLISH
        requests.post(f"https://api.datawrapper.de/v3/charts/{CHART_ID}/publish", headers=headers)
        print("🚀 Map updated, Re-titled, and Published!")

    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    update_map()
