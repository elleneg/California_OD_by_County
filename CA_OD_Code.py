import pandas as pd
import requests
import io
import os

# --- CONFIGURATION ---
API_KEY = os.getenv("CAL_OD_DATAWRAPPER")
CHART_ID = "bodZ9"
DATA_URL = "https://data.chhs.ca.gov/dataset/58619b69-b3cb-41a7-8bfc-fc3a524a9dd4/resource/2e546f88-bba8-4d77-846a-7fb77846cac6/download/2026-03_deaths_provisional_county_month_sup.csv"

def update_map():
    try:
        print("Fetching data...")
        r = requests.get(DATA_URL)
        df = pd.read_csv(io.StringIO(r.text), on_bad_lines='skip')

        mask = (df['Cause_Desc'].str.contains('Accidents', case=False, na=False)) & (df['County'] != 'California')
        filtered_df = df[mask].copy()
        filtered_df['Count'] = pd.to_numeric(filtered_df['Count'], errors='coerce').fillna(0)
        map_data = filtered_df.groupby('County')['Count'].sum().reset_index()
        map_data.columns = ['County', 'Accidental Deaths (Provisional)']

        # --- THE RESET LOGIC ---
        # This tells Datawrapper to switch its data source type to API-driven CSV
        headers = {"Authorization": f"Bearer {API_KEY}", "Content-Type": "application/json"}
        reset_metadata = {
            "externalData": "",
            "metadata": {
                "data": {
                    "source-type": "uploaded-csv"
                }
            }
        }
        requests.patch(f"https://api.datawrapper.de/v3/charts/{CHART_ID}", headers=headers, json=reset_metadata)

        # Now push the actual data
        csv_data = map_data.to_csv(index=False)
        upload_res = requests.put(
            f"https://api.datawrapper.de/v3/charts/{CHART_ID}/data",
            headers={"Authorization": f"Bearer {API_KEY}", "Content-Type": "text/csv"},
            data=csv_data
        )

        if upload_res.status_code == 204:
            requests.post(f"https://api.datawrapper.de/v3/charts/{CHART_ID}/publish", headers=headers)
            print("🚀 Success! Data forced into Datawrapper and published.")
        else:
            print(f"❌ Failed: {upload_res.text}")

    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    update_map()
