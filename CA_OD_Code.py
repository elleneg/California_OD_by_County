import pandas as pd
import requests
import io
import os

# --- CONFIGURATION ---
API_KEY = os.getenv("CAL_OD_DATAWRAPPER")
CHART_ID = "bodZ9"
DATA_URL = "https://data.chhs.ca.gov/dataset/58619b69-b3cb-41a7-8bfc-fc3a524a9dd4/resource/2e546f88-bba8-4d77-846a-7fb77846cac6/download/2026-03_deaths_provisional_county_month_sup.csv"

# California County Populations (2023/24 Estimates)
POPS = {
    'Alameda': 1628997, 'Alpine': 1159, 'Amador': 41412, 'Butte': 207183, 'Calaveras': 46332,
    'Colusa': 22036, 'Contra Costa': 1155025, 'Del Norte': 27082, 'El Dorado': 193221,
    'Fresno': 1017162, 'Glenn': 28805, 'Humboldt': 133985, 'Imperial': 179331, 'Inyo': 18839,
    'Kern': 917673, 'Kings': 153443, 'Lake': 68140, 'Lassen': 28861, 'Los Angeles': 9663345,
    'Madera': 160222, 'Marin': 254407, 'Mariposa': 17147, 'Mendocino': 90310, 'Merced': 291920,
    'Modoc': 8500, 'Mono': 13066, 'Monterey': 430723, 'Napa': 133216, 'Nevada': 102037,
    'Orange': 3135757, 'Placer': 417772, 'Plumas': 19131, 'Riverside': 2473931, 'Sacramento': 1588921,
    'San Benito': 68175, 'San Bernardino': 2195611, 'San Diego': 3269973, 'San Francisco': 808988,
    'San Joaquin': 800965, 'San Luis Obispo': 281635, 'San Mateo': 726353, 'Santa Barbara': 441257,
    'Santa Clara': 1877592, 'Santa Cruz': 261547, 'Shasta': 180366, 'Sierra': 3217, 'Siskiyou': 43660,
    'Solano': 449218, 'Sonoma': 481812, 'Stanislaus': 552999, 'Sutter': 97948, 'Tehama': 65498,
    'Trinity': 15670, 'Tulare': 479401, 'Tuolumne': 54539, 'Ventura': 829749, 'Yolo': 216110, 'Yuba': 84310
}

def update_map():
    try:
        print("Fetching data...")
        r = requests.get(DATA_URL)
        df = pd.read_csv(io.StringIO(r.text), on_bad_lines='skip')

        # 1. Filter and Clean
        mask = (df['Cause_Desc'].str.contains('Accidents', case=False, na=False)) & (df['County'] != 'California')
        filtered_df = df[mask].copy()
        filtered_df['Count'] = pd.to_numeric(filtered_df['Count'], errors='coerce').fillna(0)
        
        # 2. Group by County
        map_data = filtered_df.groupby('County')['Count'].sum().reset_index()
        
        # 3. Calculate Rate per 100k
        map_data['Population'] = map_data['County'].map(POPS)
        map_data['Rate'] = (map_data['Count'] / map_data['Population']) * 100000
        map_data['Rate'] = map_data['Rate'].round(2) # Round to 2 decimal places

        # Prepare final dataframe for Datawrapper
        final_dw_data = map_data[['County', 'Rate', 'Count']]
        final_dw_data.columns = ['County', 'Death Rate (per 100k)', 'Total Deaths']

        print(f"✅ Calculation complete. Max rate found: {final_dw_data['Death Rate (per 100k)'].max()}")

        # 4. Upload & Publish
        headers = {"Authorization": f"Bearer {API_KEY}", "Content-Type": "application/json"}
        
        # Reset source type
        requests.patch(f"https://api.datawrapper.de/v3/charts/{CHART_ID}", headers=headers, json={"metadata": {"data": {"source-type": "uploaded-csv"}}})

        # Upload CSV
        requests.put(
            f"https://api.datawrapper.de/v3/charts/{CHART_ID}/data",
            headers={"Authorization": f"Bearer {API_KEY}", "Content-Type": "text/csv"},
            data=final_dw_data.to_csv(index=False)
        )

        requests.post(f"https://api.datawrapper.de/v3/charts/{CHART_ID}/publish", headers=headers)
        print("🚀 Success! Rate-based map published.")

    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    update_map()
