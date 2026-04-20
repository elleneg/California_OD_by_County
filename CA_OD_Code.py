import pandas as pd
import requests
import io
import os

# --- CONFIGURATION ---
API_KEY = os.getenv("CAL_OD_DATAWRAPPER")
CHART_ID = "bodZ9"

URLS = {
    2021: "https://data.chhs.ca.gov/dataset/58619b69-b3cb-41a7-8bfc-fc3a524a9dd4/resource/6645396b-285b-4c2c-8833-2fa64757523f/download/2021-12_deaths_provisional_county_month_sup.csv",
    2022: "https://data.chhs.ca.gov/dataset/58619b69-b3cb-41a7-8bfc-fc3a524a9dd4/resource/79704207-6819-48ed-a261-26c71f9f2579/download/2022-12_deaths_provisional_county_month_sup.csv",
    2023: "https://data.chhs.ca.gov/dataset/58619b69-b3cb-41a7-8bfc-fc3a524a9dd4/resource/c444053d-2495-4670-bc62-094ec601d331/download/2023-12_deaths_provisional_county_month_sup.csv",
    2024: "https://data.chhs.ca.gov/dataset/58619b69-b3cb-41a7-8bfc-fc3a524a9dd4/resource/7141887e-d476-46c9-9403-16279f57876a/download/2024-12_deaths_provisional_county_month_sup.csv",
    2025: "https://data.chhs.ca.gov/dataset/58619b69-b3cb-41a7-8bfc-fc3a524a9dd4/resource/7141887e-d476-46c9-9403-16279f57876a/download/2025-12_deaths_provisional_county_month_sup.csv",
    2026: "https://data.chhs.ca.gov/dataset/58619b69-b3cb-41a7-8bfc-fc3a524a9dd4/resource/2e546f88-bba8-4d77-846a-7fb77846cac6/download/2026-03_deaths_provisional_county_month_sup.csv"
}

POPS = {
    'Alameda': 1628997, 'Alpine': 1159, 'Amador': 41412, 'Butte': 207183, 'Calaveras': 46332, 'Colusa': 22036, 'Contra Costa': 1155025, 'Del Norte': 27082, 'El Dorado': 193221, 'Fresno': 1017162, 'Glenn': 28805, 'Humboldt': 133985, 'Imperial': 179331, 'Inyo': 18839, 'Kern': 917673, 'Kings': 153443, 'Lake': 68140, 'Lassen': 28861, 'Los Angeles': 9663345, 'Madera': 160222, 'Marin': 254407, 'Mariposa': 17147, 'Mendocino': 90310, 'Merced': 291920, 'Modoc': 8500, 'Mono': 13066, 'Monterey': 430723, 'Napa': 133216, 'Nevada': 102037, 'Orange': 3135757, 'Placer': 417772, 'Plumas': 19131, 'Riverside': 2473931, 'Sacramento': 1588921, 'San Benito': 68175, 'San Bernardino': 2195611, 'San Diego': 3269973, 'San Francisco': 808988, 'San Joaquin': 800965, 'San Luis Obispo': 281635, 'San Mateo': 726353, 'Santa Barbara': 441257, 'Santa Clara': 1877592, 'Santa Cruz': 261547, 'Shasta': 180366, 'Sierra': 3217, 'Siskiyou': 43660, 'Solano': 449218, 'Sonoma': 481812, 'Stanislaus': 552999, 'Sutter': 97948, 'Tehama': 65498, 'Trinity': 15670, 'Tulare': 479401, 'Tuolumne': 54539, 'Ventura': 829749, 'Yolo': 216110, 'Yuba': 84310
}

def update_map():
    all_years_data = []
    for year, url in URLS.items():
        print(f"Processing {year}...")
        r = requests.get(url)
        df = pd.read_csv(io.StringIO(r.text))
        mask = (df['Cause_Desc'].str.contains('Accidents', case=False, na=False)) & (df['County'] != 'California')
        filtered = df[mask].copy()
        filtered['Count'] = pd.to_numeric(filtered['Count'], errors='coerce')
        yearly = filtered.groupby('County')['Count'].sum(min_count=1).reset_index()
        yearly['Year'] = str(year) # Force year to string
        all_years_data.append(yearly)

    final_df = pd.concat(all_years_data)
    final_df['Population'] = final_df['County'].map(POPS)
    final_df['Rate'] = (final_df['Count'] / final_df['Population']) * 100000
    export_df = final_df[['County', 'Year', 'Rate', 'Count']].round(2)
    export_df.columns = ['County', 'Year', 'Death Rate', 'Total Deaths']

    headers = {"Authorization": f"Bearer {API_KEY}"}
    
    # 1. THE RESET: Force metadata to clear out old column constraints
    reset_payload = {
        "metadata": {
            "data": {
                "horizontal-header": True,
                "column-format": {},
                "source-type": "uploaded-csv"
            }
        }
    }
    requests.patch(f"https://api.datawrapper.de/v3/charts/{CHART_ID}", headers=headers, json=reset_payload)

    # 2. UPLOAD DATA
    csv_data = export_df.to_csv(index=False)
    requests.put(f"https://api.datawrapper.de/v3/charts/{CHART_ID}/data", 
                 headers={**headers, "Content-Type": "text/csv"}, data=csv_data)
    
    # 3. PUBLISH
    requests.post(f"https://api.datawrapper.de/v3/charts/{CHART_ID}/publish", headers=headers)
    print("🚀 Full historical data force-synced!")

if __name__ == "__main__":
    update_map()
