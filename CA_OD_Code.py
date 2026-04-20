import pandas as pd
import requests
import io
import os

# --- CONFIGURATION ---
API_KEY = os.getenv("CAL_OD_DATAWRAPPER")
CHART_ID = "kF18a" # Make sure this is the new one!

# We are limiting this to the most recent years to ensure it actually works first
URLS = {
    2024: "https://data.chhs.ca.gov/dataset/58619b69-b3cb-41a7-8bfc-fc3a524a9dd4/resource/7141887e-d476-46c9-9403-16279f57876a/download/2024-12_deaths_provisional_county_month_sup.csv",
    2025: "https://data.chhs.ca.gov/dataset/58619b69-b3cb-41a7-8bfc-fc3a524a9dd4/resource/7141887e-d476-46c9-9403-16279f57876a/download/2025-12_deaths_provisional_county_month_sup.csv",
    2026: "https://data.chhs.ca.gov/dataset/58619b69-b3cb-41a7-8bfc-fc3a524a9dd4/resource/2e546f88-bba8-4d77-846a-7fb77846cac6/download/2026-03_deaths_provisional_county_month_sup.csv"
}

POPS = {
    'Alameda': 1628997, 'Alpine': 1159, 'Amador': 41412, 'Butte': 207183, 'Calaveras': 46332, 'Colusa': 22036, 'Contra Costa': 1155025, 'Del Norte': 27082, 'El Dorado': 193221, 'Fresno': 1017162, 'Glenn': 28805, 'Humboldt': 133985, 'Imperial': 179331, 'Inyo': 18839, 'Kern': 917673, 'Kings': 153443, 'Lake': 68140, 'Lassen': 28861, 'Los Angeles': 9663345, 'Madera': 160222, 'Marin': 254407, 'Mariposa': 17147, 'Mendocino': 90310, 'Merced': 291920, 'Modoc': 8500, 'Mono': 13066, 'Monterey': 430723, 'Napa': 133216, 'Nevada': 102037, 'Orange': 3135757, 'Placer': 417772, 'Plumas': 19131, 'Riverside': 2473931, 'Sacramento': 1588921, 'San Benito': 68175, 'San Bernardino': 2195611, 'San Diego': 3269973, 'San Francisco': 808988, 'San Joaquin': 800965, 'San Luis Obispo': 281635, 'San Mateo': 726353, 'Santa Barbara': 441257, 'Santa Clara': 1877592, 'Santa Cruz': 261547, 'Shasta': 180366, 'Sierra': 3217, 'Siskiyou': 43660, 'Solano': 449218, 'Sonoma': 481812, 'Stanislaus': 552999, 'Sutter': 97948, 'Tehama': 65498, 'Trinity': 15670, 'Tulare': 479401, 'Tuolumne': 54539, 'Ventura': 829749, 'Yolo': 216110, 'Yuba': 84310
}

def update_map():
    all_data = []
    for year, url in URLS.items():
        print(f"Trying {year}...")
        r = requests.get(url)
        df = pd.read_csv(io.StringIO(r.text))
        
        # Hardcoding the column names based on the 2024-2026 file structure
        df.columns = [c.strip() for c in df.columns]
        
        # We search specifically for the Accidents category
        target_col = 'Cause_Desc' if 'Cause_Desc' in df.columns else 'Cause Desc'
        county_col = 'County' if 'County' in df.columns else 'COUNTY'
        count_col = 'Count' if 'Count' in df.columns else 'COUNT'

        filtered = df[df[target_col].str.contains('Accidents', na=False)].copy()
        filtered[count_col] = pd.to_numeric(filtered[count_col], errors='coerce')
        
        summary = filtered.groupby(county_col)[count_col].sum(min_count=1).reset_index()
        summary.columns = ['County', 'Deaths']
        summary['Year'] = str(year)
        all_data.append(summary)

    final_df = pd.concat(all_data)
    final_df['Pop'] = final_df['County'].map(POPS)
    final_df['Death Rate'] = (final_df['Deaths'] / final_df['Pop'] * 100000).round(2)
    
    # Final data for Datawrapper
    dw_data = final_df[['County', 'Year', 'Death Rate', 'Deaths']]
    
    # Send to Datawrapper
    headers = {"Authorization": f"Bearer {API_KEY}", "Content-Type": "text/csv"}
    requests.put(f"https://api.datawrapper.de/v3/charts/{CHART_ID}/data", headers=headers, data=dw_data.to_csv(index=False))
    requests.post(f"https://api.datawrapper.de/v3/charts/{CHART_ID}/publish", headers={"Authorization": f"Bearer {API_KEY}"})
    print("✅ Done! Check your map now.")

if __name__ == "__main__":
    update_map()
