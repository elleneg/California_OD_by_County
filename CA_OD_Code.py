import pandas as pd
import requests
import io
import os

API_KEY = os.getenv("CAL_OD_DATAWRAPPER")
CHART_ID = "YOUR_NEW_ID" # Make sure your 5-character ID is here

URLS = [
    "https://data.chhs.ca.gov/dataset/58619b69-b3cb-41a7-8bfc-fc3a524a9dd4/resource/579cc04a-52d6-4c4c-b2df-ad901c9049b7/download/20260319_deaths_final_2014-2024_county_year_sup.csv",
    "https://data.chhs.ca.gov/dataset/58619b69-b3cb-41a7-8bfc-fc3a524a9dd4/resource/2e546f88-bba8-4d77-846a-7fb77846cac6/download/2026-03_deaths_provisional_county_month_sup.csv"
]

POPS = {
    'Alameda': 1628997, 'Alpine': 1159, 'Amador': 41412, 'Butte': 207183, 'Calaveras': 46332, 'Colusa': 22036, 'Contra Costa': 1155025, 'Del Norte': 27082, 'El Dorado': 193221, 'Fresno': 1017162, 'Glenn': 28805, 'Humboldt': 133985, 'Imperial': 179331, 'Inyo': 18839, 'Kern': 917673, 'Kings': 153443, 'Lake': 68140, 'Lassen': 28861, 'Los Angeles': 9663345, 'Madera': 160222, 'Marin': 254407, 'Mariposa': 17147, 'Mendocino': 90310, 'Merced': 291920, 'Modoc': 8500, 'Mono': 13066, 'Monterey': 430723, 'Napa': 133216, 'Nevada': 102037, 'Orange': 3135757, 'Placer': 417772, 'Plumas': 19131, 'Riverside': 2473931, 'Sacramento': 1588921, 'San Benito': 68175, 'San Bernardino': 2195611, 'San Diego': 3269973, 'San Francisco': 808988, 'San Joaquin': 800965, 'San Luis Obispo': 281635, 'San Mateo': 726353, 'Santa Barbara': 441257, 'Santa Clara': 1877592, 'Santa Cruz': 261547, 'Shasta': 180366, 'Sierra': 3217, 'Siskiyou': 43660, 'Solano': 449218, 'Sonoma': 481812, 'Stanislaus': 552999, 'Sutter': 97948, 'Tehama': 65498, 'Trinity': 15670, 'Tulare': 479401, 'Tuolumne': 54539, 'Ventura': 829749, 'Yolo': 216110, 'Yuba': 84310
}

def update_map():
    # 1. Process Final Data (2014-2024)
    r1 = requests.get(URLS[0])
    df_final = pd.read_csv(io.StringIO(r1.text))
    
    # Filter for Total Population and Accidents
    df_final = df_final[
        (df_final['Gender'] == 'Total Population') & 
        (df_final['Cause_Desc'].str.contains('Accidents', na=False))
    ].copy()
    
    # 2. Process Provisional Data (2025-2026)
    r2 = requests.get(URLS[1])
    df_prov = pd.read_csv(io.StringIO(r2.text))
    
    # Apply same filters
    df_prov = df_prov[
        (df_prov['Gender'] == 'Total Population') & 
        (df_prov['Cause_Desc'].str.contains('Accidents', na=False))
    ].copy()
    
    # Standardize and Combine
    df_final = df_final[['County', 'Year', 'Count']]
    df_prov = df_prov.groupby(['County', 'Year'])['Count'].sum().reset_index()
    
    combined = pd.concat([df_final, df_prov])
    combined = combined[(combined['
