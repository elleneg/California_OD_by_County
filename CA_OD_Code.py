import pandas as pd
import requests
import io
import os

# Configuration
API_KEY = os.getenv("CAL_OD_DATAWRAPPER")
CHART_ID = "kF18a" # Update this with your 5-character ID

URLS = [
    "https://data.chhs.ca.gov/dataset/58619b69-b3cb-41a7-8bfc-fc3a524a9dd4/resource/579cc04a-52d6-4c4c-b2df-ad901c9049b7/download/20260319_deaths_final_2014-2024_county_year_sup.csv",
    "https://data.chhs.ca.gov/dataset/58619b69-b3cb-41a7-8bfc-fc3a524a9dd4/resource/2e546f88-bba8-4d77-846a-7fb77846cac6/download/2026-03_deaths_provisional_county_month_sup.csv"
]

POPS = {
    'Alameda': 1628997, 'Alpine': 1159, 'Amador': 41412, 'Butte': 207183, 'Calaveras': 46332, 'Colusa': 22036, 'Contra Costa': 1155025, 'Del Norte': 27082, 'El Dorado': 193221, 'Fresno': 1017162, 'Glenn': 28805, 'Humboldt': 133985, 'Imperial': 179331, 'Inyo': 18839, 'Kern': 917673, 'Kings': 153443, 'Lake': 68140, 'Lassen': 28861, 'Los Angeles': 9663345, 'Madera': 160222, 'Marin': 254407, 'Mariposa': 17147, 'Mendocino': 90310, 'Merced': 291920, 'Modoc': 8500, 'Mono': 13066, 'Monterey': 430723, 'Napa': 133216, 'Nevada': 102037, 'Orange': 3135757, 'Placer': 417772, 'Plumas': 19131, 'Riverside': 2473931, 'Sacramento': 1588921, 'San Benito': 68175, 'San Bernardino': 2195611, 'San Diego': 3269973, 'San Francisco': 808988, 'San Joaquin': 800965, 'San Luis Obispo': 281635, 'San Mateo': 726353, 'Santa Barbara': 441257, 'Santa Clara': 1877592, 'Santa Cruz': 261547, 'Shasta': 180366, 'Sierra': 3217, 'Siskiyou': 43660, 'Solano': 449218, 'Sonoma': 481812, 'Stanislaus': 552999, 'Sutter': 97948, 'Tehama': 65498, 'Trinity': 15670, 'Tulare': 479401, 'Tuolumne': 54539, 'Ventura': 829749, 'Yolo': 216110, 'Yuba': 84310
}

def update_map():
    all_rows = []
    
    for url in URLS:
        print(f"Fetching data from: {url}")
        r = requests.get(url)
        # low_memory=False handles the mixed-type warning for large files
        df = pd.read_csv(io.StringIO(r.text), low_memory=False)
        
        # Clean column names
        df.columns = [c.strip() for c in df.columns]

        # Apply your exact filters
        mask = (
            (df['Strata'].str.strip() == 'Total Population') & 
            (df['Cause_Desc'].str.strip() == 'Accidents (unintentional injuries)')
        )
        filtered = df[mask].copy()
        
        # Keep only what we need for the pivot
        filtered = filtered[['County', 'Year', 'Count']]
        all_rows.append(filtered)

    # Combine historical and provisional
    combined = pd.concat(all_rows)
    combined['Count'] = pd.to_numeric(combined['Count'], errors='coerce')
    
    # Sum counts by County and Year (merges monthly provisional data into year totals)
    summed = combined.groupby(['County', 'Year'])['Count'].sum().reset_index()
    
    # Filter for 2021 onwards
    summed = summed[summed['Year'] >= 2021].copy()
    
    # Calculate Death Rate
    summed['Population'] = summed['County'].map(POPS)
    summed['Death Rate'] = (summed['Count'] / summed['Population'] * 100000).round(2)
    
    # Remove 'California' total and any NaN counties
    summed = summed.dropna(subset=['Population'])
    summed = summed[summed['County'] != 'California']

  # Use the 'summed' dataframe from your previous logic (before the pivot)
    # This keeps it in 'Long Format': County, Year, Death Rate, Count
    final_output = summed[['County', 'Year', 'Death Rate', 'Count']]
    
    # Save this to a CSV file that you will connect to Tableau
    final_output.to_csv("CA_Death_Rates.csv", index=False)
    print("🚀 Data ready for Tableau!")
