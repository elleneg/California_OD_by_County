import pandas as pd
import requests
import io
import os
import json
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# 1. CDC DATA SOURCE (NCHS Provisional County-Level Drug Overdose Deaths)
CDC_URL = "https://data.cdc.gov/api/views/gb4e-yj24/rows.csv?accessType=DOWNLOAD"

# 2. Population Data
POPS = {'Alameda': 1628997, 'Alpine': 1159, 'Amador': 41412, 'Butte': 207183, 'Calaveras': 46332, 'Colusa': 22036, 'Contra Costa': 1155025, 'Del Norte': 27082, 'El Dorado': 193221, 'Fresno': 1017162, 'Glenn': 28805, 'Humboldt': 133985, 'Imperial': 179331, 'Inyo': 18839, 'Kern': 917673, 'Kings': 153443, 'Lake': 68140, 'Lassen': 28861, 'Los Angeles': 9663345, 'Madera': 160222, 'Marin': 254407, 'Mariposa': 17147, 'Mendocino': 90310, 'Merced': 291920, 'Modoc': 8500, 'Mono': 13066, 'Monterey': 430723, 'Napa': 133216, 'Nevada': 102037, 'Orange': 3135757, 'Placer': 417772, 'Plumas': 19131, 'Riverside': 2473931, 'Sacramento': 1588921, 'San Benito': 68175, 'San Bernardino': 2195611, 'San Diego': 3269973, 'San Francisco': 808988, 'San Joaquin': 800965, 'San Luis Obispo': 281635, 'San Mateo': 726353, 'Santa Barbara': 441257, 'Santa Clara': 1877592, 'Santa Cruz': 261547, 'Shasta': 180366, 'Sierra': 3217, 'Siskiyou': 43660, 'Solano': 449218, 'Sonoma': 481812, 'Stanislaus': 552999, 'Sutter': 97948, 'Tehama': 65498, 'Trinity': 15670, 'Tulare': 479401, 'Tuolumne': 54539, 'Ventura': 829749, 'Yolo': 216110, 'Yuba': 84310}

print("Downloading CDC Overdose Data...")
res = requests.get(CDC_URL)
df = pd.read_csv(io.StringIO(res.text), low_memory=False)

# 3. Filter for California and Clean Columns
df = df[df['ST_ABBREV'] == 'CA'].copy()
df['Count'] = pd.to_numeric(df['Provisional Drug Overdose Deaths'], errors='coerce').fillna(0)

# 4. Get the Annual Total
# Since these are 12-month rolling counts, we find the latest month per year 
# (e.g., Dec) to get the total for that calendar year.
idx = df.groupby(['COUNTYNAME', 'Year'])['Month'].idxmax()
final = df.loc[idx, ['COUNTYNAME', 'Year', 'Count']]
final = final.rename(columns={'COUNTYNAME': 'County'})

# 5. Calculate Rates
final['Population'] = final['County'].map(POPS)
final = final.dropna(subset=['Population'])
final['Death Rate'] = (final['Count'] / final['Population'] * 100000).round(2)

# Save local backup
final.to_csv("CA_Death_Rates.csv", index=False)

# 6. Push to Google Sheets
print("Connecting to Google Sheets...")
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds_json = os.environ.get('GOOGLE_SHEETS_JSON')
creds_dict = json.loads(creds_json)
creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
client = gspread.authorize(creds)

sheet = client.open("CA_Death_Data").sheet1
sheet.clear()
data_to_upload = [final.columns.values.tolist()] + final.values.tolist()
sheet.update('A1', data_to_upload)

print("✅ SUCCESS: CDC Overdose data updated to Google Sheets!")
