import os
import json
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# ... (Keep your data processing code above this) ...

# --- NEW GOOGLE SHEETS LOGIC ---
print("Connecting to Google Sheets...")

# 1. Setup the connection
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]

# This looks for the secret name we set in the YAML 'env' section
creds_json = os.environ.get('GOOGLE_SHEETS_JSON')
if not creds_json:
    raise ValueError("ERROR: GOOGLE_SHEETS_JSON secret not found!")

creds_dict = json.loads(creds_json)
creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
client = gspread.authorize(creds)

# 2. Open the sheet 
# (MAKE SURE your Google Sheet is named exactly 'CA_Death_Data')
try:
    sheet = client.open("CA_Death_Data").sheet1
except gspread.exceptions.SpreadsheetNotFound:
    print("ERROR: Could not find sheet named 'CA_Death_Data'. Did you share it with the robot email?")
    raise

# 3. Upload the data
sheet.clear()
data_to_upload = [final.columns.values.tolist()] + final.values.tolist()
sheet.update('A1', data_to_upload)

print("✅ SUCCESS: Data pushed to Google Sheets!")
