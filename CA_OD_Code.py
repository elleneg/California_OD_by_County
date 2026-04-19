import pandas as pd
import requests
import io
import os

# --- CONFIGURATION ---
API_KEY = os.getenv("DW_API_KEY")
CHART_ID = "bodZ9"
DATA_URL = "https://data.chhs.ca.gov/dataset/death-profiles-by-county/resource/2e546f88-bba8-4d77-846a-7fb77846cac6/download/provisional_deaths_by_month_by_county.csv"

def update_map():
    try:
        print("Fetching data from CHHS...")
        r = requests.get(DATA_URL)
        
        # We add 'on_bad_lines' and 'skiprows' to handle government file formatting
        # If line 7 has the error, the data usually starts right after the header junk
        try:
            df = pd.read_csv(io.StringIO(r.text), on_bad_lines='skip')
        except:
            # Plan B: If standard reading fails, try skipping the first 5 rows of titles
            df = pd.read_csv(io.StringIO(r.text), skiprows=5, on_bad_lines='skip')

        # Cleanup: Remove any rows that are completely empty
        df = df.dropna(how='all')

        print(f"Columns found: {df.columns.tolist()}")

        # Search for overdose deaths. The state often uses 'Drug Overdose' or 'OD'
        # We look in Cause_Desc or Indicator columns
        column_to_check = 'Cause_Desc' if 'Cause_Desc' in df.columns else df.columns[1]
        
        mask = df[column_to_check].str.contains('overdose', case=False, na=False)
        filtered_df = df[mask].copy()

        if filtered_df.empty:
            print("❌ No overdose data found in the filtered rows.")
            return

        # Sum by County
        # Note: If the column is named 'Occurrence_County' or just 'County', we find it here
        county_col = 'County' if 'County' in df.columns else 'Occurrence_County'
        count_col = 'Count' if 'Count' in df.columns else 'Number_of_Deaths'

        map_data = filtered_df.groupby(county_col)[count_col].sum().reset_index()
        map_data.columns = ['County', 'Total Overdose Deaths']

        print(f"✅ Found {len(map_data)} counties.")

        # Push to Datawrapper
        headers = {"Authorization": f"Bearer {API_KEY}", "Content-Type": "text/csv"}
        upload_res = requests.put(
            f"https://api.datawrapper.de/v3/charts/{CHART_ID}/data",
            headers=headers,
            data=map_data.to_csv(index=False)
        )

        if upload_res.status_code == 204:
            requests.post(f"https://api.datawrapper.de/v3/charts/{CHART_ID}/publish", headers=headers)
            print("🚀 Map successfully updated!")
        else:
            print(f"❌ Upload failed: {upload_res.status_code}")

    except Exception as e:
        print(f"❌ Script Error: {e}")

if __name__ == "__main__":
    update_map()
