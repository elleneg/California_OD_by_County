# 1. New Data Source (California Overdose Surveillance Data)
# This link is the specific "All Drug Overdose Deaths" dataset
URL = "https://data.chhs.ca.gov/dataset/58619b69-b3cb-41a7-8bfc-fc3a524a9dd4/resource/2e546f88-bba8-4d77-846a-7fb77846cac6/download/2026-03_deaths_provisional_county_month_sup.csv"

# 2. Update the processing logic
all_data = []
res = requests.get(URL)
df = pd.read_csv(io.StringIO(res.text), low_memory=False)

# Clean column names
df.columns = [c.strip() for c in df.columns]

# The NEW filter: Looking for 'Drug-Induced' or 'Drug Overdose'
# We use a case-insensitive search to be safe
mask = (df['Strata'] == 'Total Population') & (df['Cause_Desc'].str.contains('Drug', case=False, na=False))
final = df[mask].copy()

# Note: The Overdose dataset often already has a 'Rate' column, 
# but we will keep your calculation to ensure consistency.
final['Count'] = pd.to_numeric(final['Count'], errors='coerce')
final = final.groupby(['County', 'Year'])['Count'].sum().reset_index()

# ... (Keep the rest of your Population mapping and Google Sheets code the same)
