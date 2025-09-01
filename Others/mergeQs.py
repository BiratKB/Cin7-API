import os
import io
import pandas as pd
import requests

# Get a new access token using the refresh token
DROPBOX_CLIENT_ID = os.environ["DROPBOX_ALL_ACCESS_APP_KEY"]
DROPBOX_CLIENT_SECRET = os.environ["DROPBOX_ALL_ACCESS_APP_SECRET"]
DROPBOX_REFRESH_TOKEN = os.environ["DROPBOX_ALL_ACCESS_REFRESH_TOKEN"]

def get_new_access_token():
    response = requests.post(
        "https://api.dropbox.com/oauth2/token",
        data={
            "grant_type": "refresh_token",
            "refresh_token": DROPBOX_REFRESH_TOKEN,
            "client_id": DROPBOX_CLIENT_ID,
            "client_secret": DROPBOX_CLIENT_SECRET
        },
    )
    response.raise_for_status()
    return response.json()["access_token"]

DROPBOX_ACCESS_TOKEN = get_new_access_token()

# Dropbox API endpoint to download files
DROPBOX_DOWNLOAD_URL = "https://content.dropboxapi.com/2/files/download"


# List of Dropbox file paths (in your Dropbox) to download and merge
dropbox_files = [
    "/Power BI Data Warehouse/Source of truth/2025/X 01 Cin7 Sales.xlsx",
    "/Power BI Data Warehouse/Source of truth/2025/X 02 Cin7 Sales.xlsx",
    "/Power BI Data Warehouse/Source of truth/2025/X 03 Cin7 Sales.xlsx",
    "/Power BI Data Warehouse/Source of truth/2025/X 04 Cin7 Sales.xlsx"
]



def download_file_from_dropbox(path):
    headers = {
        "Authorization": f"Bearer {DROPBOX_ACCESS_TOKEN}",
        "Dropbox-API-Arg": f'{{"path": "{path}"}}'
    }
    response = requests.post(DROPBOX_DOWNLOAD_URL, headers=headers)
    if response.status_code == 200:
        return io.BytesIO(response.content)
    else:
        raise Exception(f"Error downloading {path}: {response.status_code} {response.text}")

# Download first file to start the merge (it has headers)
merged_df = pd.read_excel(download_file_from_dropbox(dropbox_files[0]))
print(f"{dropbox_files[0]} -> {len(merged_df)} rows (+1 header)")

# Download and append remaining files (skip header rows)
for file_path in dropbox_files[1:]:
    try:
        df = pd.read_excel(download_file_from_dropbox(file_path), skiprows=1, header=None)

        # Skip empty files
        if df.empty:
            print(f"{file_path} is empty. Skipping.")
            continue

        df.columns = merged_df.columns  # ensure same columns
        print(f"{file_path} -> {len(df)} rows")
        merged_df = pd.concat([merged_df, df], ignore_index=True)

    except Exception as e:
        print(f"⚠️ Could not process {file_path}: {e}")

# Save merged file locally in GitHub Action runner
output_filename = "Others/Merged Cin7 Sales.xlsx"
merged_df.to_excel(output_filename, index=False)

print(f"\n Merge complete! Total rows in final file: {len(merged_df)}")

# Upload merged file back to Dropbox
with open(output_filename, "rb") as f:
    data = f.read()

upload_headers = {
    "Authorization": f"Bearer {DROPBOX_ACCESS_TOKEN}",
    "Dropbox-API-Arg": '{"path": "/Power BI Data Warehouse/Source of truth/2025/Merged Cin7 Sales.xlsx", "mode": "overwrite"}',
    "Content-Type": "application/octet-stream"
}

upload_response = requests.post("https://content.dropboxapi.com/2/files/upload", headers=upload_headers, data=data)

if upload_response.status_code == 200:
    print("✅ File uploaded to Dropbox successfully!")
else:
    print(f"❌ Error uploading to Dropbox: {upload_response.status_code}")
    print(upload_response.text)