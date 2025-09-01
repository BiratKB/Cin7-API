# I convert the Invoicing Book to a non binary excel so I can automate the refresh in Power Bi
import os
import requests
import io
import pandas as pd
import logging

dropbox_url = os.environ.get("INVOICING_BOOK_URL")
if not dropbox_url:
    raise Exception("INVOICING_BOOK_URL not set")

# Extract filename from URL path and replace extension
filename = dropbox_url.split('/')[-1].split('?')[0]  # get last path part, remove query
output_file = filename.replace('.xlsb', '.xlsx')

# Download file
r = requests.get(dropbox_url)
r.raise_for_status()  # raise error if download failed

# Read XLSB and convert to XLSX
xlsb_data = io.BytesIO(r.content)
xlsb = pd.ExcelFile(xlsb_data, engine='pyxlsb')
with pd.ExcelWriter(output_file) as writer:
    for sheet in xlsb.sheet_names:
        df = xlsb.parse(sheet_name=sheet, header=None)
        df.to_excel(writer, sheet_name=sheet, index=False, header=False)

logging.info(f"Excel successfully written locally at {output_file}")

# Export the EXACT path for the workflow
gh_env = os.getenv('GITHUB_ENV')
output_file_abs = os.path.abspath(output_file)
output_file_base = os.path.basename(output_file)

if gh_env:
    with open(gh_env, "a") as env_file:
        env_file.write(f"ENV_CUSTOM_DATE_FILE={output_file_abs}\n")
        env_file.write(f"ENV_CUSTOM_DATE_FILE_NAME={output_file_base}\n")

    logging.info(f"Exported ENV_CUSTOM_DATE_FILE={output_file_abs}")
    logging.info(f"Exported ENV_CUSTOM_DATE_FILE_NAME={output_file_base}")
else:
    logging.warning("GITHUB_ENV not set; cannot export ENV_CUSTOM_DATE_FILE.")
    print(output_file_abs)
