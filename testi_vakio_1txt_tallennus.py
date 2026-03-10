import requests
import zipfile
import os
from io import BytesIO

# URL of the ZIP file
url = "https://www.veikkaus.fi/odds_data/vakio_1.zip"
# Destination folder
dest_folder = r"C:\Users\mikko\OneDrive\vakioveikkaus_excel"

# Ensure the destination folder exists
os.makedirs(dest_folder, exist_ok=True)

# Fetch the ZIP file
response = requests.get(url)
if response.status_code == 200:
    with zipfile.ZipFile(BytesIO(response.content), 'r') as zip_ref:
        # Extract all files (overwrite existing ones)
        zip_ref.extractall(dest_folder)
    print("Download and extraction successful.")
else:
    print(f"Failed to download file, status code: {response.status_code}")