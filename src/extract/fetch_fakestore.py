import requests
import pandas as pd
from google.cloud import storage
from datetime import datetime
import os
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


BUCKET_NAME = "ecomm-raw-data"  
API_URL = "https://fakestoreapi.com/products"

def fetch_and_upload():
    os.makedirs("data", exist_ok=True)
    print("📦 Fetching data from FakeStore API...")
    
    response = requests.get(API_URL, timeout=10, verify=False)
    response.raise_for_status()
    data = response.json()
    df = pd.json_normalize(data)

    filename = f"products_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    local_path = f"data/{filename}"
    df.to_csv(local_path, index=False)
    print(f"✅ Saved locally as {local_path}")

    storage_client = storage.Client(project="lucid-destiny-475616-t5")
    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(f"raw/{filename}")
    blob.upload_from_filename(local_path)
    print(f"✅ Uploaded to gs://{BUCKET_NAME}/raw/{filename}")

if __name__ == "__main__":
    fetch_and_upload()
