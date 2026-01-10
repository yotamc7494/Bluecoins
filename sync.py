import os
import sqlite3
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io
import json

# Settings
SHEET_NAME = "BluecoinsDashboard"
DB_PATH = os.path.join(os.getcwd(), "bluecoins_mirror.db")

def run_sync():
    print("--- Starting Full Database Mirror ---")
    
    # 1. Auth
    info = json.loads(os.environ['GCP_SERVICE_ACCOUNT_JSON'])
    creds = Credentials.from_service_account_info(info, scopes=[
        'https://www.googleapis.com/auth/drive',
        'https://www.googleapis.com/auth/spreadsheets'
    ])

    # 2. Find and Download Newest File
    drive_service = build('drive', 'v3', credentials=creds)
    query = "name contains '.fydb' and trashed = false"
    results = drive_service.files().list(q=query, fields="files(id, name, size)", orderBy="modifiedTime desc").execute()
    
    files = [f for f in results.get('files', []) if int(f.get('size', 0)) > 20000]
    if not files:
        print("No valid database found!"); return

    request = drive_service.files().get_media(fileId=files[0]['id'])
    with io.FileIO(DB_PATH, 'wb') as f:
        downloader = MediaIoBaseDownload(f, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()

    # 3. Connect to Database
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Get all table names
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [row[0] for row in cursor.fetchall() if not row[0].startswith('sqlite_')]
    print(f"Found {len(tables)} tables: {tables}")

    # 4. Connect to Google Sheets
    gc = gspread.authorize(creds)
    sh = gc.open(SHEET_NAME)
    existing_worksheets = {ws.title: ws for ws in sh.worksheets()}

    # 5. Loop through every table and update/create tabs
    for table in tables:
        print(f"Syncing table: {table}...")
        try:
            df = pd.read_sql_query(f"SELECT * FROM {table}", conn)
            
            # Clean up data for Google Sheets (convert dates/handle NaNs)
            df = df.fillna("")
            for col in df.select_dtypes(['datetime', 'datetimetz']).columns:
                df[col] = df[col].astype(str)

            # Create tab if missing
            if table not in existing_worksheets:
                worksheet = sh.add_worksheet(title=table, rows="100", cols="20")
                print(f"Created new tab: {table}")
            else:
                worksheet = existing_worksheets[table]
            
            # Upload data
            worksheet.clear()
            worksheet.update([df.columns.values.tolist()] + df.values.tolist())
        except Exception as e:
            print(f"Could not sync {table}: {e}")

    conn.close()
    print("--- Mirror Complete! ---")

if __name__ == "__main__":
    run_sync()
