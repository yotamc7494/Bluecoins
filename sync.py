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
# Absolute path for the temp DB file to prevent blank file creation
DB_PATH = os.path.join(os.getcwd(), "bluecoins_local.db")

def run_sync():
    print("--- Starting Sync Process ---")
    
    # 1. Auth using GitHub Secret
    try:
        info = json.loads(os.environ['GCP_SERVICE_ACCOUNT_JSON'])
        creds = Credentials.from_service_account_info(info, scopes=[
            'https://www.googleapis.com/auth/drive',
            'https://www.googleapis.com/auth/spreadsheets'
        ])
    except Exception as e:
        print(f"Auth Error: {e}")
        return

    # 2. Find and Download newest .fydb file
    drive_service = build('drive', 'v3', credentials=creds)
    
    # Filter for .fydb files, sort by newest, and ignore 0-byte files
    query = "name contains '.fydb' and trashed = false"
    results = drive_service.files().list(
        q=query, 
        fields="files(id, name, modifiedTime, size)",
        orderBy="modifiedTime desc"
    ).execute()
    
    files = [f for f in results.get('files', []) if int(f.get('size', 0)) > 20000]
    
    if not files:
        print("No valid .fydb files found in Drive!")
        return

    target_file = files[0]
    print(f"Downloading: {target_file['name']} ({int(target_file['size'])/1024:.1f} KB)")

    # 3. Explicit Download to Disk (Ensures file integrity)
    request = drive_service.files().get_media(fileId=target_file['id'])
    with io.FileIO(DB_PATH, 'wb') as f:
        downloader = MediaIoBaseDownload(f, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()
            print(f"Download Progress: {int(status.progress() * 100)}%")

    # 4. Verify SQLite Connection & Table Names
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [row[0] for row in cursor.fetchall()]
    print(f"Tables found in DB: {tables}")

    if 'TRANSACTIONSTABLE' not in tables:
        print("ERROR: TRANSACTIONSTABLE not found. Check if the file is a valid Bluecoins backup.")
        return

    # 5. Execute Data Extraction
    # Joins Transactions with Categories and Accounts
    query = """
    SELECT 
        datetime(DATE/1000, 'unixepoch', 'localtime') as Date, 
        ITEMNAME as Title, 
        AMOUNT/1000000.0 as Amount, 
        CATEGORYNAME as Category,
        ACCOUNTNAME as Account
    FROM TRANSACTIONSTABLE 
    LEFT JOIN CATEGORYTABLE ON TRANSACTIONSTABLE.CATEGORYID = CATEGORYTABLE.CATEGORYID
    LEFT JOIN ACCOUNTSTABLE ON TRANSACTIONSTABLE.ACCOUNTID = ACCOUNTSTABLE.ACCOUNTID
    WHERE TRANSACTIONTYPEID IN (1, 2)
    ORDER BY DATE DESC
    """
    df = pd.read_sql_query(query, conn)
    conn.close()

    # 6. Push to Google Sheets
    gc = gspread.authorize(creds)
    sh = gc.open(SHEET_NAME)
    wks = sh.worksheet("RawData")
    wks.clear()
    
    # Insert data starting from A1
    wks.update([df.columns.values.tolist()] + df.values.tolist())
    print(f"Successfully synced {len(df)} transactions to Google Sheets!")

if __name__ == "__main__":
    run_sync()
