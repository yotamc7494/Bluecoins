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
DB_NAME = "bluecoins.sqlite" # Exactly as it appears in Drive

def run_sync():
    # 1. Auth using GitHub Secrets
    info = json.loads(os.environ['GCP_SERVICE_ACCOUNT_JSON'])
    creds = Credentials.from_service_account_info(info, scopes=[
        'https://www.googleapis.com/auth/drive',
        'https://www.googleapis.com/auth/spreadsheets'
    ])
    
    # 2. Find all .fydb files, ignore 0-byte files, and pick the newest
    drive_service = build('drive', 'v3', credentials=creds)
    
    query = "name contains '.fydb' and trashed = false"
    results = drive_service.files().list(
        q=query, 
        fields="files(id, name, modifiedTime, size)", # Added 'size'
        orderBy="modifiedTime desc" 
    ).execute()
    
    files = results.get('files', [])
    
    # Filter out files that are too small to be a real database (e.g., < 20KB)
    valid_files = [f for f in files if int(f.get('size', 0)) > 20000]

    if not valid_files:
        print("No valid (non-empty) .fydb files found!")
        return

    newest_file = valid_files[0]
    print(f"Syncing: {newest_file['name']} | Size: {int(newest_file['size'])/1024:.1f} KB")

    # 3. SQL Query (Bluecoins Schema)
    conn = sqlite3.connect("temp.db")
    # This query joins transactions with categories and accounts
    # --- ADD THIS TO sync.py TO SEE TABLE NAMES ---
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    print("Tables found in DB:", [row[0] for row in cursor.fetchall()])
    # ----------------------------------------------
    query = """
    SELECT 
        datetime(DATE/1000, 'unixepoch') as Date, 
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

    # 4. Update Sheet
    gc = gspread.authorize(creds)
    sh = gc.open(SHEET_NAME)
    wks = sh.worksheet("RawData")
    wks.clear()
    wks.update([df.columns.values.tolist()] + df.values.tolist())
    print("Sync Successful!")

if __name__ == "__main__":

    run_sync()


