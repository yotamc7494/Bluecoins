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
    
    # 2. Find all .fydb files and pick the newest
    drive_service = build('drive', 'v3', credentials=creds)
    
    # Query for files ending in .fydb that aren't in the trash
    query = "name contains '.fydb' and trashed = false"
    results = drive_service.files().list(
        q=query, 
        fields="files(id, name, modifiedTime)",
        orderBy="modifiedTime desc" # This puts the newest at the top
    ).execute()
    
    files = results.get('files', [])
    if not files:
        print("No .fydb files found!")
        return

    newest_file = files[0]
    file_id = newest_file['id']
    print(f"Syncing newest file: {newest_file['name']} (Modified: {newest_file['modifiedTime']})")

    # Download newest_file['id'] ... (rest of your download logic)

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

