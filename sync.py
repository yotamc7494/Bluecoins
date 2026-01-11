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
MASTER_TAB_NAME = "מאסטר" # Ensure this matches your tab name exactly

def run_sync():
    print("--- Starting Mirror with Master Logic ---")
    
    # 1. Auth
    info = json.loads(os.environ['GCP_SERVICE_ACCOUNT_JSON'])
    creds = Credentials.from_service_account_info(info, scopes=[
        'https://www.googleapis.com/auth/drive',
        'https://www.googleapis.com/auth/spreadsheets'
    ])

    # 2. Download Newest File (The part that was working)
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
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [row[0] for row in cursor.fetchall() if not row[0].startswith('sqlite_')]

    # 4. Connect to Sheets
    gc = gspread.authorize(creds)
    sh = gc.open(SHEET_NAME)
    existing_worksheets = {ws.title: ws for ws in sh.worksheets()}

    # 5. Loop Through Tables
    for table in tables:
        print(f"Syncing: {table}")
        try:
            df = pd.read_sql_query(f"SELECT * FROM {table}", conn)
            df = df.fillna("")

            # SPECIAL LOGIC: If this is the Transaction Table, prepare the Master data
            if table == "TRANSACTIONSTABLE":
                # Translate IDs to names for your Column A
                type_map = {2: 'New Account', 3: 'Expense', 4: 'Income', 5: 'Transfer'}
                
                # Create a copy for the Master sheet
                master_df = df.copy()
                # Insert the 'Type' as the first column
                master_df.insert(0, 'Type', master_df['transactionTypeID'].map(type_map).fillna('Other'))
                
                # Convert date formatting for Sheets compatibility
                if 'DATE' in master_df.columns:
                    master_df['DATE'] = pd.to_datetime(master_df['DATE'], unit='ms', errors='ignore').dt.strftime('%Y-%m-%d %H:%M:%S')

                # Update the מאסטר tab
                if MASTER_TAB_NAME in existing_worksheets:
                    ws_master = existing_worksheets[MASTER_TAB_NAME]
                    ws_master.clear()
                    ws_master.update([master_df.columns.values.tolist()] + master_df.values.tolist(), value_input_option='USER_ENTERED')
                    print(f"--- Updated Master Tab: {MASTER_TAB_NAME} ---")

            # Standard Mirroring (so your other calculations keep working)
            if table not in existing_worksheets:
                worksheet = sh.add_worksheet(title=table, rows="100", cols="20")
            else:
                worksheet = existing_worksheets[table]
            
            worksheet.clear()
            worksheet.update([df.columns.values.tolist()] + df.values.tolist(), value_input_option='USER_ENTERED')
            
        except Exception as e:
            print(f"Skipping {table}: {e}")

    conn.close()
    print("--- All Syncing Complete! ---")

if __name__ == "__main__":
    run_sync()
