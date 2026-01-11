import os
import sqlite3
import pandas as pd
import gspread
from datetime import datetime
from google.oauth2.service_account import Credentials
import json
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io

# Settings
SOURCE_SHEET_NAME = "BluecoinsDashboard"
MASTER_SHEET_ID = "1814vXhsCd1-krE6TCQNIjT27T992PZBHe2V9hfdKeWs"
MASTER_TAB_GID = 911608347

def run_cross_file_sync():
    from datetime import datetime
import json
import gspread
from google.oauth2.service_account import Credentials

# Configuration
SOURCE_SHEET_NAME = "BluecoinsDashboard"
MASTER_SHEET_ID = "YOUR_MASTER_SHEET_ID_HERE"
MASTER_TAB_GID = 911608347 

def run_cross_file_sync():
    print("--- Starting Full Engine Sync to DATA2 ---")
    
    # 1. Auth
    info = json.loads(os.environ['GCP_SERVICE_ACCOUNT_JSON'])
    creds = Credentials.from_service_account_info(info, scopes=[
        'https://www.googleapis.com/auth/drive',
        'https://www.googleapis.com/auth/spreadsheets'
    ])
    gc = gspread.authorize(creds)

    # 2. Get Data from Source
    sh_source = gc.open(SOURCE_SHEET_NAME)
    
    # Title Lookup Dictionary
    ws_items = sh_source.worksheet("ITEMTABLE")
    item_data = ws_items.get_all_values()
    title_lookup = {row[0]: row[1] for row in item_data[1:] if len(row) > 1}

    # Transactions
    ws_trans = sh_source.worksheet("TRANSACTIONSTABLE")
    trans_data = ws_trans.get_all_values()
    
    if len(trans_data) < 2:
        print("No transactions found."); return

    # 3. Map the data
    type_map = {'2': 'New Account', '3': 'Expense', '4': 'Income', '5': 'Transfer'}
    final_output = [["Type", "Date", "Time", "Title", "Amount", "Currency", "Exchange Rate"]]
    
    for row in trans_data[1:]:
        # A: Type (Index 6)
        tid = row[6] if len(row) > 6 else ""
        mapped_type = type_map.get(tid, "Other")
        
        # B/C: Date & Time (Index 5)
        raw_date = row[5] if len(row) > 5 else ""
        clean_date, clean_time = "", ""
        if raw_date:
            try:
                dt_obj = datetime.strptime(raw_date, "%d/%m/%Y %H:%M:%S")
                clean_date = dt_obj.strftime("%d/%m/%Y")
                clean_time = dt_obj.strftime("%H:%M:%S")
            except ValueError:
                clean_date = raw_date

        # D: Title (Index 1 mapped)
        title_id = row[1] if len(row) > 1 else ""
        mapped_title = title_lookup.get(title_id, "Unknown Item")

        # E, F, G: Amount, Currency, Rate (Indices 2, 3, 4)
        # We divide amount by 1,000,000 to get the real decimal value
        raw_amount = row[2] if len(row) > 2 else "0"
        try:
            amount = float(raw_amount) / 1000000.0
        except ValueError:
            amount = 0
            
        currency = row[3] if len(row) > 3 else ""
        rate = row[4] if len(row) > 4 else "1"

        final_output.append([
            mapped_type, clean_date, clean_time, mapped_title, 
            amount, currency, rate
        ])

    # 4. Update DATA2
    try:
        print(f"Connecting to Master Sheet ID: {MASTER_SHEET_ID}...")
        print(f"Service Account Email: {creds.service_account_email}")
        sh_master = gc.open_by_key(MASTER_SHEET_ID.strip())
        print("Succesfully connected")
        worksheets = sh_master.worksheets()
        print("Available tabs:", [f"{ws.title} (ID: {ws.id})" for ws in worksheets])
        ws_master = sh_master.get_worksheet_by_id(MASTER_TAB_GID)
        ws_master.update('A1', final_output)
        print(f"--- Successfully synced {len(final_output)-1} rows to DATA2 ---")
    except Exception as e:
        print(f"Error: {e}")





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
    # run_sync()
    run_cross_file_sync()
    













