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
MASTER_SHEET_ID = "1814vXhsCd1-krE6TCQNIjT27T992PZBHe2V9hfdKeWs" # Replace with your actual ID
MASTER_TAB_ID = 911608347

def run_cross_file_sync():
    print("--- Starting Date & Type Sync to מאסטר ---")
    
    # 1. Auth
    info = json.loads(os.environ['GCP_SERVICE_ACCOUNT_JSON'])
    creds = Credentials.from_service_account_info(info, scopes=[
        'https://www.googleapis.com/auth/drive',
        'https://www.googleapis.com/auth/spreadsheets'
    ])
    gc = gspread.authorize(creds)

    # 2. Access the Source
    sh_source = gc.open(SOURCE_SHEET_NAME)
    ws_trans = sh_source.worksheet("TRANSACTIONSTABLE")
    all_data = ws_trans.get_all_values()
    
    if len(all_data) < 2:
        print("No data found."); return

    # Mapping Setup
    type_map = {'2': 'New Account', '3': 'Expense', '4': 'Income', '5': 'Transfer'}
    
    # Columns we need:
    # F = index 5 (Date)
    # G = index 6 (Type ID)
    
    final_output = [["Type", "Date"]] # Headers for Col A and B in DATA2
    
    for row in all_data[1:]:
        # 1. Decode Type (Col G)
        tid = row[6] if len(row) > 6 else ""
        mapped_type = type_map.get(tid, "Other")
        
        # 2. Reformat Date (Col F)
        raw_date = row[5] if len(row) > 5 else ""
        formatted_date = ""
        
        if raw_date:
            try:
                # Parse the current format (01/05/2023 00:00:00)
                dt_obj = datetime.strptime(raw_date, "%d/%m/%Y %H:%M:%S")
                # Format to your target (24/12/2025 13:04:02)
                formatted_date = dt_obj.strftime("%d/%m/%Y %H:%M:%S")
            except ValueError:
                formatted_date = raw_date # Keep original if parsing fails
        
        final_output.append([mapped_type, formatted_date])

    # 3. Update the מאסטר (DATA2) File
    try:
        sh_master = gc.open_by_key(MASTER_SHEET_ID)
        ws_master = sh_master.get_worksheet_by_id(MASTER_TAB_ID)
        
        # Update Columns A and B simultaneously
        ws_master.update('A1', final_output)
        
        print(f"--- Success! Mapped Type and Dates to {MASTER_TAB_NAME} ---")
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
    run_sync()
    run_cross_file_sync()
    






