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
    print("--- Starting Full Engine Sync to DATA2 ---")
    
    try:
        # This pulls the JSON from your GitHub Secret / Environment Variable
        info = json.loads(os.environ['GCP_SERVICE_ACCOUNT_JSON'])
        
        creds = Credentials.from_service_account_info(info, scopes=[
            'https://www.googleapis.com/auth/drive',
            'https://www.googleapis.com/auth/spreadsheets'
        ])
        gc = gspread.authorize(creds)
    except KeyError:
        print("Error: GCP_SERVICE_ACCOUNT_JSON not found in environment.")
        return
    except Exception as e:
        print(f"Auth Error: {e}")
        return

    # 2. Get Data from Source
    sh_source = gc.open(SOURCE_SHEET_NAME)
    
    
    # 1. Map Parent Groups (ID -> Group Name)
    ws_parent = sh_source.worksheet("PARENTCATEGORYTABLE")
    parent_rows = ws_parent.get_all_values()
    # A=ID (0), B=Name (1)
    group_lookup = {row[0]: row[1] for row in parent_rows[1:] if len(row) > 1}

    # 2. Map Child Categories (ID -> {Name, ParentID})
    ws_child = sh_source.worksheet("CHILDCATEGORYTABLE")
    child_rows = ws_child.get_all_values()
    # A=ID (0), B=Name (1), C=ParentID (2)
    cat_lookup = {row[0]: {'name': row[1], 'parent': row[2]} for row in child_rows[1:] if len(row) > 2}

    # 3. Item & Account Lookups (Same as before)
    ws_items = sh_source.worksheet("ITEMTABLE")
    title_lookup = {row[0]: row[1] for row in ws_items.get_all_values()[1:] if len(row) > 1}
    
    ws_accounts = sh_source.worksheet("ACCOUNTSTABLE")
    account_lookup = {row[0]: row[1] for row in ws_accounts.get_all_values()[1:] if len(row) > 1}

    # 4. Process Transactions
    ws_trans = sh_source.worksheet("TRANSACTIONSTABLE")
    trans_data = ws_trans.get_all_values()
    
    # Headers for DATA2: A-I
    final_output = [["Type", "Date", "Time", "Title", "Amount", "Currency", "Exchange Rate", "Category Group", "Category"]]
    
    for row in trans_data[1:]:
        # Existing Mappings
        tid = row[6] # transactionTypeID
        type_map = {'2': 'New Account', '3': 'Expense', '4': 'Income', '5': 'Transfer'}
        mapped_type = type_map.get(tid, "Other")
        
        # Date/Time Logic (Index 5)
        raw_date = row[5]
        clean_date, clean_time = "", ""
        try:
            dt_obj = datetime.strptime(raw_date, "%d/%m/%Y %H:%M:%S")
            clean_date = dt_obj.strftime("%d/%m/%Y")
            clean_time = dt_obj.strftime("%H:%M:%S")
        except: clean_date = raw_date

        mapped_title = title_lookup.get(row[1], "Unknown Item")
        amount = (float(row[2]) / 1000000.0) if row[2] else 0
        currency, rate = row[3], row[4]

        # --- CATEGORY LOGIC (Col H & I) ---
        # TRANSACTIONSTABLE index 7 is typically categoryID (Col H in raw)
        cat_id = row[7] if len(row) > 7 else ""
        
        category_name = "None"
        group_name = "None"
        
        if cat_id in cat_lookup:
            category_name = cat_lookup[cat_id]['name']
            parent_id = cat_lookup[cat_id]['parent']
            group_name = group_lookup.get(parent_id, "Unknown Group")

        final_output.append([
            mapped_type, clean_date, clean_time, mapped_title, 
            amount, currency, rate, group_name, category_name
        ])

    # 5. Push to DATA2 GID: 911608347
    try:
        sh_master = gc.open_by_key(MASTER_SHEET_ID)
        ws_master = sh_master.get_worksheet_by_id(MASTER_TAB_GID)
        ws_master.update('A1', final_output)
        print("--- Master Sync Success! ---")
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
    




















