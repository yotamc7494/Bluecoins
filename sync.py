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
    
    # Labels (One-to-Many)
    # LABELSTABLE: B=Label Name (1), C=TransactionID (2)
    ws_labels = sh_source.worksheet("LABELSTABLE")
    label_rows = ws_labels.get_all_values()
    label_map = {}
    for row in label_rows[1:]:
        if len(row) > 2:
            label_name = row[1]
            trans_id = row[2]
            if trans_id not in label_map:
                label_map[trans_id] = []
            label_map[trans_id].append(label_name)

    # Accounts (ID -> Name)
    ws_acc = sh_source.worksheet("ACCOUNTSTABLE")
    # A=ID (0), B=Name (1)
    account_lookup = {row[0]: row[1] for row in ws_acc.get_all_values()[1:] if len(row) > 1}

    # Categories & Titles (Existing Logic)
    group_lookup = {row[0]: row[1] for row in sh_source.worksheet("PARENTCATEGORYTABLE").get_all_values()[1:]}
    cat_lookup = {row[0]: {'name': row[1], 'parent': row[2]} for row in sh_source.worksheet("CHILDCATEGORYTABLE").get_all_values()[1:]}
    title_lookup = {row[0]: row[1] for row in sh_source.worksheet("ITEMTABLE").get_all_values()[1:]}

    # 2. PROCESS TRANSACTIONS
    ws_trans = sh_source.worksheet("TRANSACTIONSTABLE")
    trans_data = ws_trans.get_all_values()
    
    # Header Update: A(0) to M(12)
    header = ["Type", "Date", "Time", "Title", "Amount", "Currency", "Exchange Rate", 
              "Category Group", "Category", "Account", "Notes", "Labels", "Status"]
    final_output = [header]
    
    for row in trans_data[1:]:
        # A-D: Basic Info
        is_deleted = row[14] if len(row) > 14 else ""
        is_future = row[17] if len(row) > 17 else ""
        
        if is_deleted == '5' or is_future == '9':
            continue  # This skips the rest of the loop for this row
        t_id = row[0] # Transaction ID for Label Lookup
        tid_type = row[6]
        type_map = {'2': 'New Account', '3': 'Expense', '4': 'Income', '5': 'Transfer'}
        mapped_type = type_map.get(tid_type, "Other")
        
        # B/C: Date & Time
        try:
            dt_obj = datetime.strptime(row[5], "%d/%m/%Y %H:%M:%S")
            clean_date, clean_time = dt_obj.strftime("%d/%m/%Y"), dt_obj.strftime("%H:%M:%S")
        except: clean_date, clean_time = row[5], ""

        # D-G: Title, Amount, Currency, Rate
        mapped_title = title_lookup.get(row[1], "Unknown Item")
        amount = (float(row[2]) / 1000000.0) if row[2] else 0
        currency, rate = row[3], row[4]

        # H-I: Categories
        cat_id = row[7]
        cat_name = cat_lookup.get(cat_id, {}).get('name', "None")
        group_name = group_lookup.get(cat_lookup.get(cat_id, {}).get('parent'), "None")

        # J: Account (Index 8 in TRANSACTIONSTABLE)
        acc_id = row[8] if len(row) > 8 else ""
        acc_name = account_lookup.get(acc_id, "Unknown Account")

        # K: Notes (Index 9 in TRANSACTIONSTABLE)
        notes = row[9] if len(row) > 9 else ""

        # L: Labels (Joining with space)
        labels_list = label_map.get(t_id, [])
        joined_labels = " ".join(labels_list)

        # M: Status (Index 10 in TRANSACTIONSTABLE)
        status = row[10] if len(row) > 10 else ""

        final_output.append([
            mapped_type, clean_date, clean_time, mapped_title, amount, 
            currency, rate, group_name, cat_name, acc_name, notes, joined_labels, status
        ])

    # 3. PUSH TO MASTER
    try:
        sh_master = gc.open_by_key(MASTER_SHEET_ID)
        ws_master = sh_master.get_worksheet_by_id(MASTER_TAB_GID)
        ws_master.update('A1', final_output)
        print(f"--- Successfully synced {len(final_output)-1} rows to DATA2 ---")
    except Exception as e:
        print(f"Update failed: {e}")





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
    






















