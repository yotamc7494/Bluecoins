import os
import sqlite3
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import json

# Settings
SOURCE_SHEET_NAME = "BluecoinsDashboard"
MASTER_SHEET_ID = "1814vXhsCd1-krE6TCQNIjT27T992PZBHe2V9hfdKeWs" # Replace with your actual ID
MASTER_TAB_NAME = "DATA2"

def run_cross_file_sync():
    print("--- Starting Cross-File Sync to מאסטר ---")
    
    # 1. Auth
    info = json.loads(os.environ['GCP_SERVICE_ACCOUNT_JSON'])
    creds = Credentials.from_service_account_info(info, scopes=[
        'https://www.googleapis.com/auth/drive',
        'https://www.googleapis.com/auth/spreadsheets'
    ])
    gc = gspread.authorize(creds)

    # 2. Access the Source Sheet (BluecoinsDashboard)
    sh_source = gc.open(SOURCE_SHEET_NAME)
    ws_trans = sh_source.worksheet("TRANSACTIONSTABLE")
    
    # 3. Get all data from TRANSACTIONSTABLE
    # We get all values to ensure we can find Column G (index 6)
    all_data = ws_trans.get_all_values()
    if len(all_data) < 2:
        print("No transaction data found."); return

    # Map the IDs: 2=New Account, 3=Expense, 4=Income, 5=Transfer
    type_map = {'2': 'New Account', '3': 'Expense', '4': 'Income', '5': 'Transfer'}
    
    # Column G is index 6 (A=0, B=1, C=2, D=3, E=4, F=5, G=6)
    # We skip the header row (all_data[1:])
    mapped_column = [["Type"]] # Header for DATA2
    for row in all_data[1:]:
        # Get the ID from Col G, default to 'Other' if not in map
        tid = row[6] if len(row) > 6 else ""
        mapped_column.append([type_map.get(tid, "Other")])

    # 4. Update the מאסטר (DATA2) File
    try:
        sh_master = gc.open_by_key(MASTER_SHEET_ID)
        ws_master = sh_master.worksheet(MASTER_TAB_NAME)
        
        # We clear and update only Column A to keep your other decoding columns safe
        # 'A1' notation ensures we start from the very top
        ws_master.update('A1', mapped_column)
        
        print(f"--- Successfully mapped {len(mapped_column)-1} rows to DATA2! ---")
    except Exception as e:
        print(f"Error accessing Master Sheet: {e}")

if __name__ == "__main__":
    run_cross_file_sync()
