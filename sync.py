import os
import sqlite3
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import json

# Settings
SHEET_NAME = "BluecoinsDashboard"
TARGET_TAB = "DATA2" # Or "מאסטר" - ensure this matches your tab name exactly

def run_sync():
    # 1. Auth
    info = json.loads(os.environ['GCP_SERVICE_ACCOUNT_JSON'])
    creds = Credentials.from_service_account_info(info, scopes=[
        'https://www.googleapis.com/auth/drive',
        'https://www.googleapis.com/auth/spreadsheets'
    ])
    
    # 2. Connect to local SQLite (assuming you've already downloaded it in the previous step)
    conn = sqlite3.connect("bluecoins_mirror.db")
    
    # 3. Query with Translation Logic
    # We map the IDs directly in SQL for speed
    query = """
    SELECT 
        CASE 
            WHEN transactionTypeID = 2 THEN 'New Account'
            WHEN transactionTypeID = 3 THEN 'Expense'
            WHEN transactionTypeID = 4 THEN 'Income'
            WHEN transactionTypeID = 5 THEN 'Transfer'
            ELSE 'Other'
        END AS Type,
        strftime('%Y-%m-%d', DATE/1000, 'unixepoch', 'localtime') as Date,
        ITEMNAME as Title,
        AMOUNT / 1000000.0 as Amount,
        ACCOUNTID as AccountID,
        CATEGORYID as CategoryID,
        notes as Notes,
        reminderTransaction as Reminder
    FROM TRANSACTIONSTABLE
    WHERE deletedTransaction = 6
    ORDER BY DATE DESC
    """
    
    df = pd.read_sql_query(query, conn)
    df = df.fillna("")

    # 4. Push to Google Sheets
    gc = gspread.authorize(creds)
    sh = gc.open(SHEET_NAME)
    worksheet = sh.worksheet(TARGET_TAB)
    
    # Clear existing data but keep headers (assuming headers are in Row 1)
    # We overwrite from A2 downwards
    worksheet.clear()
    
    # Update with headers + data
    # Using 'USER_ENTERED' so Sheets treats 'Amount' as a number and 'Date' as a date
    worksheet.update([df.columns.values.tolist()] + df.values.tolist(), value_input_option='USER_ENTERED')
    
    conn.close()
    print(f"--- Sync to {TARGET_TAB} Complete ---")

if __name__ == "__main__":
    run_sync()
