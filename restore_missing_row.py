
import pandas as pd
import os
import zipfile
import glob

# Paths
BACKUP_ZIP = "/Users/sumeetdas/Desktop/Nifty_Streamlit_bkp_01022026.zip"
CURRENT_DIR = "/Users/sumeetdas/Desktop/Stock Market/Nifty Dashboard"
TEMP_DIR = os.path.join(CURRENT_DIR, "temp_restore")

def restore_missing_date(missing_date="2026-02-01"):
    print(f"--- Restoring Missing Data for {missing_date} ---")
    
    # 1. Extract Backup
    if not os.path.exists(BACKUP_ZIP):
        print(f"Error: Backup file not found at {BACKUP_ZIP}")
        return

    print("Extracting backup...")
    with zipfile.ZipFile(BACKUP_ZIP, 'r') as zip_ref:
        zip_ref.extractall(TEMP_DIR)
        
    # 2. Iterate through all breadth CSVs in CURRENT_DIR
    csv_files = glob.glob(os.path.join(CURRENT_DIR, "breadth_*.csv")) + \
                glob.glob(os.path.join(CURRENT_DIR, "market_breadth_*.csv"))
                
    restored_count = 0
    
    for current_file in csv_files:
        filename = os.path.basename(current_file)
        backup_file = os.path.join(TEMP_DIR, filename)
        
        if not os.path.exists(backup_file):
            print(f"Skipping {filename}: Not found in backup.")
            continue
            
        # Load Dataframes
        try:
            df_curr = pd.read_csv(current_file)
            df_back = pd.read_csv(backup_file)
            
            # Check if missing date exists in Backup
            if 'Date' not in df_back.columns: continue
            
            row_to_restore = df_back[df_back['Date'] == missing_date]
            
            if row_to_restore.empty:
                # print(f"Info: {filename} had no data for {missing_date} in backup.")
                continue
                
            # Check if it already exists in Current (to avoid dupe)
            if not df_curr.empty and 'Date' in df_curr.columns:
                if missing_date in df_curr['Date'].values:
                    print(f"Skipping {filename}: {missing_date} already exists.")
                    continue
            
            # Merge and Sort
            print(f"Restoring row for {filename}...")
            df_merged = pd.concat([df_curr, row_to_restore], ignore_index=True)
            df_merged['Date'] = pd.to_datetime(df_merged['Date'])
            df_merged = df_merged.sort_values('Date').drop_duplicates(subset=['Date'], keep='last')
            
            # Save
            df_merged.to_csv(current_file, index=False)
            restored_count += 1
            
        except Exception as e:
            print(f"Error processing {filename}: {e}")

    print(f"--- Complete. Restored {restored_count} files. ---")
    
    # Cleanup
    import shutil
    try:
        shutil.rmtree(TEMP_DIR)
        print("Temp directory cleaned.")
    except:
        pass

if __name__ == "__main__":
    restore_missing_date()
