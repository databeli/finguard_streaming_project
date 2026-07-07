# Databricks notebook source
# DBTITLE 1,Generate fraud watchlist JSON files from CSV
import pandas as pd
import json
import time
from datetime import datetime
import os

# Read the CSV file from the same folder
csv_path = "/Workspace/Users/databeli14@gmail.com/finguard_project/fraud_watchlist_file_generator/fraud_watchlist.csv"
df = pd.read_csv(csv_path)

# Ensure entity_id is always a string for JSON serialization
df['entity_id'] = df['entity_id'].astype(str)

# Target volume path
output_path = "/Volumes/finguard/source/fraud_watchlist/source_data/"

# Ensure the output directory exists
dbutils.fs.mkdirs(output_path)

# Check for existing files and find the latest watchlist_id
latest_watchlist_id = None
try:
    existing_files = dbutils.fs.ls(output_path)
    if existing_files:
        print(f"Found {len(existing_files)} existing files in {output_path}")
        print("Scanning for latest watchlist_id...")
        
        max_id_num = 0
        for file_info in existing_files:
            if file_info.name.endswith('.json'):
                try:
                    # Read the JSON file
                    content = dbutils.fs.head(file_info.path, 10000)
                    data = json.loads(content)
                    watchlist_id = data.get('watchlist_id', '')
                    # Extract numeric part from watchlist_id (e.g., "WL000001" -> 1)
                    if watchlist_id.startswith('WL'):
                        id_num = int(watchlist_id[2:])
                        if id_num > max_id_num:
                            max_id_num = id_num
                            latest_watchlist_id = watchlist_id
                except Exception as e:
                    continue
        
        if latest_watchlist_id:
            print(f"Latest watchlist_id found: {latest_watchlist_id} (numeric: {max_id_num})")
            # Filter dataframe to start from next record using numeric comparison
            # Extract numeric part from dataframe's watchlist_id for comparison
            df['_id_num'] = df['watchlist_id'].str[2:].astype(int)
            df = df[df['_id_num'] > max_id_num].drop(columns=['_id_num']).reset_index(drop=True)
            print(f"Resuming from next record. {len(df)} rows remaining to process.\n")
        else:
            print("No valid watchlist_id found in existing files. Starting from beginning.\n")
    else:
        print("No existing files found. Starting from beginning.\n")
except Exception as e:
    print(f"Could not read existing files: {e}")
    print("Starting from beginning.\n")

if len(df) == 0:
    print("All records have already been processed!")
else:
    print(f"Starting to process {len(df)} rows from fraud_watchlist.csv")
    print(f"Writing JSON files to: {output_path}")
    print("\n" + "="*60 + "\n")

    # Process each row one at a time
    for index, row in df.iterrows():
        # Convert row to dictionary (column name as key, column value as value)
        row_dict = row.to_dict()
        
        # Ensure entity_id is a string in the dictionary
        row_dict['entity_id'] = str(row_dict['entity_id'])
        
        # Print the entire row being written
        print(f"Entire row being written: {row_dict}")
        
        # Create a unique filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        filename = f"fraud_watchlist_{timestamp}_{index}.json"
        file_path = f"{output_path}/{filename}"
        
        # Write JSON to file using dbutils.fs (required for Unity Catalog volumes)
        # Single-line JSON format
        json_content = json.dumps(row_dict)
        dbutils.fs.put(file_path, json_content, overwrite=True)
        
        print(f"Row {index + 1}/{len(df)}: Written {filename}")
        print(f"  Watchlist ID: {row_dict.get('watchlist_id')}")
        
        # Wait 5 seconds before processing next row (except for the last one)
        if index < len(df) - 1:
            print(f"  Waiting 5 seconds...\n")
            time.sleep(5)
    
    print("\n" + "="*60)
    print(f"\nCompleted! Generated {len(df)} JSON files in {output_path}")