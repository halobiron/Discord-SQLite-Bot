#!/usr/bin/env python3
"""
File-based storage manager for monitoring system
Alternative to Google Sheets using CSV/JSON files
"""
import csv
import json
import os
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any
import pandas as pd

class FileStorage:
    def __init__(self, data_dir: str = "data"):
        """Initialize file storage with data directory"""
        self.data_dir = data_dir
        self.ensure_directory()
    
    def ensure_directory(self):
        """Create data directory if it doesn't exist"""
        if not os.path.exists(self.data_dir):
            os.makedirs(self.data_dir)
            logging.info(f"Created data directory: {self.data_dir}")
    
    def save_fixed_rate_data(self, data: List[Dict], sheet_type: str = "5m"):
        """Save fixed rate data to CSV file"""
        filename = f"fixed_rate_{sheet_type}_{datetime.now().strftime('%Y%m')}.csv"
        filepath = os.path.join(self.data_dir, filename)
        
        # Check if file exists to write header
        file_exists = os.path.exists(filepath)
        
        with open(filepath, 'a', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['timestamp', 'station', 'fixed_rate', 'users', 'fixed_users']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            if not file_exists:
                writer.writeheader()
            
            writer.writerows(data)
        
        logging.info(f"Saved {len(data)} fixed rate records to {filepath}")
    
    def save_station_status(self, data: List[Dict], file_type: str = "temp"):
        """Save station status data"""
        if file_type == "temp":
            # Overwrite temp file
            filename = "station_status_temp.json"
            filepath = os.path.join(self.data_dir, filename)
            
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        
        elif file_type == "history":
            # Append to history file
            filename = f"station_status_history_{datetime.now().strftime('%Y%m')}.csv"
            filepath = os.path.join(self.data_dir, filename)
            
            file_exists = os.path.exists(filepath)
            
            with open(filepath, 'a', newline='', encoding='utf-8') as csvfile:
                fieldnames = ['stationId', 'stationName', 'identificationName', 
                             'connectStatus', 'scanTime', 'errorStartTime']
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                
                if not file_exists:
                    writer.writeheader()
                
                writer.writerows(data)
        
        logging.info(f"Saved {len(data)} station status records to {filepath}")
    
    def get_previous_station_status(self) -> Dict:
        """Get previous station status from temp file"""
        filepath = os.path.join(self.data_dir, "station_status_temp.json")
        
        if not os.path.exists(filepath):
            return {}
        
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Convert to expected format
            result = {}
            for record in data:
                result[record["stationId"]] = {
                    "stationName": record["stationName"],
                    "identificationName": record["identificationName"],
                    "connectStatus": record["connectStatus"],
                    "scanTime": record["scanTime"],
                    "errorStartTime": record["errorStartTime"]
                }
            
            return result
        
        except Exception as e:
            logging.error(f"Error reading previous station status: {e}")
            return {}
    
    def get_fixed_rate_data(self, sheet_type: str = "5m", hours_back: int = 1) -> List[Dict]:
        """Get fixed rate data from CSV files"""
        # Look for current month and previous month files
        current_month = datetime.now().strftime('%Y%m')
        prev_month = (datetime.now() - timedelta(days=32)).strftime('%Y%m')
        
        files_to_check = [
            f"fixed_rate_{sheet_type}_{current_month}.csv",
            f"fixed_rate_{sheet_type}_{prev_month}.csv"
        ]
        
        all_data = []
        cutoff_time = datetime.now() - timedelta(hours=hours_back)
        
        for filename in files_to_check:
            filepath = os.path.join(self.data_dir, filename)
            if os.path.exists(filepath):
                try:
                    with open(filepath, 'r', encoding='utf-8') as csvfile:
                        reader = csv.DictReader(csvfile)
                        for row in reader:
                            try:
                                row_time = datetime.strptime(row['timestamp'], '%Y-%m-%d %H:%M:%S')
                                if row_time >= cutoff_time:
                                    # Convert to expected format
                                    all_data.append({
                                        "Timestamp": row['timestamp'],
                                        "Station": row['station'],
                                        "Fixed Rate (%)": row['fixed_rate'],
                                        "Users": row['users'],
                                        "Fixed Users": row['fixed_users']
                                    })
                            except ValueError:
                                continue
                
                except Exception as e:
                    logging.warning(f"Error reading {filepath}: {e}")
        
        return sorted(all_data, key=lambda x: x['Timestamp'])
    
    def clear_old_files(self, months_to_keep: int = 3):
        """Remove old data files"""
        cutoff_date = datetime.now() - timedelta(days=months_to_keep * 30)
        cutoff_month = cutoff_date.strftime('%Y%m')
        
        for filename in os.listdir(self.data_dir):
            if filename.endswith('.csv') and len(filename.split('_')) >= 3:
                try:
                    # Extract date from filename
                    parts = filename.split('_')
                    if len(parts[-1]) == 10:  # YYYYMM.csv
                        file_month = parts[-1][:6]
                        if file_month < cutoff_month:
                            filepath = os.path.join(self.data_dir, filename)
                            os.remove(filepath)
                            logging.info(f"Removed old file: {filename}")
                except Exception as e:
                    logging.warning(f"Error processing file {filename}: {e}")
    
    def backup_to_archive(self):
        """Create backup of all data files"""
        backup_dir = os.path.join(self.data_dir, "backups")
        if not os.path.exists(backup_dir):
            os.makedirs(backup_dir)
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_file = os.path.join(backup_dir, f"backup_{timestamp}.zip")
        
        import zipfile
        with zipfile.ZipFile(backup_file, 'w') as zipf:
            for filename in os.listdir(self.data_dir):
                if filename.endswith(('.csv', '.json')) and not filename.startswith('backup_'):
                    filepath = os.path.join(self.data_dir, filename)
                    zipf.write(filepath, filename)
        
        logging.info(f"Created backup: {backup_file}")
        return backup_file
    
    def get_storage_stats(self) -> Dict:
        """Get storage statistics"""
        stats = {
            "total_files": 0,
            "total_size_mb": 0,
            "file_types": {}
        }
        
        for filename in os.listdir(self.data_dir):
            filepath = os.path.join(self.data_dir, filename)
            if os.path.isfile(filepath):
                stats["total_files"] += 1
                
                file_size = os.path.getsize(filepath) / (1024 * 1024)  # MB
                stats["total_size_mb"] += file_size
                
                ext = filename.split('.')[-1]
                if ext not in stats["file_types"]:
                    stats["file_types"][ext] = {"count": 0, "size_mb": 0}
                
                stats["file_types"][ext]["count"] += 1
                stats["file_types"][ext]["size_mb"] += file_size
        
        stats["total_size_mb"] = round(stats["total_size_mb"], 2)
        for ext in stats["file_types"]:
            stats["file_types"][ext]["size_mb"] = round(stats["file_types"][ext]["size_mb"], 2)
        
        return stats

# Create global file storage instance
file_storage = FileStorage()
