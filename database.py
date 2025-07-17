#!/usr/bin/env python3
"""
SQLite database manager for monitoring system
Replaces Google Sheets with local SQLite database
"""
import sqlite3
import logging
import os
from datetime import datetime
from typing import List, Dict, Any
import json

class MonitoringDatabase:
    def __init__(self, db_path: str = "monitoring.db"):
        """Initialize SQLite database connection"""
        self.db_path = db_path
        self.init_database()
    
    def init_database(self):
        """Create tables if they don't exist"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # Fixed Rate tables
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS fixed_rate_5m (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    station TEXT NOT NULL,
                    fixed_rate REAL NOT NULL,
                    users INTEGER NOT NULL,
                    fixed_users INTEGER NOT NULL,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS fixed_rate_15m (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    station TEXT NOT NULL,
                    fixed_rate REAL NOT NULL,
                    users REAL NOT NULL,
                    fixed_users REAL NOT NULL,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS fixed_rate_hourly (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    station TEXT NOT NULL,
                    fixed_rate REAL NOT NULL,
                    users REAL NOT NULL,
                    fixed_users REAL NOT NULL,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(timestamp, station)
                )
            ''')
            
            # Station Status tables
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS station_status_temp (
                    station_id TEXT PRIMARY KEY,
                    station_name TEXT NOT NULL,
                    identification_name TEXT,
                    connect_status INTEGER NOT NULL,
                    scan_time TEXT NOT NULL,
                    error_start_time TEXT,
                    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS station_status_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    station_id TEXT NOT NULL,
                    station_name TEXT NOT NULL,
                    identification_name TEXT,
                    connect_status INTEGER NOT NULL,
                    scan_time TEXT NOT NULL,
                    error_start_time TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS station_status_daily (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    date TEXT NOT NULL,
                    station_id TEXT NOT NULL,
                    station_name TEXT NOT NULL,
                    identification_name TEXT,
                    offline_count INTEGER DEFAULT 0,
                    no_data_count INTEGER DEFAULT 0,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(date, station_id)
                )
            ''')
            
            # Create indexes for better performance
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_fixed_rate_5m_timestamp ON fixed_rate_5m(timestamp)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_fixed_rate_5m_station ON fixed_rate_5m(station)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_station_status_history_timestamp ON station_status_history(scan_time)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_station_status_history_station ON station_status_history(station_id)')
            
            conn.commit()
            logging.info("Database initialized successfully")
    
    def save_fixed_rate_data(self, data: List[Dict], table_type: str = "5m"):
        """Save fixed rate data to appropriate table"""
        table_name = f"fixed_rate_{table_type}"
        
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            for record in data:
                # Convert comma-separated numbers back to float
                fixed_rate = float(str(record["fixed_rate"]).replace(",", "."))
                users = float(str(record["users"]).replace(",", "."))
                fixed_users = float(str(record["fixed_users"]).replace(",", "."))
                
                cursor.execute(f'''
                    INSERT INTO {table_name} (timestamp, station, fixed_rate, users, fixed_users)
                    VALUES (?, ?, ?, ?, ?)
                ''', (record["timestamp"], record["station"], fixed_rate, users, fixed_users))
            
            conn.commit()
            logging.info(f"Saved {len(data)} fixed rate records to {table_name}")
    
    def save_station_status_temp(self, data: List[Dict]):
        """Save station status to temp table (overwrite existing)"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # Clear temp table
            cursor.execute('DELETE FROM station_status_temp')
            
            # Insert new data
            for record in data:
                cursor.execute('''
                    INSERT OR REPLACE INTO station_status_temp 
                    (station_id, station_name, identification_name, connect_status, scan_time, error_start_time)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', (
                    record["stationId"],
                    record["stationName"], 
                    record["identificationName"],
                    record["connectStatus"],
                    record["scanTime"],
                    record["errorStartTime"]
                ))
            
            conn.commit()
            logging.info(f"Saved {len(data)} station status records to temp table")
    
    def save_station_status_history(self, data: List[Dict]):
        """Save station status to history table (append)"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            for record in data:
                cursor.execute('''
                    INSERT INTO station_status_history 
                    (station_id, station_name, identification_name, connect_status, scan_time, error_start_time)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', (
                    record["stationId"],
                    record["stationName"],
                    record["identificationName"], 
                    record["connectStatus"],
                    record["scanTime"],
                    record["errorStartTime"]
                ))
            
            conn.commit()
            logging.info(f"Saved {len(data)} station status records to history table")
    
    def get_previous_station_status(self) -> Dict:
        """Get previous station status data"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT station_id, station_name, identification_name, connect_status, scan_time, error_start_time
                FROM station_status_temp
            ''')
            
            result = {}
            for row in cursor.fetchall():
                result[str(row[0])] = {  # Convert station_id to string for consistent key lookup
                    "stationName": row[1],
                    "identificationName": row[2],
                    "connectStatus": row[3],
                    "scanTime": row[4],
                    "errorStartTime": row[5]
                }
            
            return result
    
    def get_fixed_rate_data(self, table_type: str = "5m", hours_back: int = 1) -> List[Dict]:
        """Get fixed rate data from specified table"""
        table_name = f"fixed_rate_{table_type}"
        
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            # MODIFIED: Added 'localtime' to ensure the time window is based on local time
            query = f'''
                SELECT timestamp, station, fixed_rate, users, fixed_users
                FROM {table_name}
                WHERE datetime(timestamp) >= datetime('now', '-{hours_back} hours', 'localtime')
                ORDER BY timestamp
            '''
            cursor.execute(query)
            
            result = []
            for row in cursor.fetchall():
                result.append({
                    "Timestamp": row[0],
                    "Station": row[1],
                    "Fixed Rate (%)": f"{row[2]:.3f}".replace(".", ","),
                    "Users": f"{row[3]:.3f}".replace(".", ","),
                    "Fixed Users": f"{row[4]:.3f}".replace(".", ",")
                })
            
            return result
    
    def clear_old_data(self, days_to_keep: int = 30):
        """Clear old data to keep database size manageable"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # Clear old 5m data (keep only recent data)
            cursor.execute('''
                DELETE FROM fixed_rate_5m 
                WHERE datetime(timestamp) < datetime('now', '-3 days')
            ''')
            
            # Clear old history data
            cursor.execute(f'''
                DELETE FROM station_status_history 
                WHERE datetime(scan_time) < datetime('now', '-{days_to_keep} days')
            ''')
            
            conn.commit()
            logging.info(f"Cleared old data older than {days_to_keep} days")
    
    def cleanup_old_data_6_months(self):
        """Xóa dữ liệu cũ hơn 6 tháng khỏi tất cả các bảng"""        
        # Thực hiện DELETE operations trong transaction
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            deleted_counts = {}
            
            # Xóa dữ liệu cũ từ bảng fixed_rate_5m
            cursor.execute('''
                DELETE FROM fixed_rate_5m 
                WHERE datetime(timestamp) < datetime('now', '-180 days')
            ''')
            deleted_counts['fixed_rate_5m'] = cursor.rowcount
            
            # Xóa dữ liệu cũ từ bảng fixed_rate_15m
            cursor.execute('''
                DELETE FROM fixed_rate_15m 
                WHERE datetime(timestamp) < datetime('now', '-180 days')
            ''')
            deleted_counts['fixed_rate_15m'] = cursor.rowcount
            
            # Xóa dữ liệu cũ từ bảng fixed_rate_hourly
            cursor.execute('''
                DELETE FROM fixed_rate_hourly 
                WHERE datetime(timestamp) < datetime('now', '-180 days')
            ''')
            deleted_counts['fixed_rate_hourly'] = cursor.rowcount
            
            # Xóa dữ liệu cũ từ bảng station_status_history
            cursor.execute('''
                DELETE FROM station_status_history 
                WHERE datetime(scan_time) < datetime('now', '-180 days')
            ''')
            deleted_counts['station_status_history'] = cursor.rowcount
            
            # Xóa dữ liệu cũ từ bảng station_status_daily
            cursor.execute('''
                DELETE FROM station_status_daily 
                WHERE datetime(date) < datetime('now', '-180 days')
            ''')
            deleted_counts['station_status_daily'] = cursor.rowcount
            
            conn.commit()
        
        # Vacuum database để thu hồi không gian (phải thực hiện ngoài transaction)
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('VACUUM')
        
        total_deleted = sum(deleted_counts.values())
        logging.info(f"Đã xóa {total_deleted} bản ghi cũ hơn 6 tháng:")
        for table, count in deleted_counts.items():
            if count > 0:
                logging.info(f"  - {table}: {count} bản ghi")
        
        # Lấy thống kê sau khi dọn dẹp
        stats = self.get_database_stats()
        logging.info(f"Kích thước database sau khi dọn dẹp: {stats.get('file_size_mb', 0)} MB")
        
        return deleted_counts
    
    def export_to_json(self, table_name: str, output_file: str):
        """Export table data to JSON file for backup"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(f"SELECT * FROM {table_name}")
            
            columns = [description[0] for description in cursor.description]
            data = [dict(zip(columns, row)) for row in cursor.fetchall()]
            
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            
            logging.info(f"Exported {len(data)} records from {table_name} to {output_file}")
    
    def get_database_stats(self) -> Dict:
        """Get database statistics"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            stats = {}
            tables = ['fixed_rate_5m', 'fixed_rate_15m', 'fixed_rate_hourly', 
                     'station_status_temp', 'station_status_history', 'station_status_daily']
            
            for table in tables:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                stats[table] = cursor.fetchone()[0]
            
            # Database file size
            if os.path.exists(self.db_path):
                stats['file_size_mb'] = round(os.path.getsize(self.db_path) / (1024 * 1024), 2)
            
            return stats

# Create global database instance
db = MonitoringDatabase()
