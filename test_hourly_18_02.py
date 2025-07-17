#!/usr/bin/env python3
"""
Simple test để kiểm tra database và tính lại record
"""

import sqlite3
import sys
import os
from datetime import datetime, timedelta

# Add current directory to path
sys.path.append(os.getcwd())

try:
    from database import db
    print("✅ Imported database successfully")
except Exception as e:
    print(f"❌ Error importing database: {e}")
    sys.exit(1)

def simple_test():
    """Simple test function"""
    print("🔍 Starting simple test...")
    
    # Test database connection
    try:
        with sqlite3.connect(db.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = cursor.fetchall()
            
        print(f"📊 Found {len(tables)} tables in database:")
        for table in tables:
            print(f"  - {table[0]}")
            
    except Exception as e:
        print(f"❌ Database connection error: {e}")
        return False
    
    # Check 15m data around target time
    target_time = "2025-05-30 18:02:00"
    target_dt = datetime.strptime(target_time, "%Y-%m-%d %H:%M:%S")
    hour_ago = target_dt - timedelta(hours=1)
    
    try:
        with sqlite3.connect(db.db_path) as conn:
            cursor = conn.cursor()
            
            # Check 15m data
            cursor.execute('''
                SELECT COUNT(*) FROM fixed_rate_15m
                WHERE datetime(timestamp) >= ? AND datetime(timestamp) <= ?
            ''', (hour_ago.strftime("%Y-%m-%d %H:%M:%S"), 
                  target_dt.strftime("%Y-%m-%d %H:%M:%S")))
            
            count_15m = cursor.fetchone()[0]
            print(f"📈 Found {count_15m} records in fixed_rate_15m table for target period")
            
            # Check hourly data
            cursor.execute('''
                SELECT COUNT(*) FROM fixed_rate_hourly
                WHERE date(timestamp) = '2025-05-30'
            ''', )
            
            count_hourly = cursor.fetchone()[0]
            print(f"⏰ Found {count_hourly} records in fixed_rate_hourly table for 2025-05-30")
            
            # Show actual 15m data
            cursor.execute('''
                SELECT timestamp, station, fixed_rate, users, fixed_users
                FROM fixed_rate_15m
                WHERE datetime(timestamp) >= ? AND datetime(timestamp) <= ?
                ORDER BY timestamp
            ''', (hour_ago.strftime("%Y-%m-%d %H:%M:%S"), 
                  target_dt.strftime("%Y-%m-%d %H:%M:%S")))
            
            rows = cursor.fetchall()
            if rows:
                print(f"\n📊 15m data for target period:")
                for row in rows:
                    print(f"  {row[0]}: Station={row[1]}, Rate={row[2]:.2f}%, Users={row[3]:.1f}, Fixed={row[4]:.1f}")
            else:
                print("❌ No 15m data found for target period")
                
    except Exception as e:
        print(f"❌ Error checking data: {e}")
        return False
    
    return True

if __name__ == "__main__":
    print("=" * 60)
    print("🔍 Simple Database Test")
    print("=" * 60)
    
    result = simple_test()
    
    if result:
        print("\n✅ Test completed successfully!")
    else:
        print("\n❌ Test failed!")
