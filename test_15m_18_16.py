#!/usr/bin/env python3
"""
Test script để tính lại record 2025-05-30 18:16:00 
và đưa vào bảng fixed_rate_15m
"""

import sqlite3
import logging
from datetime import datetime, timedelta
from database import db

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def test_recalculate_15m_record():
    """
    Test tính lại record 15m cho thời điểm 2025-05-30 18:16:00
    Dựa trên logic của aggregate_fixed_rate_15m()
    """
    
    # Thời điểm cần tính lại
    target_timestamp = "2025-05-30 18:16:00"
    target_dt = datetime.strptime(target_timestamp, "%Y-%m-%d %H:%M:%S")
    
    print(f"\n=== Test tính lại record 15m cho {target_timestamp} ===\n")
    
    try:
        # Lấy dữ liệu 5m trong khoảng 15 phút trước timestamp này
        fifteen_min_ago = target_dt - timedelta(minutes=15)
        print(f"Lấy dữ liệu 5m từ {fifteen_min_ago} đến {target_dt}")
        
        # Truy vấn trực tiếp database để lấy dữ liệu 5m trong khoảng thời gian
        with sqlite3.connect(db.db_path) as conn:
            cursor = conn.cursor()
            query = '''
                SELECT timestamp, station, fixed_rate, users, fixed_users
                FROM fixed_rate_5m
                WHERE datetime(timestamp) >= ? AND datetime(timestamp) <= ?
                ORDER BY timestamp
            '''
            cursor.execute(query, (fifteen_min_ago.strftime("%Y-%m-%d %H:%M:%S"), 
                                 target_dt.strftime("%Y-%m-%d %H:%M:%S")))
            
            rows = cursor.fetchall()
            
        if not rows:
            print("❌ Không có dữ liệu 5m trong khoảng thời gian này")
            return False
            
        print(f"📊 Tìm thấy {len(rows)} records 5m:")
        
        total_users = 0.0
        total_fixed_users = 0.0
        count = 0
        active_stations_by_timestamp = {}
        distinct_timestamps = set()
        
        for row in rows:
            timestamp, station, fixed_rate, users, fixed_users = row
            print(f"  - {timestamp}: Station={station}, Users={users:.1f}, Fixed Users={fixed_users:.1f}, Rate={fixed_rate:.2f}%")
            
            total_users += users
            total_fixed_users += fixed_users
            count += 1
            
            # Lưu timestamp để đếm số lần đo (theo logic aggregate_fixed_rate_15m)
            ts_str = timestamp
            distinct_timestamps.add(ts_str)
            
            # Khởi tạo set cho timestamp này nếu chưa tồn tại
            if ts_str not in active_stations_by_timestamp:
                active_stations_by_timestamp[ts_str] = set()
            
            # Thêm station vào set của timestamp nếu users > 0
            if users > 0:
                active_stations_by_timestamp[ts_str].add(station)
        
        if count == 0:
            print("❌ Không có dữ liệu hợp lệ để tính toán")
            return False
            
        # Tính toán theo logic aggregate_fixed_rate_15m()
        # Tính số trạm trung bình có trong mỗi lần đo
        total_active_stations = sum(len(stations) for stations in active_stations_by_timestamp.values())
        num_unique_timestamps = len(distinct_timestamps) if distinct_timestamps else 1
        avg_active_stations = total_active_stations / num_unique_timestamps
        
        # Tính fixed rate, nếu tổng users > 0
        final_fixed_rate = (total_fixed_users / total_users * 100) if total_users > 0 else 0
        avg_users = total_users / num_unique_timestamps
        avg_fixed_users = total_fixed_users / num_unique_timestamps
        
        print(f"\n📈 Kết quả tính toán:")
        print(f"  - Tổng Users: {total_users:.1f}")
        print(f"  - Tổng Fixed Users: {total_fixed_users:.1f}")
        print(f"  - Số records: {count}")
        print(f"  - Số timestamps khác nhau: {num_unique_timestamps}")
        print(f"  - Avg active stations: {avg_active_stations:.1f}")
        print(f"  - Fixed Rate: {final_fixed_rate:.2f}%")
        print(f"  - Avg Users per timestamp: {avg_users:.1f}")
        print(f"  - Avg Fixed Users per timestamp: {avg_fixed_users:.1f}")
        
        # Tạo record theo format của aggregate_fixed_rate_15m()
        # Trong logic gốc, station field được sử dụng để lưu avg_active_stations-1
        fifteen_m_record = {
            "timestamp": target_timestamp,
            "station": format(avg_active_stations-1, ".3f").replace(".", ","),
            "fixed_rate": format(final_fixed_rate, ".3f").replace(".", ","),
            "users": format(avg_users, ".3f").replace(".", ","),
            "fixed_users": format(avg_fixed_users, ".3f").replace(".", ",")
        }
        
        print(f"\n💾 Record sẽ được lưu vào fixed_rate_15m:")
        print(f"  - Timestamp: {fifteen_m_record['timestamp']}")
        print(f"  - Station (avg_active_stations-1): {fifteen_m_record['station']}")
        print(f"  - Fixed Rate: {fifteen_m_record['fixed_rate']}")
        print(f"  - Users: {fifteen_m_record['users']}")
        print(f"  - Fixed Users: {fifteen_m_record['fixed_users']}")
        
        # Kiểm tra xem record này đã tồn tại chưa
        with sqlite3.connect(db.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT id, fixed_rate, users, fixed_users
                FROM fixed_rate_15m
                WHERE timestamp = ?
            ''', (target_timestamp,))
            
            existing = cursor.fetchone()
            
        if existing:
            print(f"\n⚠️  Record đã tồn tại trong fixed_rate_15m:")
            print(f"  - ID: {existing[0]}")
            print(f"  - Fixed Rate hiện tại: {existing[1]:.2f}%")
            print(f"  - Users hiện tại: {existing[2]:.1f}")
            print(f"  - Fixed Users hiện tại: {existing[3]:.1f}")
            
            # Hỏi có muốn update không
            response = input("\n🔄 Bạn có muốn cập nhật record này? (y/n): ").lower().strip()
            if response == 'y':
                # Convert comma format back to float for database storage
                db_fixed_rate = float(fifteen_m_record['fixed_rate'].replace(",", "."))
                db_users = float(fifteen_m_record['users'].replace(",", "."))
                db_fixed_users = float(fifteen_m_record['fixed_users'].replace(",", "."))
                db_station = fifteen_m_record['station']  # Keep as string for station field
                
                with sqlite3.connect(db.db_path) as conn:
                    cursor = conn.cursor()
                    cursor.execute('''
                        UPDATE fixed_rate_15m
                        SET station = ?, fixed_rate = ?, users = ?, fixed_users = ?
                        WHERE timestamp = ?
                    ''', (db_station, db_fixed_rate, db_users, db_fixed_users, target_timestamp))
                    conn.commit()
                    
                print("✅ Đã cập nhật record thành công!")
            else:
                print("❌ Không cập nhật record.")
        else:
            # Thêm record mới
            response = input("\n➕ Bạn có muốn thêm record mới này vào fixed_rate_15m? (y/n): ").lower().strip()
            if response == 'y':
                # Convert comma format back to float for database storage
                db_fixed_rate = float(fifteen_m_record['fixed_rate'].replace(",", "."))
                db_users = float(fifteen_m_record['users'].replace(",", "."))
                db_fixed_users = float(fifteen_m_record['fixed_users'].replace(",", "."))
                db_station = fifteen_m_record['station']  # Keep as string for station field
                
                with sqlite3.connect(db.db_path) as conn:
                    cursor = conn.cursor()
                    cursor.execute('''
                        INSERT INTO fixed_rate_15m (timestamp, station, fixed_rate, users, fixed_users)
                        VALUES (?, ?, ?, ?, ?)
                    ''', (fifteen_m_record['timestamp'], db_station, 
                         db_fixed_rate, db_users, db_fixed_users))
                    conn.commit()
                    
                print("✅ Đã thêm record mới thành công!")
            else:
                print("❌ Không thêm record mới.")
        
        return True
        
    except Exception as e:
        print(f"❌ Lỗi khi tính toán: {e}")
        import traceback
        traceback.print_exc()
        return False

def check_5m_data_around_time():
    """Kiểm tra dữ liệu 5m xung quanh thời điểm 18:16:00"""
    
    print(f"\n=== Kiểm tra dữ liệu 5m xung quanh 2025-05-30 18:16:00 ===\n")
    
    try:
        # Kiểm tra dữ liệu từ 18:00 đến 18:30
        start_time = "2025-05-30 18:00:00"
        end_time = "2025-05-30 18:30:00"
        
        with sqlite3.connect(db.db_path) as conn:
            cursor = conn.cursor()
            query = '''
                SELECT timestamp, station, fixed_rate, users, fixed_users
                FROM fixed_rate_5m
                WHERE datetime(timestamp) >= ? AND datetime(timestamp) <= ?
                ORDER BY timestamp
            '''
            cursor.execute(query, (start_time, end_time))
            
            rows = cursor.fetchall()
            
        if not rows:
            print("❌ Không có dữ liệu 5m trong khoảng thời gian này")
            return
            
        print(f"📊 Tìm thấy {len(rows)} records 5m từ {start_time} đến {end_time}:")
        for row in rows:
            timestamp, station, fixed_rate, users, fixed_users = row
            print(f"  - {timestamp}: Station={station}, Users={users:.1f}, Fixed Users={fixed_users:.1f}, Rate={fixed_rate:.2f}%")
            
    except Exception as e:
        print(f"❌ Lỗi khi kiểm tra dữ liệu: {e}")

def check_existing_15m_data():
    """Kiểm tra dữ liệu 15m đã có"""
    
    print(f"\n=== Kiểm tra dữ liệu 15m hiện có cho ngày 2025-05-30 ===\n")
    
    try:
        with sqlite3.connect(db.db_path) as conn:
            cursor = conn.cursor()
            query = '''
                SELECT timestamp, station, fixed_rate, users, fixed_users
                FROM fixed_rate_15m
                WHERE date(timestamp) = '2025-05-30'
                ORDER BY timestamp
            '''
            cursor.execute(query)
            
            rows = cursor.fetchall()
            
        if not rows:
            print("❌ Không có dữ liệu 15m cho ngày 2025-05-30")
            return
            
        print(f"📊 Tìm thấy {len(rows)} records 15m cho ngày 2025-05-30:")
        for row in rows:
            timestamp, station, fixed_rate, users, fixed_users = row
            print(f"  - {timestamp}: Station={station}, Users={users:.1f}, Fixed Users={fixed_users:.1f}, Rate={fixed_rate:.2f}%")
            
    except Exception as e:
        print(f"❌ Lỗi khi kiểm tra dữ liệu: {e}")

if __name__ == "__main__":
    print("🔍 Test tính lại record 15m cho timestamp: 2025-05-30 18:16:00")
    print("=" * 60)
    
    # Kiểm tra dữ liệu hiện có
    check_existing_15m_data()
    
    # Kiểm tra dữ liệu 5m xung quanh thời điểm target
    check_5m_data_around_time()
    
    # Thực hiện test tính lại
    test_recalculate_15m_record()
    
    print("\n" + "=" * 60)
    print("✅ Hoàn thành test!")
