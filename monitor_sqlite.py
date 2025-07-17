#!/usr/bin/env python3
"""
Enhanced station monitoring system with SQLite database storage.
Replaces Google Sheets to avoid 5xx errors and network dependencies.
"""
import math
import hmac
import hashlib
import json
import time
import threading
import logging
import requests
import schedule
import os
import re
import time as time_module
from datetime import datetime, timedelta, timezone, time
from flask import Flask, request, jsonify
from uuid import uuid4
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Import local database module
from database import db

# Global lock for preventing concurrent execution of report_station_status
status_report_lock = threading.Lock()

# ------------------ CONFIG & CONSTANTS ------------------
CONFIG = {
    "DISCORD": {
        "WEBHOOK_URL": os.getenv("DISCORD_WEBHOOK_URL")
    },
    "API": {
        "ACCESS_KEY": os.getenv("API_ACCESS_KEY"),
        "SECRET_KEY": os.getenv("API_SECRET_KEY"),
        "SIGN_METHOD": os.getenv("API_SIGN_METHOD", "HmacSHA256"),
        "BASE_URL": os.getenv("API_BASE_URL", "http://rtk.taikhoandodac.vn:8090"),
        "ENDPOINTS": {
            "ONLINE_USERS": {
                "URI": "/openapi/broadcast/online-users",
                "URL": f"{os.getenv('API_BASE_URL', 'http://rtk.taikhoandodac.vn:8090')}/openapi/broadcast/online-users?page=1&size=50",
                "METHOD": "GET"
            },
            "STATION_LIST": {
                "URI": "/openapi/stream/stations",
                "URL": f"{os.getenv('API_BASE_URL', 'http://rtk.taikhoandodac.vn:8090')}/openapi/stream/stations?page=1&size=50&count=true",
                "METHOD": "GET"
            },
            "DYNAMIC_INFO": {
                "URI": "/openapi/stream/stations/dynamic-info",
                "URL": f"{os.getenv('API_BASE_URL', 'http://rtk.taikhoandodac.vn:8090')}/openapi/stream/stations/dynamic-info",
                "METHOD": "POST"
            }
        }
    },
    "WHITELIST": ['PYN1', 'PYN3', 'PYN4', 'PYN5', 'HNI1'],
}

# Validate required environment variables
required_env_vars = ['DISCORD_WEBHOOK_URL', 'API_ACCESS_KEY', 'API_SECRET_KEY']
missing_vars = [var for var in required_env_vars if not os.getenv(var)]
if missing_vars:
    raise ValueError(f"Các biến môi trường bị thiếu trong file .env: {', '.join(missing_vars)}")

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# ------------------ UTILITY FUNCTIONS ------------------

def calc_sign(secret_key, method, uri, x_headers):
    """Calculate the HMAC-SHA256 signature for API authentication."""
    sorted_keys = sorted(x_headers.keys())
    header_strings = "&".join(f"{key.lower()}={x_headers[key]}" for key in sorted_keys)
    builder = f"{method} {uri} {header_strings}"
    signature = hmac.new(secret_key.encode(), builder.encode(), hashlib.sha256).hexdigest()
    return signature

def make_api_call(endpoint, body=None):
    """Make an API call with signed headers."""
    method = endpoint["METHOD"]
    uri = endpoint["URI"]
    url = endpoint["URL"]

    x_headers = {
        "X-Nonce": uuid4().hex,
        "X-Access-Key": CONFIG["API"]["ACCESS_KEY"],
        "X-Sign-Method": CONFIG["API"]["SIGN_METHOD"],
        "X-Timestamp": str(int(time_module.time() * 1000))
    }
    signature = calc_sign(CONFIG["API"]["SECRET_KEY"], method, uri, x_headers)
    headers = {
        **x_headers,
        "Sign": signature,
        "Connection": "keep-alive",
        "Content-Type": "application/json",
        "Accept-Language": "en-US"
    }

    try:
        if method.upper() == "GET":
            response = requests.get(url, headers=headers, timeout=10)
        elif method.upper() == "POST":
            response = requests.post(url, headers=headers, json=body, timeout=10)
        else:
            raise ValueError("Unsupported HTTP method")
        
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logging.error(f"Error in API call to {uri}: {e}")
        return None

def send_discord_message(chat_id, message):
    """Send a Discord message via webhook (ignores chat_id)."""
    url = CONFIG['DISCORD']['WEBHOOK_URL']
    payload = {"content": message}
    try:
        response = requests.post(url, json=payload, timeout=10)
        if response.status_code == 204 or response.status_code == 200:
            logging.info("Discord message sent successfully")
        else:
            logging.error(f"Error sending Discord message: {response.text}")
    except Exception as e:
        logging.error(f"Exception when sending Discord message: {str(e)}")

send_telegram_message = send_discord_message

# ------------------ API DATA FUNCTIONS ------------------
def get_online_users(max_retries=3):
    """Get online users data with retries."""
    attempts = 0
    endpoint = CONFIG["API"]["ENDPOINTS"]["ONLINE_USERS"]
    while attempts < max_retries:
        result = make_api_call(endpoint)
        if result and result.get("code") == "SUCCESS" and result.get("data"):
            return result["data"]
        attempts += 1
        time_module.sleep(2 ** attempts)
    logging.error(f"Failed to get online users data after {attempts} attempts.")
    return None

def get_all_stations():
    """Retrieve station list from API."""
    endpoint = CONFIG["API"]["ENDPOINTS"]["STATION_LIST"]
    response = make_api_call(endpoint)
    mapping = {}
    if response and response.get("code") == "SUCCESS":
        for rec in response["data"].get("records", []):
            mapping[rec["id"]] = {
                "stationName": rec["stationName"],
                "identificationName": rec["identificationName"]
            }
    return mapping

def get_dynamic_info():
    """Retrieve dynamic station info."""
    station_mapping = get_all_stations()
    station_ids = list(station_mapping.keys())
    if not station_ids:
        logging.error("No station IDs available")
        return None
        
    endpoint = CONFIG["API"]["ENDPOINTS"]["DYNAMIC_INFO"]
    response = make_api_call(endpoint, body={"ids": station_ids})
    if response and response.get("code") == "SUCCESS":
        station_data = response["data"]
        for station in station_data:
            mapping = station_mapping.get(station["stationId"])
            station["identificationName"] = mapping["identificationName"] if mapping else ""
        return station_data
    return None

# ------------------ FIXED RATE FUNCTIONS ------------------
def process_fixed_rate_scan():
    """Scan fixed rates every 5 minutes and save to SQLite."""
    data = get_online_users()
    if not data:
        logging.error("Unable to fetch online user data for fixed rate scan")
        return
    
    scan_time = datetime.now()
    
    # Group records by masterStationName
    station_groups = {}
    for record in data.get("records", []):
        station = record.get("masterStationName", "unknown")
        station_groups.setdefault(station, []).append(record)
    
    # Compute and save fixed rate for each station
    records_to_save = []
    for station, records in station_groups.items():
        total_users = len(records)
        fixed_count = sum(1 for r in records if r.get("status") == 4)
        fixed_rate = (fixed_count / total_users * 100) if total_users > 0 else 0.0
        
        records_to_save.append({
            "timestamp": scan_time.strftime("%Y-%m-%d %H:%M:%S"),
            "station": station,
            "fixed_rate": format(fixed_rate, ".3f").replace(".", ","),
            "users": format(total_users, ".3f").replace(".", ","),
            "fixed_users": format(fixed_count, ".3f").replace(".", ",")
        })
    
    if records_to_save:
        try:
            db.save_fixed_rate_data(records_to_save, "5m")
            logging.info(f"Saved {len(records_to_save)} 5-minute fixed rate records to SQLite")
        except Exception as e:
            logging.error(f"Error saving fixed rate data to SQLite: {e}")

def aggregate_fixed_rate_15m():
    """Aggregate 15-minute data from 5m table."""
    try:
        # Get data from last 15 minutes
        data = db.get_fixed_rate_data("5m", hours_back=0.25)  # 15 minutes
        
        if not data:
            logging.info("No data available for 15-minute aggregation")
            return

        now = datetime.now()
        total_users = 0.0
        total_fixed_users = 0.0
        count = 0
        active_stations_by_timestamp = {}
        distinct_timestamps = set()
        
        for row in data:
            try:
                # Convert values back to float
                users = float(str(row.get("Users", "0")).replace(",", "."))
                fixed_users = float(str(row.get("Fixed Users", "0")).replace(",", "."))
                total_users += users
                total_fixed_users += fixed_users
                count += 1
                
                ts_str = row["Timestamp"]
                distinct_timestamps.add(ts_str)
                
                if ts_str not in active_stations_by_timestamp:
                    active_stations_by_timestamp[ts_str] = set()
                
                station = row.get("Station", "")
                if users > 0:
                    active_stations_by_timestamp[ts_str].add(station)
                    
            except Exception as e:
                logging.error(f"Error processing 15m aggregation row: {e}")
        
        if count == 0:
            return

        # Calculate averages
        total_active_stations = sum(len(stations) for stations in active_stations_by_timestamp.values())
        num_unique_timestamps = len(distinct_timestamps) if distinct_timestamps else 1
        avg_active_stations = total_active_stations / num_unique_timestamps

        fixed_rate = (total_fixed_users / total_users * 100) if total_users > 0 else 0
        record = {
            "timestamp": now.strftime("%Y-%m-%d %H:%M:%S"),
            "station": format(avg_active_stations-1, ".3f").replace(".", ","),
            "fixed_rate": format(fixed_rate, ".3f").replace(".", ","),
            "users": format(total_users/num_unique_timestamps, ".3f").replace(".", ","),
            "fixed_users": format(total_fixed_users/num_unique_timestamps, ".3f").replace(".", ",")
        }

        # Clear 5m data and save 15m aggregated data
        db.save_fixed_rate_data([record], "15m")
        # Clear old 5m data to keep database clean
        db.clear_old_data(days_to_keep=3)
        logging.info("Aggregated 15-minute fixed rate data saved to SQLite.")
        
    except Exception as e:
        logging.error(f"Error in 15-minute aggregation: {e}")

def aggregate_fixed_rate_hourly():
    """Aggregate hourly data from 15m table."""
    try:
        data = db.get_fixed_rate_data("15m", hours_back=1)
        
        if not data:
            logging.info("No data available for hourly aggregation")
            return

        now = datetime.now()
        total_users = 0.0
        total_fixed_users = 0.0
        count = 0

        for row in data:
            try:
                users = float(str(row.get("Users", "0")).replace(",", "."))
                fixed_users = float(str(row.get("Fixed Users", "0")).replace(",", "."))
                total_users += users
                total_fixed_users += fixed_users
                count += 1
            except Exception as e:
                logging.error(f"Error processing hourly aggregation row: {e}")

        if count == 0:
            return
        
        fixed_rate = (total_fixed_users / total_users * 100) if total_users > 0 else 0

        record = {
            "timestamp": now.strftime("%Y-%m-%d %H:%M:%S"),
            "station": "ALL",
            "fixed_rate": fixed_rate,
            "users": total_users/count,
            "fixed_users": total_fixed_users/count
        }
        
        db.save_fixed_rate_data([record], "hourly")
        logging.info("Aggregated hourly fixed rate data saved to SQLite.")
        
    except Exception as e:
        logging.error(f"Error in hourly aggregation: {e}")

# ------------------ STATION STATUS FUNCTIONS ------------------
def save_temp_scan_data(station_data, previous_data):
    """Save current station status data and update error tracking."""
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Prepare data for saving
    temp_data = []
    history_data = []
    
    for station in station_data:
        station_id = station.get("stationId")
        station_name = station.get("stationName")
        identification_name = station.get("identificationName")
        connect_status = station.get("connectStatus")
        
        # Track error start time
        prev = previous_data.get(str(station_id), {})
        if connect_status in [2, 3]:  # Error statuses
            error_start = prev.get("errorStartTime") or current_time
        else:
            error_start = ""
        
        record = {
            "stationId": station_id,
            "stationName": station_name,
            "identificationName": identification_name,
            "connectStatus": connect_status,
            "scanTime": current_time,
            "errorStartTime": error_start
        }
        
        temp_data.append(record)
        history_data.append(record)
    
    try:
        # Save to SQLite
        db.save_station_status_temp(temp_data)
        db.save_station_status_history(history_data)
        logging.info(f"Updated station status data in SQLite")
    except Exception as e:
        logging.error(f"Error saving station status to SQLite: {e}")

def report_station_status(chat_id=None, force_send=False, province_prefix: str = None):
    """Generate and send station status report, optionally filtered by province."""
    with status_report_lock:
        logging.info(f"report_station_status started - PID: {os.getpid()}, Thread: {threading.current_thread().name}")
        
        current_data_full = get_dynamic_info()
        if not current_data_full:
            logging.error("Failed to retrieve current station data")
            return

        try:
            previous_data_full = db.get_previous_station_status()
        except Exception as e:
            logging.error(f"Error getting previous station status: {e}")
            previous_data_full = {}

        # Filter data if province_prefix is provided
        report_title_suffix = ""
        if province_prefix:
            report_title_suffix = f" cho tỉnh {province_prefix.upper()}"
            current_data = [
                st for st in current_data_full
                if isinstance(st, dict) and st.get("stationName", "").upper().startswith(province_prefix.upper())
            ]
            previous_data = {
                k: v for k, v in previous_data_full.items()
                if v.get("stationName", "").upper().startswith(province_prefix.upper())
            }
            if not current_data:
                 logging.warning(f"No stations found matching prefix '{province_prefix}' for status report.")
                 save_temp_scan_data(current_data_full, previous_data_full)
                 return
        else:
            current_data = current_data_full
            previous_data = previous_data_full

        new_problems = []
        fixed_stations = []
        still_problems = []

        dt_now = datetime.now()
        yesterday = dt_now - timedelta(days=1)

        for station in current_data:
            if not isinstance(station, dict):
                continue

            station_id = station.get("stationId")
            current_status = station.get("connectStatus")
            station_name = station.get("stationName")

            # Whitelist check
            if station_name in CONFIG["WHITELIST"]:
                continue

            prev = previous_data.get(str(station_id))

            if prev is None:
                if current_status in [2, 3]:
                    new_problems.append(f'{station_name} ({station.get("identificationName")})')
                continue

            prev_status = prev.get("connectStatus")

            if current_status in [2, 3]:
                if prev_status == 1:
                     if datetime.now().time() > time(20, 15):
                         continue
                     new_problems.append(f'{station_name} ({station.get("identificationName")})')
                else:
                    error_start = prev.get("errorStartTime")
                    dt_error = None
                    if error_start:
                        try:
                            dt_error = datetime.strptime(error_start, "%Y-%m-%d %H:%M:%S")
                        except Exception as e:
                            dt_error = None
                    
                    if dt_error and dt_error.date() == yesterday.date() and dt_error.time() <= time(21, 00):
                        continue
                    if dt_error:
                        try:
                            downtime = dt_now - dt_error
                            downtime_str = str(downtime).split(".")[0]
                        except Exception as e:
                            downtime_str = "N/A"
                    else:
                        downtime_str = "N/A"
                    still_problems.append(f'{station_name} ({station.get("identificationName")}) - TG bị lỗi: {downtime_str}')

            elif current_status == 1 and prev_status in [2, 3]:
                error_start = prev.get("errorStartTime")
                dt_error = None
                if error_start:
                    try:
                        dt_error = datetime.strptime(error_start, "%Y-%m-%d %H:%M:%S")
                    except Exception as e:
                        dt_error = None

                if dt_error and dt_error.date() == yesterday.date() and dt_error.time() <= time(21, 00):
                    continue

                if dt_error:
                    try:
                        downtime = dt_now - dt_error
                        downtime_str = str(downtime).split(".")[0]
                    except Exception as e:
                        downtime_str = "N/A"
                else:
                    downtime_str = "N/A"

                fixed_stations.append(f'{station_name} ({station.get("identificationName")}) - TG gián đoạn: {downtime_str}')

        # Calculate counts
        total_stations = len(current_data)
        online_count = sum(1 for st in current_data if isinstance(st, dict) and st.get("connectStatus") == 1)
        no_data_count = sum(1 for st in current_data if isinstance(st, dict) and st.get("connectStatus") == 2)
        offline_count = sum(1 for st in current_data if isinstance(st, dict) and st.get("connectStatus") == 3)

        # Format message
        current_time = dt_now.strftime("%d/%m/%Y %H:%M:%S")
        new_problems_formatted = ('\n    - '.join(new_problems)) if new_problems else 'Không có'
        fixed_stations_formatted = ('\n    - '.join(fixed_stations)) if fixed_stations else 'Không có'
        still_problems_formatted = ('\n    - '.join(still_problems)) if still_problems else 'Không có'
        
        whitelist_display = "Không"
        if "WHITELIST" in CONFIG and isinstance(CONFIG["WHITELIST"], list) and CONFIG["WHITELIST"]:
             whitelist_display = ', '.join(CONFIG['WHITELIST'])

        message = (
            f"Báo cáo trạng thái trạm{report_title_suffix} lúc {current_time}\n\n"
            f"1. Trạm mới phát sinh lỗi:\n   - {new_problems_formatted}\n\n"
            f"2. Trạm mới được khắc phục:\n   - {fixed_stations_formatted}\n\n"
            f"3. Các trạm còn bị lỗi (bỏ qua: {whitelist_display}):\n   - {still_problems_formatted}\n\n"
            f"4. Tổng quan{' tỉnh' if province_prefix else ' hệ thống'}:\n"
            f"   - Tổng số trạm{' trong tỉnh' if province_prefix else ' hiện tại'}: {total_stations}\n"
            f"   - Đang hoạt động: {online_count}\n"
            f"   - Không đẩy dữ liệu: {no_data_count}\n"
            f"   - Bị tắt: {offline_count}"
        )

        # Send message if changes detected or forced
        should_send = force_send or new_problems or fixed_stations or (province_prefix and still_problems)
        if should_send:
            send_telegram_message(chat_id, message)
        else:
            logging.info(f"No significant changes in station status{report_title_suffix}, report not sent")

        # Save the full snapshot
        save_temp_scan_data(current_data_full, previous_data_full)
        
        logging.info(f"report_station_status completed - Thread: {threading.current_thread().name}")

# ------------------ REPORT FUNCTIONS ------------------
def report_station_fixed_rate(chat_id, station_name, duration_minutes: int = 10):
    """Report fixed rate for a specific station using SQLite data."""
    try:
        all_records = db.get_fixed_rate_data("5m", hours_back=duration_minutes/60)
        
        # Filter for specific station
        station_records = [
            rec for rec in all_records
            if rec.get("Station") and str(rec.get("Station")).strip().upper() == station_name.strip().upper()
        ]

        if not station_records:
            message = f"Không tìm thấy dữ liệu nào cho trạm '{station_name}' trong {duration_minutes} phút qua."
            send_telegram_message(chat_id, message)
            return

        # Calculate averages
        total_users_in_window = 0.0
        total_fixed_users_in_window = 0.0
        
        for rec in station_records:
            try:
                users_str = str(rec.get("Users", "0")).replace(",", ".")
                fixed_users_str = str(rec.get("Fixed Users", "0")).replace(",", ".")
                total_users_in_window += float(users_str)
                total_fixed_users_in_window += float(fixed_users_str)
            except ValueError:
                continue

        avg_fixed_rate = (total_fixed_users_in_window / total_users_in_window * 100) if total_users_in_window > 0 else 0.0
        num_records = len(station_records)
        avg_users = total_users_in_window / num_records if num_records > 0 else 0.0
        avg_fixed_users = total_fixed_users_in_window / num_records if num_records > 0 else 0.0

        message = (
            f"Báo cáo Fixed Rate trung bình cho trạm: *{station_name}*\n"
            f"- Tỷ lệ Fixed (TB): `{avg_fixed_rate:.2f}%`\n"
            f"- Tổng Users (TB): `{avg_users:.1f}`\n"
            f"- Fixed Users (TB): `{avg_fixed_users:.1f}`\n"
        )

        send_telegram_message(chat_id, message)
        
    except Exception as e:
        logging.error(f"Error in report_station_fixed_rate: {e}")
        send_telegram_message(chat_id, f"Lỗi khi tạo báo cáo cho trạm '{station_name}'.")

def report_province_fixed_rate(chat_id, province_prefix: str, duration_minutes: int = 10):
    """Report average fixed rate for a province using SQLite data."""
    try:
        all_records = db.get_fixed_rate_data("5m", hours_back=duration_minutes/60)
        
        # Filter for province
        province_records = [
            rec for rec in all_records
            if rec.get("Station") and str(rec.get("Station")).strip().upper().startswith(province_prefix.strip().upper())
        ]

        if not province_records:
            message = f"Không tìm thấy trạm nào bắt đầu bằng '{province_prefix}' trong {duration_minutes} phút qua."
            send_telegram_message(chat_id, message)
            return

        # Calculate averages
        total_users_in_window = 0.0
        total_fixed_users_in_window = 0.0
        station_count = set()
        
        for rec in province_records:
            try:
                station_count.add(rec.get("Station"))
                users_str = str(rec.get("Users", "0")).replace(",", ".")
                fixed_users_str = str(rec.get("Fixed Users", "0")).replace(",", ".")
                total_users_in_window += float(users_str)
                total_fixed_users_in_window += float(fixed_users_str)
            except ValueError:
                continue

        avg_fixed_rate = (total_fixed_users_in_window / total_users_in_window * 100) if total_users_in_window > 0 else 0.0
        num_records = len(province_records)
        avg_users = total_users_in_window / num_records if num_records > 0 else 0.0
        avg_fixed_users = total_fixed_users_in_window / num_records if num_records > 0 else 0.0

        message = (
            f"Báo cáo Fixed Rate trung bình cho tỉnh: *{province_prefix.upper()}*\n"
            f"- Tỷ lệ Fixed (TB): `{avg_fixed_rate:.2f}%`\n"
            f"- Tổng Users (TB/điểm đo): `{avg_users:.1f}`\n"
            f"- Fixed Users (TB/điểm đo): `{avg_fixed_users:.1f}`\n"
            f"- Số trạm có dữ liệu: `{len(station_count)}`\n"
        )

        send_telegram_message(chat_id, message)
        
    except Exception as e:
        logging.error(f"Error in report_province_fixed_rate: {e}")
        send_telegram_message(chat_id, f"Lỗi khi tạo báo cáo cho tỉnh '{province_prefix}'.")

def report_fixed_rate():
    """Create fixed rate report from SQLite data."""
    try:
        records = db.get_fixed_rate_data("15m", hours_back=1)
        
        if not records:
            message = "Không có dữ liệu để tạo báo cáo fixed rate."
            send_telegram_message(None, message)
            return False

        # Use latest record
        latest = records[-1]
        
        fixed_rate = float(str(latest.get("Fixed Rate (%)", "0")).replace(",", "."))
        users = float(str(latest.get("Users", "0")).replace(",", "."))
        fixed_users = float(str(latest.get("Fixed Users", "0")).replace(",", "."))
        stations = float(str(latest.get("Station", "0")).replace(",", "."))
        
        now = datetime.now()
        message = f"""Báo cáo chất lượng lúc {now.strftime('%d/%m/%Y %H:%M:%S')}:
+ Tỉ lệ Fixed: {fixed_rate:.2f}%
+ Số người dùng trung bình: {users:.1f}
+ Số người dùng fixed trung bình: {fixed_users:.1f}
+ Số trạm có người dùng: {stations:.1f}
        """
        send_telegram_message(None, message)
        logging.info("Fixed rate report sent successfully.")
        return True
        
    except Exception as e:
        logging.error(f"Error creating fixed rate report: {e}")
        return False

def generate_hourly_report():
    """Generate hourly report from SQLite data."""
    try:
        all_data = db.get_fixed_rate_data("hourly", hours_back=24)
        
        today = datetime.now().strftime("%Y-%m-%d")
        today_data = [row for row in all_data if row.get("Timestamp", "").startswith(today)]
        
        if not today_data:
            return "Không có dữ liệu giờ cho ngày hôm nay."
        
        report_lines = [f"Báo cáo fixed rate theo giờ ngày {today}:"]
        report_lines.append("\n```")
        header = f"{'Giờ':^5} | {'Users':^5} | {'Fixed':^5} | {'Tỷ lệ':^6}"
        divider = "-" * len(header)
        report_lines.append(header)
        report_lines.append(divider)
        
        for row in sorted(today_data, key=lambda x: x.get("Timestamp")):
            dt = datetime.strptime(row.get("Timestamp"), "%Y-%m-%d %H:%M:%S")
            hour_str = dt.strftime("%H:00")
            
            try:
                # Use the correct column names as returned by database.py
                users_val = float(str(row.get("Users", "0")).replace(",", "."))
                fixed_users_val = float(str(row.get("Fixed Users", "0")).replace(",", "."))
                
                formatted_users = format(users_val, ".1f")
                formatted_fixed_users = format(fixed_users_val, ".1f")
                
                # Use the stored fixed_rate directly (it's already a percentage)
                fixed_rate_val = float(str(row.get("Fixed Rate (%)", "0")).replace(",", "."))
                
                line = f"{hour_str:^5} | {formatted_users:^5} | {formatted_fixed_users:^5} | {fixed_rate_val:^6.2f}"
                report_lines.append(line)
            except (ValueError, TypeError) as e:
                logging.error(f"Error converting values: {e}")
                continue
        
        report_lines.append("```")
        return "\n".join(report_lines)
        
    except Exception as e:
        logging.error(f"Error generating hourly report: {e}")
        return f"Lỗi khi tạo báo cáo giờ: {str(e)}"

def add_whitelist(stations_str):
    """Add stations to whitelist."""
    stations = [s.strip().upper() for s in stations_str.split(",") if s.strip()]
    added = []
    
    for station in stations:
        if station not in CONFIG["WHITELIST"]:
            CONFIG["WHITELIST"].append(station)
            added.append(station)
    
    if added:
        message = f"Đã thêm vào whitelist: {', '.join(added)}"
    else:
        message = "Các trạm đã có trong whitelist."
    
    send_telegram_message(None, message)
    logging.info(f"Whitelist updated: {CONFIG['WHITELIST']}")

# ------------------ SCHEDULER JOBS ------------------
def daytime_only(job_func):
    """Only run function during daytime hours (6:00 - 20:00)."""
    def wrapper():
        now = datetime.now()
        if 6 <= now.hour < 20:
            job_func()
        else:
            logging.info(f"Job {job_func.__name__} skipped (outside daytime hours)")
    return wrapper

def job_fixed_rate_scan():
    process_fixed_rate_scan()

def job_aggregate_15m():
    aggregate_fixed_rate_15m()

def job_aggregate_hourly():
    aggregate_fixed_rate_hourly()

def job_status_report():
    report_station_status(force_send=True)

def job_fixed_rate_report():
    report_fixed_rate()

def job_daily_bccl_report():
    """Send daily hourly fixed rate report at 21:00."""
    report = generate_hourly_report()
    send_telegram_message(None, report)

def job_db_cleanup():
    """Daily database cleanup."""
    try:
        # Xóa dữ liệu cũ hơn 6 tháng
        deleted_counts = db.cleanup_old_data_6_months()
        
        # Lấy thống kê database sau khi dọn dẹp
        stats = db.get_database_stats()
        logging.info(f"Database stats after cleanup: {stats}")
        
        # Log chi tiết về dữ liệu đã xóa
        total_deleted = sum(deleted_counts.values())
        if total_deleted > 0:
            logging.info(f"Đã xóa {total_deleted} bản ghi cũ hơn 6 tháng:")
            for table, count in deleted_counts.items():
                if count > 0:
                    logging.info(f"  - {table}: {count} bản ghi")
        else:
            logging.info("Không có dữ liệu cũ hơn 6 tháng để xóa")
        
    except Exception as e:
        logging.error(f"Error in database cleanup: {e}")

def run_schedule():
    schedule.every(139).seconds.do(daytime_only(job_fixed_rate_scan))    
    
    # 15-minute aggregation
    aggregate_minutes = ["01", "16", "31", "46"]
    for m in aggregate_minutes:
        schedule.every().hour.at(":" + m).do(daytime_only(job_aggregate_15m))
    
    # Status reports
    schedule.every(5).minutes.do(daytime_only(report_station_status))
    
    # Hourly tasks
    schedule.every().hour.at(":02").do(daytime_only(job_aggregate_hourly))
    
    # Scheduled reports
    schedule.every().day.at("06:30").do(job_status_report)
    schedule.every().day.at("20:30").do(job_status_report)
    schedule.every().day.at("10:31").do(job_fixed_rate_report)
    schedule.every().day.at("16:01").do(job_fixed_rate_report)
    schedule.every().day.at("20:30").do(job_daily_bccl_report)
    
    # Daily cleanup
    schedule.every().day.at("02:00").do(job_db_cleanup)
    
    while True:
        schedule.run_pending()
        time_module.sleep(1)

def start_scheduler():
    scheduler_thread = threading.Thread(target=run_schedule, daemon=True)
    scheduler_thread.start()

# ------------------ FLASK WEBHOOK ENDPOINT ------------------
app = Flask(__name__) 

@app.route('/webhook', methods=['POST']) 
def webhook():
    """Handle Telegram webhook commands."""
    try:
        update = request.get_json()
        if not update or "message" not in update:
            logging.warning("Received invalid update format")
            return jsonify({"status": "ignored"}), 200
            
        message = update["message"]
        user = message.get("from", {})
        chat = message.get("chat", {})
        text = message.get("text", "").strip()
        chat_id = chat.get("id")
        
        if not chat_id or user.get("is_bot") or not text:
            return jsonify({"status": "ignored"}), 200

        command_base = text.split('@')[0].strip()
        if not command_base:
             return jsonify({"status": "ignored_empty_command"}), 200

        parts = command_base.split(" ", 2)
        main_command = parts[0].lower()

        # Command handling (same as before but using SQLite)
        if main_command in ["/st", "/rp"]:
            province_arg = None
            ack_msg = "Đang xử lý báo cáo trạng thái toàn hệ thống..."
            if len(parts) > 1 and parts[1].strip():
                province_arg = parts[1].strip().upper()
                ack_msg = f"Đang xử lý báo cáo trạng thái cho tỉnh {province_arg}..."
            
            threading.Thread(target=report_station_status, args=(chat_id, True, province_arg)).start()
            send_telegram_message(chat_id, ack_msg)
            
        elif main_command == "/fr":
            if len(parts) == 1:
                def handle_global_fixedrate():
                    report_fixed_rate()
                threading.Thread(target=handle_global_fixedrate).start()
                send_telegram_message(chat_id, "Đang xử lý báo cáo fixed rate toàn hệ thống...")
            elif len(parts) >= 2:
                arg1 = parts[1].strip()
                if not arg1:
                    send_telegram_message(chat_id, "Vui lòng cung cấp tên tỉnh/trạm. Ví dụ: /fr YBI hoặc /fr TNN5 [phút]")
                    return jsonify({"status": "OK"}), 200
                    
                province_or_station = arg1.upper()
                duration_minutes = 10
                province_default_duration = 15
                
                parsed_duration = None
                if len(parts) == 3:
                    duration_str = parts[2].strip()
                    try:
                        parsed_duration = int(duration_str)
                        if parsed_duration <= 0:
                            send_telegram_message(chat_id, f"Số phút '{duration_str}' không hợp lệ.")
                            return jsonify({"status": "OK"}), 200
                    except ValueError:
                        send_telegram_message(chat_id, f"Tham số thời gian '{duration_str}' không hợp lệ.")
                        return jsonify({"status": "OK"}), 200
                
                is_province = len(province_or_station) <= 3 or province_or_station.isalpha()
                
                if is_province:
                    final_duration = parsed_duration if parsed_duration is not None else province_default_duration
                    threading.Thread(target=report_province_fixed_rate, args=(chat_id, province_or_station, final_duration)).start()
                    send_telegram_message(chat_id, f"Đang xử lý báo cáo fixed rate cho tỉnh {province_or_station}...")
                else:
                    final_duration = parsed_duration if parsed_duration is not None else duration_minutes
                    threading.Thread(target=report_station_fixed_rate, args=(chat_id, province_or_station, final_duration)).start()
                    send_telegram_message(chat_id, f"Đang xử lý báo cáo fixed rate cho trạm {province_or_station}...")
                    
        elif main_command == "/bccl":
            def handle_bccl():
                report = generate_hourly_report()
                send_telegram_message(chat_id, report)
            threading.Thread(target=handle_bccl).start()
            send_telegram_message(chat_id, "Đang xử lý báo cáo BCCL...")
            
        elif main_command == "/addwhitelist":
             if len(parts) == 2:
                 station_list_str = parts[1]
                 add_whitelist(station_list_str)
             else:
                 send_telegram_message(chat_id, "Vui lòng cung cấp danh sách trạm. Cú pháp: /addwhitelist TNN1,YBI2")
                 
        elif main_command == "/viewwhitelist":
             if CONFIG["WHITELIST"]:
                 whitelist_str = ", ".join(sorted(CONFIG["WHITELIST"]))
                 msg = f"Các trạm trong whitelist:\n`{whitelist_str}`"
             else:
                 msg = "Danh sách trắng hiện đang trống."
             send_telegram_message(chat_id, msg)
             
        elif main_command == "/dbstats":
             def handle_stats():
                 stats = db.get_database_stats()
                 msg = f"Thống kê database:\n```\n{json.dumps(stats, indent=2)}\n```"
                 send_telegram_message(chat_id, msg)
             threading.Thread(target=handle_stats).start()
             
        else:
            logging.info(f"Received unrecognized command: {main_command}")
            return jsonify({"status": "unrecognized_command"}), 200

        return jsonify({"status": "OK"}), 200
        
    except Exception as e:
        logging.exception(f"Error processing webhook request: {str(e)}")
        if 'chat_id' in locals() and chat_id:
             try:
                 send_telegram_message(chat_id, "Rất tiếc, đã xảy ra lỗi khi xử lý lệnh của bạn.")
             except Exception as send_error:
                 logging.error(f"Failed to send error notification: {send_error}")
        return jsonify({"error": "Internal Server Error"}), 500

if __name__ == "__main__":
    start_scheduler()
    logging.info("Scheduler started with SQLite database.")
    app.run(host='0.0.0.0', port=5000, debug=False)
