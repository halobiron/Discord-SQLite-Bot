# pip install discord.py schedule flask requests
import os
import asyncio
import logging
from datetime import datetime
import monitor_sqlite as monitor  # Use SQLite version instead of Google Sheets
from discord.ext import commands
import discord
from typing import Optional
from discord import Interaction, app_commands
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Load Discord bot token from environment
BOT_TOKEN = os.getenv("DISCORD_BOT_TOKEN")
if not BOT_TOKEN:
    raise ValueError("DISCORD_BOT_TOKEN không được tìm thấy trong file .env")

intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix='/', intents=intents)

logging.basicConfig(level=logging.INFO)

@bot.event
async def on_ready():
    logging.info(f'Đã đăng nhập với tên {bot.user} (ID: {bot.user.id})')
    
    # Sync slash commands to make sure they're properly registered
    try:
        synced = await bot.tree.sync()
        logging.info(f'Synced {len(synced)} command(s)')
    except Exception as e:
        logging.error(f'Failed to sync commands: {e}')
    
    # Start the scheduler in monitor module
    monitor.start_scheduler()

    logging.info('Ready and commands are available globally.')

# Remove the conflicting prefix command - only use slash commands

# Remove other conflicting prefix commands - only use slash commands



# Application (slash) commands
@bot.tree.command(name='rp', description='Báo cáo tình trạng trạm')
@discord.app_commands.describe(province='Tiền tố tỉnh')
async def rp(interaction: Interaction, province: Optional[str] = None):
    try:
        # Defer the response immediately to prevent timeout
        await interaction.response.defer()
        
        def _run():
            monitor.report_station_status(force_send=True, province_prefix=province)
        
        # Run the monitoring function
        await asyncio.get_event_loop().run_in_executor(None, _run)
        
        # Send follow-up message confirming completion
        await interaction.followup.send(f"✅ Hoàn thành báo cáo trạng thái trạm{' cho ' + province if province else ''}!")
        
    except discord.NotFound:
        logging.error("Discord interaction not found - possibly expired or already handled")
        # Can't send a response if interaction is invalid
    except Exception as e:
        logging.error(f"Error in rp command: {e}")
        try:
            # Try to send error message if interaction is still valid
            if not interaction.response.is_done():
                await interaction.response.send_message(f"❌ Lỗi khi tạo báo cáo: {str(e)}", ephemeral=True)
            else:
                await interaction.followup.send(f"❌ Lỗi khi tạo báo cáo: {str(e)}")
        except:
            logging.error("Could not send error message to Discord")

@bot.tree.command(name='fr', description='Báo cáo tỷ lệ cố định')
@discord.app_commands.describe(arg1='Tỉnh hoặc trạm')
async def fr(interaction: Interaction, arg1: Optional[str] = None):
    try:
        # Defer the response immediately to prevent timeout
        await interaction.response.defer()
        
        if not arg1:
            def _run_global():
                try:
                    # Get the data directly and format the message for Discord response
                    records = monitor.db.get_fixed_rate_data("15m", hours_back=2)  # Increase to 2 hours to ensure data
                    
                    if not records:
                        return "Không có dữ liệu để tạo báo cáo fixed rate."

                    # Use latest record
                    latest = records[-1]
                    
                    # Convert values, handling comma as decimal separator
                    fixed_rate = float(str(latest.get("Fixed Rate (%)", "0")).replace(",", "."))
                    users = float(str(latest.get("Users", "0")).replace(",", "."))
                    fixed_users = float(str(latest.get("Fixed Users", "0")).replace(",", "."))
                    stations = float(str(latest.get("Station", "0")).replace(",", "."))
                    
                    from datetime import datetime
                    now = datetime.now()
                    message = f"""**Báo cáo chất lượng lúc {now.strftime('%d/%m/%Y %H:%M:%S')}:**
📊 **Tỉ lệ Fixed:** {fixed_rate:.2f}%
👥 **Số người dùng trung bình:** {users:.1f}
✅ **Số người dùng fixed trung bình:** {fixed_users:.1f}
📡 **Số trạm có người dùng:** {stations:.1f}

*Dữ liệu từ: {latest.get("Timestamp", "N/A")}*"""
                    return message
                except Exception as e:
                    monitor.logging.error(f"Error creating fixed rate report: {e}")
                    import traceback
                    traceback.print_exc()
                    return f"❌ Lỗi khi tạo báo cáo: {str(e)}"
            
            result = await asyncio.get_event_loop().run_in_executor(None, _run_global)
            await interaction.followup.send(result)
        else:
            def _run():
                try:
                    is_province = len(arg1) <= 3 or arg1.isalpha()
                    if is_province:
                        # Province report
                        records = monitor.db.get_fixed_rate_data("5m", hours_back=15/60)
                        
                        # Filter for province
                        province_records = [
                            rec for rec in records
                            if rec.get("Station") and str(rec.get("Station")).strip().upper().startswith(arg1.strip().upper())
                        ]

                        if not province_records:
                            return f"Không tìm thấy trạm nào bắt đầu bằng '{arg1}' trong 15 phút qua."

                        # Calculate averages
                        total_users = 0.0
                        total_fixed_users = 0.0
                        station_count = set()
                        
                        for rec in province_records:
                            try:
                                station_count.add(rec.get("Station"))
                                users_str = str(rec.get("Users", "0")).replace(",", ".")
                                fixed_users_str = str(rec.get("Fixed Users", "0")).replace(",", ".")
                                total_users += float(users_str)
                                total_fixed_users += float(fixed_users_str)
                            except ValueError:
                                continue

                        avg_fixed_rate = (total_fixed_users / total_users * 100) if total_users > 0 else 0.0
                        num_records = len(province_records)
                        avg_users = total_users / num_records if num_records > 0 else 0.0
                        avg_fixed_users = total_fixed_users / num_records if num_records > 0 else 0.0

                        return f"""**Báo cáo Fixed Rate cho tỉnh: {arg1.upper()}**
📊 **Tỷ lệ Fixed (TB):** {avg_fixed_rate:.2f}%
👥 **Tổng Users (TB/điểm đo):** {avg_users:.1f}
✅ **Fixed Users (TB/điểm đo):** {avg_fixed_users:.1f}
📡 **Số trạm có dữ liệu:** {len(station_count)}"""
                    else:
                        # Station report
                        records = monitor.db.get_fixed_rate_data("5m", hours_back=10/60)
                        
                        # Filter for station
                        station_records = [
                            rec for rec in records
                            if rec.get("Station") and str(rec.get("Station")).strip().upper() == arg1.strip().upper()
                        ]

                        if not station_records:
                            return f"Không tìm thấy dữ liệu nào cho trạm '{arg1}' trong 10 phút qua."

                        # Calculate averages
                        total_users = 0.0
                        total_fixed_users = 0.0
                        
                        for rec in station_records:
                            try:
                                users_str = str(rec.get("Users", "0")).replace(",", ".")
                                fixed_users_str = str(rec.get("Fixed Users", "0")).replace(",", ".")
                                total_users += float(users_str)
                                total_fixed_users += float(fixed_users_str)
                            except ValueError:
                                continue

                        avg_fixed_rate = (total_fixed_users / total_users * 100) if total_users > 0 else 0.0
                        num_records = len(station_records)
                        avg_users = total_users / num_records if num_records > 0 else 0.0
                        avg_fixed_users = total_fixed_users / num_records if num_records > 0 else 0.0

                        return f"""**Báo cáo Fixed Rate cho trạm: {arg1.upper()}**
📊 **Tỷ lệ Fixed (TB):** {avg_fixed_rate:.2f}%
👥 **Tổng Users (TB):** {avg_users:.1f}
✅ **Fixed Users (TB):** {avg_fixed_users:.1f}"""
                        
                except Exception as e:
                    monitor.logging.error(f"Error in fr command: {e}")
                    return f"❌ Lỗi khi tạo báo cáo: {str(e)}"
            
            result = await asyncio.get_event_loop().run_in_executor(None, _run)
            await interaction.followup.send(result)
            
    except discord.NotFound:
        logging.error("Discord interaction not found - possibly expired or already handled")
    except Exception as e:
        logging.error(f"Error in fr command: {e}")
        try:
            if not interaction.response.is_done():
                await interaction.response.send_message(f"❌ Lỗi khi tạo báo cáo: {str(e)}", ephemeral=True)
            else:
                await interaction.followup.send(f"❌ Lỗi khi tạo báo cáo: {str(e)}")
        except:
            logging.error("Could not send error message to Discord")

@bot.tree.command(name='ping', description='Test command to check bot responsiveness')
async def ping(interaction: Interaction):
    """Simple test command that responds immediately."""
    try:
        await interaction.response.send_message("🏓 Pong! Bot is working correctly.", ephemeral=True)
    except Exception as e:
        logging.error(f"Error in ping command: {e}")

@bot.tree.command(name='bccl', description='Tạo báo cáo hàng giờ')
async def bccl(interaction: Interaction):
    # Defer the response to prevent timeout
    await interaction.response.defer()
    
    try:
        def _run():
            report = monitor.generate_hourly_report()
            monitor.send_discord_message(None, report, is_fr=True)
        await asyncio.get_event_loop().run_in_executor(None, _run)
        await interaction.followup.send("✅ Hoàn thành tạo báo cáo hàng giờ!")
    except Exception as e:
        logging.error(f"Error in bccl command: {e}")
        await interaction.followup.send(f"❌ Lỗi khi tạo báo cáo: {str(e)}")

@bot.tree.command(name='addwhitelist', description='Thêm trạm vào danh sách trắng')
@discord.app_commands.describe(stations='ST1,ST2,...')
async def addwhitelist(interaction: Interaction, stations: str):
    # Defer the response to prevent timeout
    await interaction.response.defer()
    
    try:
        def _run():
            monitor.add_whitelist(stations)
        await asyncio.get_event_loop().run_in_executor(None, _run)
        await interaction.followup.send(f"✅ Đã thêm vào danh sách trắng: {stations}")
    except Exception as e:
        logging.error(f"Error in addwhitelist command: {e}")
        await interaction.followup.send(f"❌ Lỗi khi thêm vào danh sách trắng: {str(e)}")

@bot.tree.command(name='cleanup', description='Dọn dẹp database (xóa dữ liệu cũ hơn 6 tháng)')
async def cleanup(interaction: Interaction):
    """Manual database cleanup command"""
    # Defer the response to prevent timeout - ephemeral makes it private
    await interaction.response.defer(ephemeral=True)
    
    try:
        def _run():
            try:
                deleted_counts = monitor.db.cleanup_old_data_6_months()
                stats = monitor.db.get_database_stats()
                
                total_deleted = sum(deleted_counts.values())
                if total_deleted > 0:
                    message = f"🧹 **Dọn dẹp database hoàn tất**\n"
                    message += f"📊 Đã xóa {total_deleted} bản ghi cũ hơn 6 tháng\n"
                    message += f"💾 Kích thước database: {stats.get('file_size_mb', 0)} MB\n\n"
                    message += "Chi tiết:\n"
                    for table, count in deleted_counts.items():
                        if count > 0:
                            message += f"  • {table}: {count} bản ghi\n"
                    return message
                else:
                    return f"✅ Không có dữ liệu cũ hơn 6 tháng để xóa.\n💾 Kích thước database: {stats.get('file_size_mb', 0)} MB"
            except Exception as e:
                return f"❌ Lỗi khi dọn dẹp database: {str(e)}"
        
        result = await asyncio.get_event_loop().run_in_executor(None, _run)
        await interaction.followup.send(result)
        
    except Exception as e:
        logging.error(f"Error in cleanup command: {e}")
        await interaction.followup.send(f"❌ Lỗi khi thực hiện dọn dẹp: {str(e)}")

if __name__ == '__main__':
    bot.run(BOT_TOKEN)
