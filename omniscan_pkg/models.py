import sqlite3
import os
import threading
import logging
from collections import defaultdict
from datetime import datetime
from .notifications import truncate_field_value, send_discord_webhook, format_file_list
from discord import Embed, Color
import aiohttp

logger = logging.getLogger(__name__)

class StuckFileTracker:
    def __init__(self, db_file='history.db'):
        self.db_file = db_file
        self.max_retries = 3
        self.lock = threading.Lock()
        self._init_db()

    def _init_db(self):
        with self.lock:
            try:
                conn = sqlite3.connect(self.db_file)
                cursor = conn.cursor()
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS stuck_files (
                        path TEXT PRIMARY KEY,
                        attempts INTEGER DEFAULT 0,
                        last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS events (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        event_type TEXT,
                        details TEXT,
                        status TEXT
                    )
                ''')
                conn.commit()
                conn.close()
            except Exception as e:
                logger.error(f"Failed to init DB: {e}")

    def add_event(self, event_type, details, status):
        """Add an event to the history log."""
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        with self.lock:
            try:
                conn = sqlite3.connect(self.db_file)
                cursor = conn.cursor()
                cursor.execute('INSERT INTO events (timestamp, event_type, details, status) VALUES (?, ?, ?, ?)', (timestamp, event_type, details, status))
                # Prune old events (keep last 20000)
                cursor.execute('DELETE FROM events WHERE id NOT IN (SELECT id FROM events ORDER BY id DESC LIMIT 20000)')
                conn.commit()
                conn.close()
            except Exception as e:
                logger.error(f"DB Error adding event: {e}")

    def get_history(self, limit=50, offset=0, search=None):
        """Get recent history events, optionally filtered by search term."""
        with self.lock:
            try:
                conn = sqlite3.connect(self.db_file)
                cursor = conn.cursor()
                if search:
                    search_term = f"%{search}%"
                    cursor.execute('SELECT timestamp, event_type, details, status FROM events WHERE details LIKE ? OR event_type LIKE ? ORDER BY id DESC LIMIT ? OFFSET ?', (search_term, search_term, limit, offset))
                else:
                    cursor.execute('SELECT timestamp, event_type, details, status FROM events ORDER BY id DESC LIMIT ? OFFSET ?', (limit, offset))
                rows = cursor.fetchall()
                conn.close()
                return rows
            except Exception as e:
                logger.error(f"DB Error fetching history: {e}")
                return []

    def save_history(self):
        # No-op for compatibility with existing code calling save_history
        pass

    def increment_attempt(self, file_path):
        """Increment retry count for a file. Returns True if max retries exceeded."""
        with self.lock:
            try:
                conn = sqlite3.connect(self.db_file)
                cursor = conn.cursor()
                
                # Check existing
                cursor.execute('SELECT attempts FROM stuck_files WHERE path = ?', (file_path,))
                row = cursor.fetchone()
                
                if row:
                    attempts = row[0] + 1
                    cursor.execute('UPDATE stuck_files SET attempts = ?, last_seen = CURRENT_TIMESTAMP WHERE path = ?', (attempts, file_path))
                else:
                    attempts = 1
                    cursor.execute('INSERT INTO stuck_files (path, attempts) VALUES (?, ?)', (file_path, attempts))
                
                conn.commit()
                conn.close()
                return attempts > self.max_retries
            except Exception as e:
                logger.error(f"DB Error incrementing {file_path}: {e}")
                return False

    def clear_entry(self, file_path):
        """Remove file from history if it exists."""
        with self.lock:
            try:
                conn = sqlite3.connect(self.db_file)
                cursor = conn.cursor()
                cursor.execute('DELETE FROM stuck_files WHERE path = ?', (file_path,))
                conn.commit()
                conn.close()
            except Exception as e:
                logger.error(f"DB Error clearing {file_path}: {e}")

    def get_all_stuck(self):
        """Return a list of all stuck files."""
        with self.lock:
            try:
                conn = sqlite3.connect(self.db_file)
                cursor = conn.cursor()
                cursor.execute('SELECT path, attempts, last_seen FROM stuck_files')
                rows = cursor.fetchall()
                conn.close()
                return rows
            except Exception as e:
                logger.error(f"DB Error fetching stuck files: {e}")
                return []

    def clear_all_stuck(self):
        """Clear all entries from the stuck files database."""
        with self.lock:
            try:
                conn = sqlite3.connect(self.db_file)
                cursor = conn.cursor()
                cursor.execute('DELETE FROM stuck_files')
                conn.commit()
                conn.close()
                return True
            except Exception as e:
                logger.error(f"DB Error clearing all stuck files: {e}")
                return False

class RunStats:
    def __init__(self, config):
        self.config = config
        self.start_time = datetime.now()
        self.missing_items = defaultdict(list)
        self.stuck_items = []
        self.corrupt_items = []
        self.errors = []
        self.warnings = []
        self.total_scanned = 0
        self.total_missing = 0
        self.broken_symlinks = 0
        self.lock = threading.Lock()

    def add_missing_item(self, library_name, file_path):
        with self.lock:
            self.missing_items[library_name].append(file_path)
            self.total_missing += 1

    def add_stuck_item(self, file_path):
        with self.lock:
            self.stuck_items.append(file_path)

    def add_corrupt_item(self, file_path):
        with self.lock:
            self.corrupt_items.append(file_path)

    def add_error(self, error):
        with self.lock:
            self.errors.append(error)

    def add_warning(self, warning):
        with self.lock:
            self.warnings.append(warning)

    def increment_scanned(self):
        with self.lock:
            self.total_scanned += 1

    def increment_broken_symlinks(self):
        with self.lock:
            self.broken_symlinks += 1

    def get_run_time(self):
        return datetime.now() - self.start_time

    async def send_discord_summary(self):
        if self.config.get('DRY_RUN'):
            logger.info("[DRY RUN] 📢 Would send Discord summary notification")
            return

        if not self.config['NOTIFICATIONS_ENABLED']:
            logger.info("📢 Notifications are disabled in config.ini")
            return
            
        webhook_url = self.config['DISCORD_WEBHOOK_URL']
        if not webhook_url:
            logger.warning("Discord webhook URL not configured. Skipping notification.")
            return

        try:
            # Create webhook client with aiohttp session
            async with aiohttp.ClientSession() as session:
                from discord import Webhook
                webhook = Webhook.from_url(webhook_url, session=session)

                # Create embed
                embed = Embed(
                    title="📊 Omniscan Scan Summary",
                    color=Color.blue(),
                    timestamp=datetime.now()
                )

                # Add overview
                embed.description = (
                   f"**Scan Complete**\n"
                   f"Found **{self.total_missing}** missing items\n"
                   f"Scanned **{self.total_scanned}** total files"
                )

                # Add broken symlinks summary if any
                if self.broken_symlinks > 0:
                    embed.add_field(
                        name="⚠️ Issues Detected",
                        value=f"Broken Symlinks Skipped: **{self.broken_symlinks}**",
                        inline=False
                    )

                # Add stuck items summary
                if self.stuck_items:
                    embed.add_field(
                        name=f"⛔ Stuck Files ({len(self.stuck_items)})",
                        value=format_file_list(self.stuck_items, prefix="! ", code_block=True),
                        inline=False
                    )

                # Add corrupt items summary
                if self.corrupt_items:
                    embed.add_field(
                        name=f"💀 Corrupt/Empty Files ({len(self.corrupt_items)})",
                        value=format_file_list(self.corrupt_items, prefix="x ", code_block=True),
                        inline=False
                    )

                # Add library-specific stats
                for library, items in self.missing_items.items():
                    embed.add_field(
                        name=f"📁 {library}",
                        value=f"Found: **{len(items)}** items",
                        inline=True
                    )

                # Add footer
                embed.set_footer(text=f"Omniscan Media Monitor • Run Time: {self.get_run_time()}")

                # Send webhook
                await send_discord_webhook(webhook, embed, self.config)
                logger.info("✅ Discord notification sent successfully")

        except Exception as e:
            logger.error(f"Failed to send Discord notification: {str(e)}")

    async def send_discord_pending(self, folders_count):
        if self.config.get('DRY_RUN'):
            logger.info("[DRY RUN] 📢 Would send pending scan notification")
            return

        if not self.config['NOTIFICATIONS_ENABLED']:
            return
            
        webhook_url = self.config['DISCORD_WEBHOOK_URL']
        if not webhook_url:
            return

        try:
            est_seconds = folders_count * 10 
            est_minutes = est_seconds // 60
            est_sec_remainder = est_seconds % 60
            est_str = f"{est_minutes}m {est_sec_remainder}s" if est_minutes > 0 else f"{est_seconds}s"

            async with aiohttp.ClientSession() as session:
                from discord import Webhook
                webhook = Webhook.from_url(webhook_url, session=session)

                embed = Embed(
                    title="🔍 Scan Started",
                    description=f"Scanning **{folders_count}** folders for missing items.\nEstimated time: **{est_str}**",
                    color=Color.orange(),
                    timestamp=datetime.now()
                )

                embed.add_field(
                    name="📊 Overview",
                    value=f"Found **{self.total_missing}** missing items.",
                    inline=False
                )

                for library, items in self.missing_items.items():
                    embed.add_field(
                        name=f"📁 {library} ({len(items)} items)",
                        value=format_file_list(items, max_items=10, prefix="• ", code_block=True),
                        inline=False
                    )

                embed.set_footer(text="Omniscan Media Monitor")
                await send_discord_webhook(webhook, embed, self.config)
                logger.info("✅ Pending scan notification sent successfully")

        except Exception as e:
            logger.error(f"Failed to send pending notification: {str(e)}")
