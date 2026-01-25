import os
import time
import logging
import fnmatch
import threading
import subprocess
import random
import gc
from urllib.parse import quote
from concurrent.futures import ThreadPoolExecutor
from collections import defaultdict
import requests
from plexapi.server import PlexServer
import asyncio
from datetime import datetime, timedelta
from .notifications import send_discord_webhook, format_file_list
from discord import Embed, Color
from .metrics import (
    SCANNED_FILES_TOTAL, MISSING_FILES_TOTAL, TRIGGERED_SCANS_TOTAL, 
    SCAN_ERRORS_TOTAL, WATCHED_DIRECTORIES, PENDING_SCANS,
    HEALTH_CHECKS_TOTAL, HEALTH_CHECK_FAILURES
)
from .models import StuckFileTracker

# ANSI escape codes for text formatting
BOLD = '\033[1m'
RESET = '\033[0m'

logger = logging.getLogger(__name__)

class PlexScanner:
    def __init__(self, config):
        self.config = config
        self.plex = None
        self.history = StuckFileTracker()
        self.library_ids = {}
        self.library_paths = {}
        self.library_sections_cache = []
        self.library_files = {} # Changed to dict for easier clearing
        self.library_files_lock = threading.Lock()
        self.pending_scans = {}
        self.pending_scans_lock = threading.Lock()
        self.pending_notifications = defaultdict(lambda: {'added': [], 'deleted': [], 'library_title': ''})
        self.last_health_results = [] # Store last 20 health check results
        self.is_scanning = False # Track if a full scan is currently running
        
        # Executor for processing file events asynchronously
        self.event_executor = ThreadPoolExecutor(max_workers=config.get('SCAN_WORKERS', 4))

        # Start the background worker for debounced scans
        self.worker_thread = threading.Thread(target=self._process_scan_queue, daemon=True)
        self.worker_thread.start()

    def _send_discord_embed(self, embed):
        """Send a constructed Embed to Discord."""
        if not self.config['NOTIFICATIONS_ENABLED'] or not self.config.get('DISCORD_WEBHOOK_URL'):
            return

        try:
            async def _send():
                import aiohttp
                async with aiohttp.ClientSession() as session:
                    from discord import Webhook
                    webhook = Webhook.from_url(self.config['DISCORD_WEBHOOK_URL'], session=session)
                    await send_discord_webhook(webhook, embed, self.config)
            
            asyncio.run(_send())
        except Exception as e:
            logger.error(f"Failed to send notification: {e}")

    def send_single_notification(self, title, description, color):
        """Send a single-event notification to Discord."""
        embed = Embed(title=title, description=description, color=color, timestamp=datetime.now())
        embed.set_footer(text="Omniscan Media Monitor")
        self._send_discord_embed(embed)

    def connect_to_plex(self, retry=True):
        """Connect to Plex. If retry=True, loops until connected. If False, raises error on failure."""
        if self.config.get('SERVER_TYPE', 'plex') != 'plex':
            return None
            
        retry_delay = 5
        max_delay = 300  # 5 minutes
        
        while True:
            try:
                if not self.config['PLEX_URL'] or not self.config['TOKEN']:
                    if not retry: raise ValueError("PLEX_SERVER or PLEX_TOKEN not configured.")
                    logger.error("PLEX_SERVER or PLEX_TOKEN not configured.")
                    return None
                    
                self.plex = PlexServer(self.config['PLEX_URL'], self.config['TOKEN'])
                # Test connection
                logger.info(f"Connected to Plex: {self.plex.friendlyName} (v{self.plex.version})")
                return self.plex
            except Exception as e:
                if not retry:
                    raise e
                logger.error(f"Failed to connect to Plex ({self.config['PLEX_URL']}): {e}")
                logger.info(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, max_delay)

    def is_ignored(self, file_path):
        """Check if file matches any ignore pattern."""
        filename = os.path.basename(file_path)
        for pattern in self.config['IGNORE_PATTERNS']:
            if fnmatch.fnmatch(filename, pattern):
                return True
            if fnmatch.fnmatch(file_path, pattern):
                return True
        return False

    def get_library_ids(self):
        """Fetch library section IDs and paths dynamically from Plex or Jellyfin/Emby."""
        self.library_sections_cache = []
        server_type = self.config.get('SERVER_TYPE', 'plex')

        if server_type == 'plex':
            if not self.plex: return {}
            for section in self.plex.library.sections():
                lib_type = section.type
                lib_key = section.key
                lib_title = section.title
                self.library_ids[lib_type] = lib_key
                
                section_locations = []
                for location in section.locations:
                    self.library_paths[location] = lib_key
                    section_locations.append(location)
                    logger.debug(f"Found library '{lib_title}' (ID: {lib_key}) at path: {location}")
                    
                self.library_sections_cache.append({
                    'id': lib_key,
                    'title': lib_title,
                    'type': lib_type,
                    'locations': section_locations
                })
        elif server_type in ['jellyfin', 'emby']:
            self._get_jellyfin_libraries()

        return self.library_ids

    def _get_jellyfin_libraries(self):
        """Fetch libraries from Jellyfin/Emby."""
        url = f"{self.config['SERVER_URL']}/Library/VirtualFolders"
        headers = {"X-Emby-Token": self.config['API_KEY']}
        try:
            res = requests.get(url, headers=headers)
            res.raise_for_status()
            data = res.json()
            
            for item in data:
                lib_title = item.get('Name')
                lib_id = item.get('ItemId') # Actually usually not needed for refresh, but good for ID
                locations = item.get('Locations', [])
                collection_type = item.get('CollectionType', 'unknown') # movies, tvshows
                
                # Normalize types to match internal logic if needed, or keep raw
                self.library_sections_cache.append({
                    'id': lib_id,
                    'title': lib_title,
                    'type': collection_type,
                    'locations': locations
                })
                logger.debug(f"Found {self.config['SERVER_TYPE']} library '{lib_title}' at: {locations}")
        except Exception as e:
            logger.error(f"Failed to fetch {self.config['SERVER_TYPE']} libraries: {e}")

    def get_library_id_for_path(self, file_path):
        """Get the library section ID and type for a given file path from cache."""
        best_match = None
        best_match_length = 0
        
        for section in self.library_sections_cache:
            section_id = section['id']
            section_title = section['title']
            section_type = section['type']
            
            for location_path in section['locations']:
                normalized_scan_path = os.path.normpath(file_path)
                normalized_location = os.path.normpath(location_path)
                
                if normalized_scan_path.startswith(normalized_location):
                    if len(normalized_location) > best_match_length:
                        best_match = (section_id, section_title, section_type)
                        best_match_length = len(normalized_location)
        
        if best_match:
            return best_match
        
        return None, None, None

    def cache_library_files(self, library_id):
        """Cache all files in a library section."""
        with self.library_files_lock:
            if library_id in self.library_files and self.library_files[library_id]:
                return
        
        try:
            section = self.plex.library.sectionByID(int(library_id))
            logger.info(f"💾 Initializing cache for library {BOLD}{section.title}{RESET}...")
            cache_start = time.time()
            
            items = []
            if section.type == 'show':
                items = section.search(libtype='episode')
            else:
                items = section.all()

            count = 0
            new_files = set()
            for item in items:
                for media in item.media:
                    for part in media.parts:
                        if part.file:
                            new_files.add(part.file)
                            count += 1
            
            # Clear items list immediately to free memory
            del items

            with self.library_files_lock:
                self.library_files[library_id] = new_files

            cache_time = time.time() - cache_start
            logger.info(f"💾 Cache initialized for library {BOLD}{section.title}{RESET}: {BOLD}{count}{RESET} files in {BOLD}{cache_time:.2f}{RESET} seconds")
        except Exception as e:
            logger.error(f"Error caching library {library_id}: {str(e)}")

    def is_in_library(self, file_path):
        """Check if a file exists in the media server."""
        server_type = self.config.get('SERVER_TYPE', 'plex')
        
        # Check cache if it exists (usually during full scan)
        library_id, library_title, _ = self.get_library_id_for_path(file_path)
        if library_id:
            with self.library_files_lock:
                if library_id in self.library_files and self.library_files[library_id]:
                    return file_path in self.library_files[library_id]

        # If cache is empty or missing, fallback to direct API check (memory efficient for watcher)
        if server_type == 'plex':
            return self._is_in_plex_api(file_path, library_id)
        elif server_type in ['jellyfin', 'emby']:
            return self._is_in_jellyfin_api(file_path, library_id)
        return False

    def _is_in_plex_api(self, file_path, library_id=None):
        """Directly check Plex API for a file without using a large RAM cache."""
        if not self.plex: return False
        try:
            if not library_id:
                library_id, _, _ = self.get_library_id_for_path(file_path)
            
            if not library_id: return False
            
            section = self.plex.library.sectionByID(int(library_id))
            # Search by filepath (Plex supports this filter)
            results = section.search(filepath=file_path)
            return len(results) > 0
        except Exception as e:
            logger.debug(f"Direct Plex check failed for {file_path}: {e}")
            return False

    def _is_in_jellyfin_api(self, file_path, library_id=None):
        """Check if file exists in Jellyfin/Emby via API search."""
        if not library_id:
            library_id, _, _ = self.get_library_id_for_path(file_path)
        if not library_id: return False
        
        url = f"{self.config['SERVER_URL']}/Items?ParentId={library_id}&Recursive=true&Fields=Path&IncludeItemTypes=Movie,Episode"
        headers = {"X-Emby-Token": self.config['API_KEY']}
        try:
            # We don't want to fetch all items if we are just checking one
            # Jellyfin supports Path filter in some versions or via Search
            # For simplicity, if we don't have cache, we might have to use a targeted query
            # Search by term (filename) is often faster than fetching all
            filename = os.path.basename(file_path)
            search_url = f"{self.config['SERVER_URL']}/Items?ParentId={library_id}&Recursive=true&Fields=Path&IncludeItemTypes=Movie,Episode&searchTerm={quote(filename)}"
            res = requests.get(search_url, headers=headers)
            res.raise_for_status()
            items = res.json().get('Items', [])
            for item in items:
                if item.get('Path') == file_path:
                    return True
            return False
        except Exception as e:
            logger.error(f"Failed to check {self.config['SERVER_TYPE']} for {file_path}: {e}")
            return False

    def _is_in_jellyfin(self, file_path):
        """Legacy method for compatibility, now calls is_in_library logic."""
        return self.is_in_library(file_path)

    def _cache_jellyfin_library(self, library_id):
        try:
            # Fetch all items in this library (ParentId = library_id)
            url = f"{self.config['SERVER_URL']}/Items?ParentId={library_id}&Recursive=true&Fields=Path&IncludeItemTypes=Movie,Episode"
            headers = {"X-Emby-Token": self.config['API_KEY']}
            res = requests.get(url, headers=headers)
            res.raise_for_status()
            data = res.json()
            items = data.get('Items', [])
            
            new_files = set()
            for item in items:
                if 'Path' in item:
                    new_files.add(item['Path'])
            
            # Clear large objects to free memory
            del data
            del items

            with self.library_files_lock:
                self.library_files[library_id] = new_files
            
            logger.info(f"💾 Cached {len(new_files)} items for {self.config['SERVER_TYPE']} library {library_id}")
        except Exception as e:
            logger.error(f"Failed to cache {self.config['SERVER_TYPE']} library: {e}")

    def _is_in_plex(self, file_path):
        """Legacy method for compatibility, now calls is_in_library logic."""
        return self.is_in_library(file_path)

    def get_entity_root(self, file_path):
        """Get the root folder of the show or movie for batching scans."""
        library_id, library_title, library_type = self.get_library_id_for_path(file_path)
        if not library_id:
            return os.path.dirname(file_path)
            
        # Find the matching library location
        best_location = None
        for section in self.library_sections_cache:
            if str(section['id']) == str(library_id):
                for location in section['locations']:
                    if os.path.normpath(file_path).startswith(os.path.normpath(location)):
                        if not best_location or len(location) > len(best_location):
                            best_location = os.path.normpath(location)
        
        if not best_location:
            return os.path.dirname(file_path)
            
        rel_path = os.path.relpath(file_path, best_location)
        parts = rel_path.split(os.sep)
        
        if len(parts) > 1:
            # It's in a subfolder of the library root. 
            # Usually Library/Show Name/Season/File or Library/Movie Name/File
            # We return the first directory after the library root.
            return os.path.join(best_location, parts[0])
        else:
            # It's directly in the library root or parts[0] is the file itself
            return best_location

    def is_library_root(self, library_id, folder_path):
        """Check if the given folder path is a root location for the library."""
        for section in self.library_sections_cache:
            if str(section['id']) == str(library_id):
                for location in section['locations']:
                    if os.path.normpath(folder_path) == os.path.normpath(location):
                        return True
        return False

    def trigger_scan(self, library_id, folder_path, force=False):
        """Enqueue a library scan for a specific folder."""
        if force:
            self._do_trigger_scan(library_id, folder_path)
            return

        with self.pending_scans_lock:
            # Update the last event time for this (library, folder)
            self.pending_scans[(library_id, folder_path)] = time.time()
            logger.info(f"⏳ Scan queued (debouncing): {BOLD}{folder_path}{RESET}")

    def _process_scan_queue(self):
        """Background worker to process debounced scans and notifications."""
        while True:
            try:
                time.sleep(1)
                to_trigger = []
                
                with self.pending_scans_lock:
                    PENDING_SCANS.set(len(self.pending_scans))
                    now = time.time()
                    debounce_delay = self.config.get('SCAN_DEBOUNCE', 10)
                    
                    for key, last_time in list(self.pending_scans.items()):
                        if now - last_time >= debounce_delay:
                            library_id, folder_path = key
                            to_trigger.append((library_id, folder_path))
                            
                            # Prepare notification
                            notif_data = self.pending_notifications.get(folder_path)
                            if notif_data:
                                self._send_grouped_notification(folder_path, notif_data)
                                del self.pending_notifications[folder_path]
                                
                            del self.pending_scans[key]
                
                for library_id, folder_path in to_trigger:
                    self._do_trigger_scan(library_id, folder_path)
            except Exception as e:
                logger.error(f"Error in scan queue worker: {e}")
                time.sleep(5)

    def _send_grouped_notification(self, entity_root, data):
        """Send a single Discord notification for multiple file events."""
        added = data['added']
        deleted = data['deleted']
        library = data['library_title'] or "Unknown Library"
        entity_name = os.path.basename(entity_root)
        
        # Determine Color
        color = Color.blue()
        if added and deleted:
            color = Color.gold() # Mixed changes
        elif added:
            color = Color.green()
        elif deleted:
            color = Color.red()

        embed = Embed(
            title=f"📂 Update: {library}",
            description=f"Changes detected in **{entity_name}**",
            color=color,
            timestamp=datetime.now()
        )
        
        if added:
            embed.add_field(
                name=f"✅ Added ({len(added)})", 
                value=format_file_list(added, max_items=10, prefix="+ ", code_block=True, language="diff"), 
                inline=False
            )
        
        if deleted:
            embed.add_field(
                name=f"🗑️ Deleted ({len(deleted)})", 
                value=format_file_list(deleted, max_items=10, prefix="- ", code_block=True, language="diff"), 
                inline=False
            )

        embed.set_footer(text="Omniscan Media Monitor")
        self._send_discord_embed(embed)

    def _do_trigger_scan(self, library_id, folder_path):
        """Actually trigger a library scan for a specific folder and wait for completion."""
        TRIGGERED_SCANS_TOTAL.inc()
        if self.config.get('DRY_RUN'):
            logger.info(f"[DRY RUN] 🔎 Would trigger scan for: {BOLD}{folder_path}{RESET}")
            return

        server_type = self.config.get('SERVER_TYPE', 'plex')
        
        if server_type == 'plex':
            self._trigger_plex_scan(library_id, folder_path)
        elif server_type in ['jellyfin', 'emby']:
            self._trigger_jellyfin_emby_scan(library_id, folder_path)

    def _trigger_jellyfin_emby_scan(self, library_id, folder_path):
        """Trigger a scan for Jellyfin or Emby (they share similar path-based scan APIs)."""
        url = f"{self.config['SERVER_URL']}/Library/Media/Updated"
        headers = {
            "X-Emby-Token": self.config['API_KEY'],
            "Content-Type": "application/json"
        }
        # Jellyfin/Emby usually take a list of paths to check
        payload = {
            "Updates": [{"Path": folder_path}]
        }
        
        try:
            response = requests.post(url, json=payload, headers=headers)
            response.raise_for_status()
            logger.info(f"🔎 {self.config['SERVER_TYPE'].capitalize()} scan triggered for: {BOLD}{folder_path}{RESET}")
            self.history.add_event("Scan Triggered", folder_path, self.config['SERVER_TYPE'])
        except Exception as e:
            logger.error(f"Failed to trigger {self.config['SERVER_TYPE']} scan: {e}")

    def _trigger_plex_scan(self, library_id, folder_path):
        library_id = str(library_id)
        encoded_path = quote(folder_path)
        url = f"{self.config['PLEX_URL']}/library/sections/{library_id}/refresh?path={encoded_path}&X-Plex-Token={self.config['TOKEN']}"
        
        try:
            response = requests.get(url)
            response.raise_for_status()
            logger.info(f"🔎 Plex scan triggered for: {BOLD}{folder_path}{RESET}")
            self.history.add_event("Scan Triggered", folder_path, "Plex")
            
            time.sleep(5) 
            
            max_wait = 600
            start_wait = time.time()
            
            while True:
                if time.time() - start_wait > max_wait:
                    logger.warning(f"⚠️ Scan wait timed out for: {folder_path}")
                    break

                try:
                    is_scanning = False
                    for activity in self.plex.activities:
                        if activity.type == 'library.refresh.section' and str(activity.sectionID) == library_id:
                            is_scanning = True
                            break
                    
                    if not is_scanning:
                        logger.info(f"✅ Plex scan finished for: {folder_path}")
                        break
                    
                    time.sleep(5)
                except Exception as e:
                    logger.error(f"Error checking scan status: {e}")
                    time.sleep(5)
        except Exception as e:
            logger.error(f"Failed to trigger Plex scan for {folder_path}: {e}")

    def is_broken_symlink(self, file_path):
        if not os.path.islink(file_path):
            return False
        return not os.path.exists(os.path.realpath(file_path))

    def check_file_health(self, file_path):
        """Check file integrity using tail-read, sampled reads, and ffprobe. Returns (is_healthy, status_dict)."""
        HEALTH_CHECKS_TOTAL.inc()
        health_status = {
            "file": os.path.basename(file_path), 
            "time": datetime.now().strftime("%H:%M:%S"),
            "status": "Unknown"
        }

        try:
            # 1. Check for 0-byte files
            file_size = os.path.getsize(file_path)
            if file_size == 0:
                logger.warning(f"File corruption detected (0 bytes): {file_path}")
                health_status.update({"status": "Corrupt", "error": "0 Bytes"})
                self._add_health_result(health_status)
                self.history.add_event("Health Check Failed", os.path.basename(file_path), "0 Bytes")
                HEALTH_CHECK_FAILURES.inc()
                return False, health_status

            # 2. Hybrid Verification (Tail Read + Random Sampling)
            # This is crucial for detecting truncated or sparse files on Rclone/Usenet
            try:
                with open(file_path, 'rb') as f:
                    # A. Tail Read
                    seek_pos = max(0, file_size - (1024 * 1024))
                    f.seek(seek_pos)
                    f.read(1024) 

                    # B. Random Sampling (3 spots in the middle)
                    if file_size > (5 * 1024 * 1024): # Only sample if > 5MB
                        for _ in range(3):
                            random_pos = random.randint(1024 * 1024, file_size - (1024 * 1024))
                            f.seek(random_pos)
                            if not f.read(1024):
                                raise IOError("Empty read at sampled position")

            except (OSError, IOError) as e:
                logger.warning(f"Data verification failed (Read Error): {file_path} - {e}")
                health_status.update({"status": "Corrupt", "error": "Incomplete/Read Error"})
                self._add_health_result(health_status)
                self.history.add_event("Health Check Failed", os.path.basename(file_path), "Incomplete")
                HEALTH_CHECK_FAILURES.inc()
                return False, health_status

            # 3. FFprobe Metadata Check
            cmd = [
                'ffprobe', 
                '-v', 'error', 
                '-show_entries', 'format=duration', 
                '-of', 'default=noprint_wrappers=1:nokey=1',
                file_path
            ]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)

            if result.returncode != 0:
                logger.warning(f"File corruption detected (ffprobe error): {file_path} - {result.stderr}")
                HEALTH_CHECK_FAILURES.inc()
                health_status.update({"status": "Corrupt", "error": "Bitstream Error"})
                self._add_health_result(health_status)
                self.history.add_event("Health Check Failed", os.path.basename(file_path), "Corrupt")
                return False, health_status
            
            duration_str = result.stdout.strip()
            if not duration_str:
                logger.warning(f"File corruption detected (no duration): {file_path}")
                health_status.update({"status": "Corrupt", "error": "No Duration"})
                self._add_health_result(health_status)
                self.history.add_event("Health Check Failed", os.path.basename(file_path), "No Duration")
                return False, health_status
            
            # Sample Detection
            if self.config.get('IGNORE_SAMPLES'):
                try:
                    duration = float(duration_str)
                    if duration < self.config['MIN_DURATION']:
                        logger.info(f"⏩ Ignoring sample/short file ({duration}s): {file_path}")
                        health_status.update({"status": "Ignored", "error": f"Sample ({int(duration)}s)"})
                        self._add_health_result(health_status)
                        self.history.add_event("Sample Ignored", os.path.basename(file_path), f"{int(duration)}s")
                        return False, health_status
                except ValueError:
                    pass
            
            health_status.update({"status": "Healthy"})
            self._add_health_result(health_status)
            self.history.add_event("Health Check Passed", os.path.basename(file_path), "Healthy")
            return True, health_status

        except subprocess.TimeoutExpired:
            logger.warning(f"File health check timed out: {file_path}")
            health_status.update({"status": "Timeout", "error": "Scan Timed Out"})
            self._add_health_result(health_status)
            self.history.add_event("Health Check Timeout", os.path.basename(file_path), "Timeout")
            HEALTH_CHECK_FAILURES.inc()
            return False, health_status
        except Exception as e:
            logger.error(f"Error running health check on {file_path}: {e}")
            health_status.update({"status": "Error", "error": str(e)})
            self._add_health_result(health_status)
            self.history.add_event("Health Check Error", os.path.basename(file_path), "Error")
            HEALTH_CHECK_FAILURES.inc()
            return False, health_status

    def _add_health_result(self, result):
        self.last_health_results.insert(0, result)
        if len(self.last_health_results) > 20:
            self.last_health_results.pop()

    def submit_file_event(self, event_type, file_path):
        """Submit a file event for asynchronous processing."""
        if event_type == 'created' or event_type == 'moved':
            self.event_executor.submit(self.scan_file, file_path)
        elif event_type == 'deleted':
            self.event_executor.submit(self.handle_deletion, file_path)

    def scan_file(self, file_path, stats=None, tracker=None):
        """Scan a single file and trigger Plex refresh if missing."""
        if self.is_ignored(file_path):
            return

        if self.config['SYMLINK_CHECK'] and self.is_broken_symlink(file_path):
            if stats: stats.increment_broken_symlinks()
            return

        try:
            if os.path.getsize(file_path) == 0:
                if stats: stats.add_corrupt_item(file_path)
                logger.warning(f"Skipping empty file: {file_path}")
                return
        except OSError:
            return

        file_name = os.path.basename(file_path)
        file_ext = os.path.splitext(file_name)[1].lower()
        if file_ext not in self.config['MEDIA_EXTENSIONS']:
            return

        if stats: stats.increment_scanned()
        SCANNED_FILES_TOTAL.inc()

        if not self.is_in_library(file_path):
            logger.info(f"🆕 Found new file: {BOLD}{file_path}{RESET}")
            
            # Health Check & Sample Detection
            if self.config.get('HEALTH_CHECK'):
                is_healthy, _ = self.check_file_health(file_path)
                if not is_healthy:
                    if stats: stats.add_corrupt_item(file_path)
                    self.send_single_notification(
                        "⚠️ Corrupt File Detected", 
                        f"The file is corrupt or empty and will be skipped:\n**{os.path.basename(file_path)}**", 
                        Color.red()
                    )
                    return

            MISSING_FILES_TOTAL.inc()
            library_id, library_title, library_type = self.get_library_id_for_path(file_path)
            
            if library_title or self.config.get('SERVER_TYPE') != 'plex':
                should_scan = True
                if tracker:
                    if tracker.increment_attempt(file_path):
                        stats.add_stuck_item(file_path)
                        should_scan = False
                    else:
                        stats.add_missing_item(library_title, file_path)
                
                if should_scan:
                    # Enqueue for notification
                    parent_folder = os.path.dirname(file_path)
                    
                    # If parent is library root (flat structure), scan specific file to avoid full scan
                    target_path = file_path if self.is_library_root(library_id, parent_folder) else parent_folder
                    
                    with self.pending_scans_lock:
                        # Use target_path as key for notifications so they group correctly with the scan
                        if target_path not in self.pending_notifications:
                            self.pending_notifications[target_path]['library_title'] = library_title
                        self.pending_notifications[target_path]['added'].append(os.path.basename(file_path))
                    
                    self.trigger_scan(library_id, target_path)
        else:
            if tracker: tracker.clear_entry(file_path)

    def handle_deletion(self, file_path):
        # Filter by extension first
        file_name = os.path.basename(file_path)
        file_ext = os.path.splitext(file_name)[1].lower()
        if file_ext not in self.config['MEDIA_EXTENSIONS']:
            return

        # Double-check if file is actually gone (to prevent Rclone/Network false positives)
        if os.path.exists(file_path):
            logger.debug(f"False positive deletion ignored (file exists): {file_path}")
            return

        # Check if the root scan path itself is accessible. 
        # If the root of the scan is missing, the mount is likely down.
        scan_root = None
        for path in self.config['SCAN_PATHS']:
             if file_path.startswith(path):
                 scan_root = path
                 break
        
        if scan_root and not os.path.exists(scan_root):
            logger.warning(f"🛑 Scan root not accessible: {scan_root}. Assuming mount failure. Ignoring deletion of {file_path}")
            return
        
        # Small delay to filter out transient glitches (e.g. during renames or network hiccups)
        time.sleep(2)
        if os.path.exists(file_path):
            logger.debug(f"False positive deletion ignored (file reappeared): {file_path}")
            return

        # Check if parent directory still exists. If the whole folder is gone, we might be seeing a recursive delete.
        # But if the folder is gone, the file is definitely gone, so we proceed.
        
        logger.info(f"🗑️ File deleted: {BOLD}{file_path}{RESET}")
        
        library_id, library_title, library_type = self.get_library_id_for_path(file_path)
        if library_id or self.config.get('SERVER_TYPE') != 'plex':
            # Enqueue for notification
            parent_folder = os.path.dirname(file_path)
            
            # If parent is library root (flat structure), trigger for file path (though file is gone, Plex might need specific path or parent)
            # Actually for deletion, scanning parent is usually safer to ensure it's removed? 
            # But if parent is root, we trigger full scan.
            # Plex "refresh" on a deleted file path might not work if file is gone?
            # It usually does for emptying the trash or detecting change.
            # Let's try targeting the file path if root.
            target_path = file_path if self.is_library_root(library_id, parent_folder) else parent_folder

            with self.pending_scans_lock:
                if target_path not in self.pending_notifications:
                    self.pending_notifications[target_path]['library_title'] = library_title or "Media"
                self.pending_notifications[target_path]['deleted'].append(os.path.basename(file_path))

            self.trigger_scan(library_id, target_path)

    def scan_directory(self, path, stats, tracker, folders_to_scan, folders_to_scan_lock):
        for root, dirs, files in os.walk(path, followlinks=True):
            
            if self.config.get('INCREMENTAL_SCAN'):
                try:
                    mtime = os.path.getmtime(root)
                    cutoff_time = time.time() - (self.config['SCAN_SINCE_DAYS'] * 86400)
                    if mtime < cutoff_time:
                        continue
                except OSError:
                    pass

            dirs.sort()
            files.sort()
            for file in files:
                
                # Rate Limiting
                if self.config['SCAN_DELAY'] > 0:
                    time.sleep(self.config['SCAN_DELAY'])

                if file.startswith('.'):
                    continue

                file_ext = os.path.splitext(file)[1].lower()
                if file_ext not in self.config['MEDIA_EXTENSIONS']:
                    continue

                file_path = os.path.join(root, file)
                
                if self.is_ignored(file_path):
                    continue

                if self.config['SYMLINK_CHECK'] and self.is_broken_symlink(file_path):
                    stats.increment_broken_symlinks()
                    continue

                try:
                    if os.path.getsize(file_path) == 0:
                        stats.add_corrupt_item(file_path)
                        continue
                except OSError:
                    continue

                stats.increment_scanned()
                SCANNED_FILES_TOTAL.inc()

                if not self.is_in_library(file_path):
                    if self.config.get('HEALTH_CHECK'):
                        is_healthy, _ = self.check_file_health(file_path)
                        if not is_healthy:
                            stats.add_corrupt_item(file_path)
                            # check_file_health handles logging
                            continue

                    library_id, library_title, library_type = self.get_library_id_for_path(file_path)
                    
                    if library_title:
                        if tracker.increment_attempt(file_path):
                            stats.add_stuck_item(file_path)
                        else:
                            stats.add_missing_item(library_title, file_path)
                            parent_folder = os.path.dirname(file_path)
                            
                            # If parent is library root, scan file instead
                            target_path = file_path if self.is_library_root(library_id, parent_folder) else parent_folder
                            
                            with folders_to_scan_lock:
                                folders_to_scan.add((library_id, target_path))
                else:
                    tracker.clear_entry(file_path)

    def run_scan(self):
        from .models import RunStats, StuckFileTracker
        if self.is_scanning:
            logger.warning("Scan already in progress, skipping...")
            return
            
        self.is_scanning = True
        try:
            stats = RunStats(self.config)
            tracker = StuckFileTracker()
            
            # Use lock when clearing and re-filling cache
            with self.library_files_lock:
                self.library_files.clear()
            logger.info("Cache cleared for new scan")
            
            if not self.plex:
                self.connect_to_plex()

            self.get_library_ids()

            # Pre-cache libraries to prevent race conditions during parallel scanning
            for section in self.library_sections_cache:
                self.cache_library_files(section['id'])

            folders_to_scan = set()
            folders_to_scan_lock = threading.Lock()
            
            WATCHED_DIRECTORIES.set(len(self.config['SCAN_PATHS']))
            max_workers = self.config['SCAN_WORKERS']
            
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = []
                
                for SCAN_PATH in self.config['SCAN_PATHS']:
                    logger.info(f"\nScanning directory: {BOLD}{SCAN_PATH}{RESET}")

                    if not os.path.isdir(SCAN_PATH):
                        error_msg = f"Directory not found: {SCAN_PATH}"
                        logger.error(error_msg)
                        stats.add_error(error_msg)
                        continue

                    try:
                        # Iterate directly instead of converting to list
                        with os.scandir(SCAN_PATH) as it:
                            for entry in it:
                                if entry.name.startswith('.'): continue
                                
                                if entry.is_dir():
                                    if not self.is_ignored(entry.path):
                                        futures.append(executor.submit(self.scan_directory, entry.path, stats, tracker, folders_to_scan, folders_to_scan_lock))
                                elif entry.is_file():
                                    file_path = entry.path
                                    if self.is_ignored(file_path): continue
                                    
                                    if self.config['SYMLINK_CHECK'] and self.is_broken_symlink(file_path):
                                        stats.increment_broken_symlinks()
                                        continue
                                        
                                    try:
                                        if os.path.getsize(file_path) == 0:
                                            stats.add_corrupt_item(file_path)
                                            continue
                                    except OSError:
                                        continue
                                        
                                    file_name = os.path.basename(file_path)
                                    file_ext = os.path.splitext(file_name)[1].lower()
                                    if file_ext not in self.config['MEDIA_EXTENSIONS']: continue
                                    
                                    stats.increment_scanned()
                                    SCANNED_FILES_TOTAL.inc()
                                    
                                    if not self.is_in_library(file_path):
                                        if self.config.get('HEALTH_CHECK'):
                                           is_healthy, _ = self.check_file_health(file_path)
                                           if not is_healthy:
                                               stats.add_corrupt_item(file_path)
                                               continue

                                        library_id, library_title, library_type = self.get_library_id_for_path(file_path)
                                        if library_title:
                                            if tracker.increment_attempt(file_path):
                                                stats.add_stuck_item(file_path)
                                            else:
                                                stats.add_missing_item(library_title, file_path)
                                                parent_folder = os.path.dirname(file_path)
                                                with folders_to_scan_lock:
                                                    folders_to_scan.add((library_id, parent_folder))
                                    else:
                                        tracker.clear_entry(file_path)
                    except OSError as e:
                        logger.error(f"Error accessing {SCAN_PATH}: {e}")
                        continue

                for future in futures:
                    future.result()

            if stats.total_missing > 0:
                asyncio.run(stats.send_discord_pending(len(folders_to_scan)))
                
                sorted_folders = sorted(list(folders_to_scan), key=lambda x: x[1])
                for library_id, folder_path in sorted_folders:
                     self.trigger_scan(library_id, folder_path)

            tracker.save_history()
            asyncio.run(stats.send_discord_summary())
            
        except Exception as e:
            logger.error(f"Error during scan: {e}")
        finally:
            self.is_scanning = False
            # Clear cache after full scan to save memory
            with self.library_files_lock:
                self.library_files.clear()
            gc.collect() # Trigger garbage collection to release memory immediately