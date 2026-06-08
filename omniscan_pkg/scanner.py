import os
import time
import logging
import fnmatch
import threading
import gc
import queue
from urllib.parse import quote
from concurrent.futures import ThreadPoolExecutor
from collections import defaultdict
import requests
from plexapi.server import PlexServer
from datetime import datetime, timedelta
from .notifications import send_discord_webhook_sync, format_file_list
from discord import Embed, Color
from .metrics import (
    SCANNED_FILES_TOTAL, MISSING_FILES_TOTAL, TRIGGERED_SCANS_TOTAL, 
    SCAN_ERRORS_TOTAL, WATCHED_DIRECTORIES, PENDING_SCANS
)
from .models import StuckFileTracker

# ANSI escape codes for text formatting
BOLD = '\033[1m'
RESET = '\033[0m'

logger = logging.getLogger(__name__)

import re

class PlexScanner:
    def __init__(self, config):
        self.config = config
        self.plex = None
        
        # Compile ignore patterns for performance
        self.ignore_regex = None
        if config.get('IGNORE_PATTERNS'):
            # Convert glob patterns to regex
            # fnmatch.translate converts glob to regex, we join them with OR
            try:
                patterns = [fnmatch.translate(p) for p in config['IGNORE_PATTERNS'] if p.strip()]
                if patterns:
                    self.ignore_regex = re.compile('|'.join(patterns))
            except Exception as e:
                logger.error(f"Failed to compile ignore patterns: {e}")

        self.history = StuckFileTracker()
        self.library_ids = {}
        self.library_paths = {}
        self.library_sections_cache = []
        self.library_files = {} # Changed to dict for easier clearing
        self.library_files_lock = threading.Lock()
        self.loading_libraries = set()
        self.loading_lock = threading.Lock()
        self.pending_scans = {}
        self.pending_scans_lock = threading.Lock()
        self.pending_notifications = defaultdict(lambda: {'added': [], 'deleted': [], 'library_title': ''})
        self.is_scanning = False # Track if a full scan is currently running
        self.pending_files = set() # Track files currently queued for scan to prevent duplicates
        self.pending_files_lock = threading.Lock()
        
        # Persistent session for connection pooling
        self.http_session = requests.Session()
        self.http_session.headers.update({
            'User-Agent': 'Omniscan/1.0'
        })
        
        # Executor for processing file events asynchronously
        self.event_executor = ThreadPoolExecutor(max_workers=config.get('SCAN_WORKERS', 4))
        
        # Executor for monitoring Plex scans without blocking the queue
        self.scan_monitor_executor = ThreadPoolExecutor(max_workers=4)

        # Start the background worker for debounced scans
        self.worker_thread = threading.Thread(target=self._process_scan_queue, daemon=True)
        self.worker_thread.start()

        # Notification queue and worker
        self.notification_queue = queue.Queue()
        self.notification_worker_thread = threading.Thread(target=self._notification_worker, daemon=True)
        self.notification_worker_thread.start()

    def _notification_worker(self):
        """Sequential worker for sending Discord notifications to avoid rate limits."""
        while True:
            try:
                embed = self.notification_queue.get()
                if send_discord_webhook_sync(self.config['DISCORD_WEBHOOK_URL'], embed, self.config):
                    logger.info(f"✅ Discord notification sent: {embed.title}")
                else:
                    logger.error(f"❌ Failed to send Discord notification: {embed.title}")
                
                # Small delay between notifications to be safe
                time.sleep(1)
                self.notification_queue.task_done()
            except Exception as e:
                logger.error(f"Error in notification worker: {e}")
                time.sleep(5)

    def _send_discord_embed(self, embed):
        """Queue a constructed Embed for the notification worker."""
        if not self.config.get('NOTIFICATIONS_ENABLED', True) or not self.config.get('DISCORD_WEBHOOK_URL'):
            return
        
        self.notification_queue.put(embed)

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
                    
                self.plex = PlexServer(self.config['PLEX_URL'], self.config['TOKEN'], session=self.http_session)
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
        """Check if file matches any ignore pattern using compiled regex."""
        if not self.ignore_regex:
            return False
        
        # Check both filename and full path to match standard behavior
        # (Usually full path match is what users want for folders like /RecycleBin)
        if self.ignore_regex.match(file_path):
            return True
        if self.ignore_regex.match(os.path.basename(file_path)):
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
                lib_key = str(section.key)
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
        import pathlib
        best_match = None
        best_match_length = 0
        
        path = pathlib.Path(os.path.normpath(file_path))
        
        for section in self.library_sections_cache:
            section_id = section['id']
            section_title = section['title']
            section_type = section['type']
            
            for location_path in section['locations']:
                loc = pathlib.Path(os.path.normpath(location_path))
                
                # Check if path is the location itself or a child of it
                if loc == path or loc in path.parents:
                    if len(str(loc)) > best_match_length:
                        best_match = (section_id, section_title, section_type)
                        best_match_length = len(str(loc))
        
        if best_match:
            return best_match
        
        return None, None, None

    def cache_library_files(self, library_id):
        """Cache all files in a library section using paginated fetching to save memory."""
        library_id = str(library_id)
        with self.library_files_lock:
            if library_id in self.library_files and self.library_files[library_id]:
                return
        
        server_type = self.config.get('SERVER_TYPE', 'plex')
        if server_type in ['jellyfin', 'emby']:
            self._cache_jellyfin_library(library_id)
            return

        try:
            section = self.plex.library.sectionByID(int(library_id))
            logger.info(f"💾 Initializing cache for library {BOLD}{section.title}{RESET}...")
            cache_start = time.time()
            
            batch_size = 5000
            start = 0
            new_files = set()
            count = 0
            
            while True:
                items = []
                if section.type == 'show':
                    items = section.search(libtype='episode', container_start=start, maxresults=batch_size)
                elif section.type == 'artist':
                    items = section.search(libtype='track', container_start=start, maxresults=batch_size)
                else:
                    items = section.all(container_start=start, maxresults=batch_size)

                if not items:
                    break

                for item in items:
                    for media in item.media:
                        for part in media.parts:
                            if part.file:
                                new_files.add(os.path.normpath(part.file))
                                count += 1
                
                batch_count = len(items)
                start += batch_count
                
                # Free memory after each batch
                del items
                gc.collect()

                if batch_count < batch_size:
                    break

            with self.library_files_lock:
                self.library_files[library_id] = new_files

            cache_time = time.time() - cache_start
            logger.info(f"💾 Cache initialized for library {BOLD}{section.title}{RESET}: {BOLD}{count}{RESET} files in {BOLD}{cache_time:.2f}{RESET} seconds")
        except Exception as e:
            logger.error(f"Error caching library {library_id}: {str(e)}")

    def _trigger_cache_fill(self, library_id):
        # Optimization: Only fill if notifications or stats need it, 
        # but actually we almost always need it for is_in_library.
        with self.loading_lock:
            if library_id in self.loading_libraries:
                return
            self.loading_libraries.add(library_id)
        
        self.event_executor.submit(self._background_cache_fill, library_id)

    def _background_cache_fill(self, library_id):
        try:
            self.cache_library_files(library_id)
        finally:
            with self.loading_lock:
                self.loading_libraries.discard(library_id)

    def is_in_library(self, file_path):
        """Check if a file exists in the media server."""
        server_type = self.config.get('SERVER_TYPE', 'plex')
        
        # Check cache if it exists
        library_id, library_title, _ = self.get_library_id_for_path(file_path)
        if library_id:
            norm_path = os.path.normpath(file_path)
            
            # Ensure cache is loaded
            with self.library_files_lock:
                cache_filled = library_id in self.library_files
            
            if not cache_filled:
                self._trigger_cache_fill(library_id)
                # Fallback to direct API check while cache warms up
                if server_type == 'plex':
                    return self._is_in_plex_api(file_path, library_id)
                elif server_type in ['jellyfin', 'emby']:
                    return self._is_in_jellyfin_api(file_path, library_id)

            with self.library_files_lock:
                if library_id in self.library_files and self.library_files[library_id] is not None:
                    return norm_path in self.library_files[library_id]

        # If cache check failed or library not found in cache, fallback to direct API check
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
            
            # Use libtype based on section type for more accurate search
            if section.type == 'show':
                libtype = 'episode'
            elif section.type == 'artist':
                libtype = 'track'
            else:
                libtype = 'movie'
            
            # Search by filename in title field as a fallback
            filename = os.path.basename(file_path)
            results = section.search(title=filename, libtype=libtype)
            
            norm_target = os.path.normpath(file_path)
            for item in results:
                if hasattr(item, 'media'):
                    for media in item.media:
                        for part in media.parts:
                            if os.path.normpath(part.file) == norm_target:
                                return True
            return False
        except Exception as e:
            logger.debug(f"Direct Plex check failed for {file_path}: {e}")
            return False

    def _is_in_jellyfin_api(self, file_path, library_id=None):
        """Check if file exists in Jellyfin/Emby via API search."""
        if not library_id:
            library_id, _, _ = self.get_library_id_for_path(file_path)
        if not library_id: return False
        
        url = f"{self.config['SERVER_URL']}/Items?ParentId={library_id}&Recursive=true&Fields=Path&IncludeItemTypes=Movie,Episode,Audio,MusicVideo"
        headers = {"X-Emby-Token": self.config['API_KEY']}
        try:
            # We don't want to fetch all items if we are just checking one
            # Jellyfin supports Path filter in some versions or via Search
            # For simplicity, if we don't have cache, we might have to use a targeted query
            # Search by term (filename) is often faster than fetching all
            filename = os.path.basename(file_path)
            search_url = f"{self.config['SERVER_URL']}/Items?ParentId={library_id}&Recursive=true&Fields=Path&IncludeItemTypes=Movie,Episode,Audio,MusicVideo&searchTerm={quote(filename)}"
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
        """Cache Jellyfin/Emby library using pagination to save memory."""
        try:
            new_files = set()
            batch_size = 5000
            start_index = 0
            
            while True:
                # Fetch items in batches using StartIndex and Limit
                url = f"{self.config['SERVER_URL']}/Items?ParentId={library_id}&Recursive=true&Fields=Path&IncludeItemTypes=Movie,Episode,Audio,MusicVideo,MusicAlbum&StartIndex={start_index}&Limit={batch_size}"
                headers = {"X-Emby-Token": self.config['API_KEY']}
                res = requests.get(url, headers=headers)
                res.raise_for_status()
                data = res.json()
                items = data.get('Items', [])
                
                if not items:
                    break
                    
                for item in items:
                    if 'Path' in item:
                        new_files.add(item['Path'])
                
                batch_count = len(items)
                total_count = data.get('TotalRecordCount', 0)
                start_index += batch_count
                
                # Clear large objects to free memory
                del data
                del items
                gc.collect()

                if batch_count < batch_size or start_index >= total_count:
                    break
            
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
        parts = [p for p in rel_path.split(os.sep) if p and p != '.']
        
        if not parts:
            return best_location
            
        # The first part after the library root is the Entity (Show or Movie)
        return os.path.join(best_location, parts[0])

    def is_library_root(self, library_id, folder_path):
        """Check if the given folder path is a root location for the library."""
        for section in self.library_sections_cache:
            if str(section['id']) == str(library_id):
                for location in section['locations']:
                    if os.path.normpath(folder_path) == os.path.normpath(location):
                        return True
        return False

    def is_entity_root(self, folder_path):
        """Check if the given folder is a top-level entity (Show or Movie folder) in its library."""
        library_id, _, _ = self.get_library_id_for_path(folder_path)
        if not library_id:
            return False
            
        entity_root = self.get_entity_root(folder_path)
        return os.path.normpath(folder_path) == os.path.normpath(entity_root)

    def should_scan_directory(self, dir_path):
        """Check if a directory or any of its subdirectories belong to a library."""
        normalized_dir = os.path.normpath(dir_path)
        
        # 1. Is the directory itself in a library (or a subdirectory of one)?
        lib_id, _, _ = self.get_library_id_for_path(normalized_dir)
        if lib_id:
            return True
            
        # 2. Is any library location a subdirectory of this directory?
        for section in self.library_sections_cache:
            for location in section['locations']:
                normalized_location = os.path.normpath(location)
                if normalized_location.startswith(normalized_dir + os.sep):
                    return True
                    
        return False

    def trigger_scan(self, library_id, folder_path, force=False):
        """Enqueue a library scan for a specific folder."""
        if force:
            self._do_trigger_scan(library_id, folder_path)
            return

        with self.pending_scans_lock:
            # Check for parent/child redundancies
            keys_to_remove = []
            for (pid, ppath) in list(self.pending_scans.keys()):
                if pid == library_id:
                    # Case 1: A parent/ancestor of the new folder is already pending scan.
                    # The new scan is redundant, so we skip it.
                    if folder_path.startswith(ppath + os.sep) or ppath == folder_path:
                        logger.debug(f"⏳ Skipping specific scan for {folder_path} as broad parent {ppath} is already pending")
                        return

                    # Case 2: The new folder is a parent/ancestor of an already pending scan.
                    # The pending scan is redundant, so we remove it.
                    if ppath.startswith(folder_path + os.sep):
                        logger.debug(f"⏳ Removing specific pending scan {ppath} in favor of broad parent scan {folder_path}")
                        keys_to_remove.append((pid, ppath))

            for k in keys_to_remove:
                del self.pending_scans[k]

            is_new = (library_id, folder_path) not in self.pending_scans
            # Update the last event time for this (library, folder)
            self.pending_scans[(library_id, folder_path)] = time.time()
            if is_new:
                logger.info(f"⏳ Scan queued (debouncing): {BOLD}{folder_path}{RESET}")

    def _process_scan_queue(self):
        """Background worker to process debounced scans and notifications."""
        last_gc = time.time()
        while True:
            try:
                time.sleep(1)
                
                # Periodic memory cleanup
                if time.time() - last_gc > 300: # Every 5 minutes
                    gc.collect()
                    last_gc = time.time()

                to_trigger = []
                ready_notifications = []
                
                with self.pending_scans_lock:
                    PENDING_SCANS.set(len(self.pending_scans))
                    now = time.time()
                    debounce_delay = self.config.get('SCAN_DEBOUNCE', 10)
                    
                    # 1. Process Scans that are ready
                    for key, last_time in list(self.pending_scans.items()):
                        if now - last_time >= debounce_delay:
                            library_id, folder_path = key
                            to_trigger.append((library_id, folder_path))
                            del self.pending_scans[key]

                    # 2. Process Notifications that are ready
                    # We send notifications after the same debounce delay as scans
                    # to ensure they are grouped similarly.
                    for notif_path, notif_data in list(self.pending_notifications.items()):
                        # We use the time the last file was added to this notification group
                        # (Stored implicitly by the fact that it's in pending_notifications)
                        # Actually, we should track 'last_updated' for notifications too if we want perfect debouncing.
                        # For now, let's trigger them if their associated folder is NOT in pending_scans
                        # OR if enough time has passed.
                        
                        is_still_scoping = False
                        for (pid, ppath) in self.pending_scans:
                            if notif_path == ppath or notif_path.startswith(ppath + os.sep):
                                is_still_scoping = True
                                break
                        
                        if not is_still_scoping:
                            # If no scan is pending for this folder, it's ready to notify
                            ready_notifications.append((notif_path, notif_data))
                            del self.pending_notifications[notif_path]
                            
                            # Clear pending files for these notifications
                            with self.pending_files_lock:
                                for f in notif_data['added'] + notif_data['deleted']:
                                    self.pending_files.discard(os.path.normpath(f))
                
                # Send a single grouped notification for all ready folders
                if ready_notifications:
                    logger.info(f"🔔 Sending notifications for {len(ready_notifications)} folders")
                    self._send_multi_grouped_notification(ready_notifications)

                for library_id, folder_path in to_trigger:
                    # Submit to monitor executor so we don't block the queue loop
                    self.scan_monitor_executor.submit(self._do_trigger_scan, library_id, folder_path)
            except Exception as e:
                logger.error(f"Error in scan queue worker: {e}")
                time.sleep(5)

    def _send_multi_grouped_notification(self, notifications):
        """Send a single Discord notification for multiple entities/folders."""
        if not notifications:
            return

        # If only one folder, use the standard grouped notification logic
        if len(notifications) == 1:
            root, data = notifications[0]
            self._send_grouped_notification(root, data)
            return

        total_added = sum(len(d['added']) for _, d in notifications)
        total_deleted = sum(len(d['deleted']) for _, d in notifications)
        
        color = Color.blue()
        if total_added and total_deleted: color = Color.gold()
        elif total_added: color = Color.green()
        elif total_deleted: color = Color.red()

        embed = Embed(
            title=f"📂 Bulk Update: {len(notifications)} folders",
            description=f"Detected **{total_added}** additions and **{total_deleted}** deletions across multiple folders.",
            color=color,
            timestamp=datetime.now()
        )

        # Group by folder for fields
        for root, data in notifications[:20]: # Limit to 20 folders to stay under Discord's 25 field limit
            added = data['added']
            deleted = data['deleted']
            entity_name = os.path.basename(root)
            
            if entity_name.lower().startswith("season ") or entity_name.lower() in ["specials", "extras"]:
                parent_name = os.path.basename(os.path.dirname(root))
                if parent_name: entity_name = f"{parent_name} - {entity_name}"

            msg = ""
            if added: 
                msg += f"✅ +{len(added)}\n"
                # Try to add a direct link for the first added item if possible
                if len(added) == 1 and self.plex:
                    try:
                        # Best effort link generation
                        # We need to find the item in Plex first.
                        # Since we just added it, it might be in the cache or readable via API.
                        # We use the path to find the key.
                        fpath = added[0]
                        lid, _, _ = self.get_library_id_for_path(fpath)
                        if lid:
                            # Search by file path to get the key
                            # This is a bit expensive so we only do it for single item adds to be safe?
                            # Or we can construct a search URL.
                            # A direct link to the library filter is safer and faster.
                            
                            # Construct a deep link to the library filtered by folder
                            # This works even if the specific item ID isn't known yet
                            # URL format: https://app.plex.tv/desktop/#!/server/{machineIdentifier}/details?key=%2Flibrary%2Fsections%2F{lid}%2Ffolder%3Fparent%3D{quote(root)}
                            # Actually, linking to the folder view is more reliable for "added" events
                            
                            machine_id = self.plex.machineIdentifier
                            # Plex Web URL usually needs to know the specific server UUID
                            # We can try to generate a local link or app.plex.tv link
                            
                            # Let's link to the folder in Plex Web
                            # /library/sections/{id}/folder?parent={path}
                            encoded_root = quote(root)
                            link = f"https://app.plex.tv/desktop/#!/server/{machine_id}/details?key=%2Flibrary%2Fsections%2F{lid}%2Ffolder%3Fparent%3D{encoded_root}"
                            msg += f"[View in Plex]({link})\n"
                    except Exception:
                        pass

            if deleted: msg += f"🗑️ -{len(deleted)}\n"
            
            embed.add_field(name=f"📁 {entity_name}", value=msg or "No changes", inline=True)

        if len(notifications) > 20:
            embed.add_field(name="...", value=f"and {len(notifications) - 20} more folders", inline=False)

        embed.set_footer(text="Omniscan Media Monitor")
        self._send_discord_embed(embed)

    def _send_grouped_notification(self, entity_root, data):
        """Send a single Discord notification for multiple file events."""
        added = data['added']
        deleted = data['deleted']
        library = data['library_title'] or "Unknown Library"
        entity_name = os.path.basename(entity_root)
        
        # Check if entity_name is "Season X" or "Specials" and prepend parent folder name
        if entity_name.lower().startswith("season ") or entity_name.lower() in ["specials", "extras"]:
            parent_name = os.path.basename(os.path.dirname(entity_root))
            if parent_name:
                entity_name = f"{parent_name} - {entity_name}"

        # Determine Color
        color = Color.blue()
        if added and deleted:
            color = Color.gold() # Mixed changes
        elif added:
            color = Color.green()
        elif deleted:
            color = Color.red()

        desc = f"Changes detected in **{entity_name}**"
        
        # Add Plex Link if available
        if self.plex and (added or deleted):
            try:
                lid, _, _ = self.get_library_id_for_path(entity_root)
                if lid:
                    machine_id = self.plex.machineIdentifier
                    encoded_root = quote(entity_root)
                    link = f"https://app.plex.tv/desktop/#!/server/{machine_id}/details?key=%2Flibrary%2Fsections%2F{lid}%2Ffolder%3Fparent%3D{encoded_root}"
                    desc += f"\n[View in Plex]({link})"
            except: pass

        embed = Embed(
            title=f"📂 Update: {library}",
            description=desc,
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
        
        try:
            if server_type == 'plex':
                self._trigger_plex_scan(library_id, folder_path)
            elif server_type in ['jellyfin', 'emby']:
                self._trigger_jellyfin_emby_scan(library_id, folder_path)
        finally:
            # Clear cache for this library so it's re-indexed on next check
            # This is critical to ensure that even if the scan took time or had minor issues,
            # we don't rely on stale cache data.
            with self.library_files_lock:
                lib_id_str = str(library_id)
                lib_id_int = None
                try: lib_id_int = int(library_id)
                except: pass

                if lib_id_str in self.library_files:
                    logger.debug(f"🧹 Invalidating cache (str) for library {lib_id_str} after scan")
                    del self.library_files[lib_id_str]
                
                if lib_id_int is not None and lib_id_int in self.library_files:
                    logger.debug(f"🧹 Invalidating cache (int) for library {lib_id_int} after scan")
                    del self.library_files[lib_id_int]

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
            response = self.http_session.post(url, json=payload, headers=headers)
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
            response = self.http_session.get(url)
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

    def check_file_integrity(self, file_path):
        """Check if file is valid (not 0-byte, and optionally passes ffprobe)."""
        if not self.config.get('INTEGRITY_CHECK'):
            return True, None

        try:
            if not os.path.exists(file_path):
                return False, "file not found"
            
            # 1. 0-byte check
            size = os.path.getsize(file_path)
            if size == 0:
                return False, "0-byte file"
        except Exception as e:
            return False, f"error reading size: {e}"

        # 2. ffprobe check
        if self.config.get('FFPROBE_CHECK'):
            import subprocess
            try:
                cmd = ["ffprobe", "-v", "error", "-show_format", "-show_streams", file_path]
                res = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=5)
                if res.returncode != 0:
                    err_msg = res.stderr.strip() if res.stderr else f"exit code {res.returncode}"
                    return False, f"ffprobe error: {err_msg}"
            except subprocess.TimeoutExpired:
                return False, "ffprobe timeout"
            except FileNotFoundError:
                logger.warning("ffprobe command not found. Please ensure ffmpeg is installed.")
                return True, None
            except Exception as e:
                return False, f"ffprobe exception: {str(e)}"

        return True, None

    def submit_file_event(self, event_type, file_path):
        """Submit a file event for asynchronous processing."""
        if event_type == 'created' or event_type == 'moved':
            self.event_executor.submit(self.scan_file, file_path)
        elif event_type == 'deleted':
            self.event_executor.submit(self.handle_deletion, file_path)

    def scan_file(self, file_path, stats=None, tracker=None):
        """Scan a single file and trigger Plex refresh if missing."""
        # Fallback to global history if no specific tracker provided (e.g. for Watcher/Webhooks)
        if not tracker:
            tracker = self.history

        if self.is_ignored(file_path):
            return

        if self.config['SYMLINK_CHECK'] and self.is_broken_symlink(file_path):
            if stats: stats.increment_broken_symlinks()
            return

        file_name = os.path.basename(file_path)
        file_ext = os.path.splitext(file_name)[1].lower()
        if file_ext not in self.config['MEDIA_EXTENSIONS']:
            return

        # NEW: Early Library Check
        # Ensure the file actually belongs to a Plex/Jellyfin library path before proceeding.
        library_id, library_title, library_type = self.get_library_id_for_path(file_path)
        if not library_id:
            # Not in any library, skip entirely.
            return

        if stats: stats.increment_scanned()
        SCANNED_FILES_TOTAL.inc()

        # Only check library membership for video/audio files.
        # Subtitle sidecar files (.srt/.sub/.ass/.vtt) are not indexed by
        # Plex as individual items — skipping them prevents false stuck entries.
        if file_ext not in self.config['LIBRARY_EXTENSIONS']:
            return

        if not self.is_in_library(file_path):
            is_valid, reason = self.check_file_integrity(file_path)
            if not is_valid:
                logger.warning(f"❌ File failed integrity validation ({reason}): {file_path}")
                if tracker:
                    tracker.add_event("Corrupt", file_path, reason)
                if stats:
                    stats.add_corrupt_item(file_path, reason)
                return

            norm_path = os.path.normpath(file_path)
            with self.pending_files_lock:
                if norm_path in self.pending_files:
                    return
                self.pending_files.add(norm_path)

            logger.info(f"🆕 Found new file: {BOLD}{file_path}{RESET}")
            
            MISSING_FILES_TOTAL.inc()
            
            if library_title or self.config.get('SERVER_TYPE') != 'plex':
                should_scan = True
                if tracker:
                    if tracker.increment_attempt(file_path):
                        if stats:
                            stats.add_stuck_item(file_path)
                        should_scan = False
                    else:
                        if stats:
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
                        self.pending_notifications[target_path]['added'].append(file_path)
                    
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
             norm_p = os.path.normpath(path)
             norm_f = os.path.normpath(file_path)
             if norm_f == norm_p or norm_f.startswith(norm_p + os.sep):
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

        # NEW: Early Library Check
        library_id, library_title, library_type = self.get_library_id_for_path(file_path)
        if not library_id:
            return

        logger.info(f"🗑️ File deleted: {BOLD}{file_path}{RESET}")
        
        if library_id or self.config.get('SERVER_TYPE') != 'plex':
            norm_path = os.path.normpath(file_path)
            with self.pending_files_lock:
                if norm_path in self.pending_files:
                    return
                self.pending_files.add(norm_path)

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
                self.pending_notifications[target_path]['deleted'].append(file_path)

            self.trigger_scan(library_id, target_path)

    def scan_directory(self, path, stats, tracker, folders_to_scan, folders_to_scan_lock):
        # Pre-calculate cutoff time for incremental scan
        cutoff_time = 0
        if self.config.get('INCREMENTAL_SCAN'):
            cutoff_time = time.time() - (self.config['SCAN_SINCE_DAYS'] * 86400)

        for root, dirs, files in os.walk(path, followlinks=True):
            
            # Prune ignored directories in-place to avoid traversing them
            # This is a significant optimization for large ignored trees (e.g. .git, extras)
            dirs[:] = [d for d in dirs if not self.is_ignored(os.path.join(root, d)) and self.should_scan_directory(os.path.join(root, d))]
            
            if self.config.get('INCREMENTAL_SCAN'):
                try:
                    mtime = os.path.getmtime(root)
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

                # NEW: Early Library Check
                # Ensure the file actually belongs to a Plex/Jellyfin library path before proceeding.
                library_id, library_title, library_type = self.get_library_id_for_path(file_path)
                if not library_id:
                    # Not in any library, skip entirely.
                    continue

                if self.config['SYMLINK_CHECK'] and self.is_broken_symlink(file_path):
                    stats.increment_broken_symlinks()
                    continue

                stats.increment_scanned()
                SCANNED_FILES_TOTAL.inc()

                # Only check library membership for video/audio files.
                # Subtitle sidecar files are not Plex library items.
                if file_ext not in self.config['LIBRARY_EXTENSIONS']:
                    continue

                if not self.is_in_library(file_path):
                    is_valid, reason = self.check_file_integrity(file_path)
                    if not is_valid:
                        logger.warning(f"❌ File failed integrity validation ({reason}): {file_path}")
                        tracker.add_event("Corrupt", file_path, reason)
                        stats.add_corrupt_item(file_path, reason)
                        continue

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
                                    if not self.is_ignored(entry.path) and self.should_scan_directory(entry.path):
                                        futures.append(executor.submit(self.scan_directory, entry.path, stats, tracker, folders_to_scan, folders_to_scan_lock))
                                elif entry.is_file():
                                    file_path = entry.path
                                    if self.is_ignored(file_path): continue
                                    
                                    # NEW: Early Library Check
                                    library_id, library_title, library_type = self.get_library_id_for_path(file_path)
                                    if not library_id:
                                        continue

                                    if self.config['SYMLINK_CHECK'] and self.is_broken_symlink(file_path):
                                        stats.increment_broken_symlinks()
                                        continue
                                        
                                    file_name = os.path.basename(file_path)
                                    file_ext = os.path.splitext(file_name)[1].lower()
                                    if file_ext not in self.config['MEDIA_EXTENSIONS']: continue
                                    
                                    stats.increment_scanned()
                                    SCANNED_FILES_TOTAL.inc()

                                    # Only check library membership for video/audio files.
                                    # Subtitle sidecar files are not Plex library items.
                                    if file_ext not in self.config['LIBRARY_EXTENSIONS']:
                                        continue

                                    if not self.is_in_library(file_path):
                                        is_valid, reason = self.check_file_integrity(file_path)
                                        if not is_valid:
                                            logger.warning(f"❌ File failed integrity validation ({reason}): {file_path}")
                                            tracker.add_event("Corrupt", file_path, reason)
                                            stats.add_corrupt_item(file_path, reason)
                                            continue

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
                stats.send_discord_pending(len(folders_to_scan))
                
                sorted_folders = sorted(list(folders_to_scan), key=lambda x: x[1])
                for library_id, folder_path in sorted_folders:
                    self.trigger_scan(library_id, folder_path)

            tracker.save_history()
            stats.send_discord_summary()
            
        except Exception as e:
            logger.error(f"Error during scan: {e}")
        finally:
            self.is_scanning = False
            # Clear cache if NOT in watch mode.
            if not self.config.get('WATCH_MODE'):
                with self.library_files_lock:
                    self.library_files.clear()
            else:
                logger.info("🧠 Retaining library cache for active watcher")
            
            # Always trigger garbage collection to release memory from scan objects
            gc.collect()
        