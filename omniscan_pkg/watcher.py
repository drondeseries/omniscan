import time
import os
import logging
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

logger = logging.getLogger(__name__)

class PlexWatcher(FileSystemEventHandler):
    def __init__(self, scanner):
        self.scanner = scanner

    def on_created(self, event):
        if not event.is_directory:
            self.scanner.submit_file_event('created', event.src_path)
        else:
            logger.info(f"📁 Directory created: {event.src_path}")
            # Trigger scan for the new directory
            lid, _, _ = self.scanner.get_library_id_for_path(event.src_path)
            if lid:
                self.scanner.trigger_scan(lid, event.src_path)

    def on_moved(self, event):
        if not event.is_directory:
            self.scanner.submit_file_event('moved', event.dest_path)
        else:
            logger.info(f"📁 Directory moved/renamed: {event.src_path} -> {event.dest_path}")
            # Trigger scan for the destination directory
            lid, _, _ = self.scanner.get_library_id_for_path(event.dest_path)
            if lid:
                self.scanner.trigger_scan(lid, event.dest_path)

    def on_deleted(self, event):
        if not event.is_directory:
            self.scanner.submit_file_event('deleted', event.src_path)
        else:
            logger.info(f"📁 Directory deleted: {event.src_path}")
            # Trigger scan for parent directory
            parent = os.path.dirname(event.src_path)
            lid, _, _ = self.scanner.get_library_id_for_path(parent)
            
            # Mass Deletion Protection
            if lid and self.scanner.config.get('ABORT_ON_MASS_DELETION'):
                threshold = self.scanner.config.get('DELETION_THRESHOLD', 50)
                fc = self.scanner.library_files.get(lid, {})
                if isinstance(fc, dict):
                    norm_deleted = os.path.normpath(event.src_path)
                    count = sum(1 for p in fc if p.startswith(norm_deleted + os.sep) or p == norm_deleted)
                    if count > threshold:
                        logger.error(f"🛑 ABORTING SCAN: Directory '{event.src_path}' deleted containing {count} items (Threshold: {threshold}).")
                        return

            if lid:
                self.scanner.trigger_scan(lid, parent)

def start_watcher(scanner, stop_event=None):
    """Start the watchdog observer."""
    logger.info("Using Native Observer (Inotify)")
    observer = Observer()
        
    handler = PlexWatcher(scanner)
    
    paths_to_watch = scanner.config.get('WATCH_DIRECTORIES') or scanner.config['SCAN_PATHS']
    if not paths_to_watch:
        logger.warning("No paths configured to watch.")
        return

    for path in paths_to_watch:
        if os.path.isdir(path):
            logger.info(f"👀 Watching directory: {path}")
            observer.schedule(handler, path, recursive=True)
        else:
            logger.warning(f"Directory not found, cannot watch: {path}")

    observer.start()
    
    import threading
    if stop_event is None:
        stop_event = threading.Event()
    
    # Setup signal handling for graceful stop if running in the main thread
    if threading.current_thread() is threading.main_thread():
        import signal
        def signal_handler(signum, frame):
            logger.info("🛑 Watcher stopping...")
            stop_event.set()

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

    try:
        while not stop_event.is_set():
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        observer.stop()
        observer.join()
        logger.info("👋 Watcher stopped.")
