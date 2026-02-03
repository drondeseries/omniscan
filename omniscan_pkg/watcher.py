import time
import os
import logging
from watchdog.observers import Observer
from watchdog.observers.polling import PollingObserver
from watchdog.events import FileSystemEventHandler

logger = logging.getLogger(__name__)

class PlexWatcher(FileSystemEventHandler):
    def __init__(self, scanner):
        self.scanner = scanner

    def on_created(self, event):
        if not event.is_directory:
            self.scanner.submit_file_event('created', event.src_path)

    def on_moved(self, event):
        if not event.is_directory:
            self.scanner.submit_file_event('moved', event.dest_path)

    def on_deleted(self, event):
        if not event.is_directory:
            self.scanner.submit_file_event('deleted', event.src_path)

def start_watcher(scanner):
    """Start the watchdog observer."""
    use_polling = scanner.config.get('USE_POLLING', False)
    if use_polling:
        logger.info("Using PollingObserver (CPU intensive, but better for network mounts)")
        observer = PollingObserver()
    else:
        logger.info("Using Native Observer (Inotify)")
        observer = Observer()
        
    handler = PlexWatcher(scanner)
    
    paths_to_watch = scanner.config['SCAN_PATHS']
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
    
    # Setup signal handling for graceful stop if running as main blocker
    import signal
    import threading
    stop_event = threading.Event()
    
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
