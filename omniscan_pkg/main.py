import os
import time
import logging
import logging.handlers
import schedule
import argparse
import sys
from .config import load_config
from .scanner import PlexScanner
from .watcher import start_watcher
from .models import StuckFileTracker
from .web import run_web_server
import threading
import secrets
import configparser

# ANSI escape codes
BOLD = '\033[1m'
RESET = '\033[0m'

logger = logging.getLogger(__name__)

def parse_args():
    parser = argparse.ArgumentParser(description="Omniscan - Media Library Health & Sync")
    parser.add_argument('--watch', action='store_true', help="Enable real-time file monitoring")
    parser.add_argument('--scan-now', type=str, help="Scan a specific file or directory immediately and exit")
    parser.add_argument('--dry-run', action='store_true', help="Enable dry run mode (no Plex triggers)")
    parser.add_argument('--list-stuck', action='store_true', help="List all files marked as stuck")
    parser.add_argument('--clear-stuck', action='store_true', help="Clear all stuck files from history")
    return parser.parse_args()

def setup_logging(config):
    # Determine log file path (in config dir)
    log_file = os.path.join(os.getcwd(), 'omniscan.log')
    
    handlers = [logging.StreamHandler(sys.stdout)]
    
    # Add file handler
    file_handler = logging.handlers.RotatingFileHandler(
        log_file, maxBytes=5*1024*1024, backupCount=5, encoding='utf-8'
    )
    file_handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s', datefmt='%d %b %Y | %I:%M:%S %p'))
    handlers.append(file_handler)

    logging.basicConfig(
        level=getattr(logging, config['LOG_LEVEL'].upper()),
        format='%(asctime)s [%(levelname)s] %(message)s',
        datefmt='%d %b %Y | %I:%M:%S %p',
        handlers=handlers,
        force=True 
    )

def main():
    args = parse_args()
    
    # Load config
    config = load_config('config.ini')
    
    # CLI overrides
    if args.dry_run:
        config['DRY_RUN'] = True
    
    # Setup Logging
    setup_logging(config)
    
    logger.info("Starting Omniscan")
    
    if not config.get('WEB_PASSWORD'):
        generated_pwd = secrets.token_urlsafe(16)
        config['WEB_PASSWORD'] = generated_pwd
        logger.warning(f"{BOLD}⚠️  NO WEB PASSWORD SET ⚠️{RESET}")
        logger.warning(f"Generated temporary password: {BOLD}{generated_pwd}{RESET}")
        logger.warning(f"User: {config.get('WEB_USERNAME', 'admin')}")
        logger.warning(f"Saving generated password to config.ini...")
        
        try:
            cfg_parser = configparser.ConfigParser()
            cfg_parser.read('config.ini')
            if not cfg_parser.has_section('web'):
                cfg_parser.add_section('web')
            cfg_parser.set('web', 'username', config.get('WEB_USERNAME', 'admin'))
            cfg_parser.set('web', 'password', generated_pwd)
            with open('config.ini', 'w') as f:
                cfg_parser.write(f)
            logger.info("✅ Password saved to config.ini")
        except Exception as e:
            logger.error(f"Failed to save password to config.ini: {e}")

    if config.get('DRY_RUN'):
        logger.info(f"{BOLD}⚠️ DRY RUN MODE ENABLED - No changes will be made ⚠️{RESET}")

    # Stuck File Management
    if args.list_stuck:
        tracker = StuckFileTracker()
        stuck_files = tracker.get_all_stuck()
        if not stuck_files:
            print("No stuck files found.")
        else:
            print(f"Found {len(stuck_files)} stuck files:")
            for path, attempts, last_seen in stuck_files:
                print(f"[{attempts} attempts] {last_seen}: {path}")
        sys.exit(0)

    if args.clear_stuck:
        tracker = StuckFileTracker()
        if tracker.clear_all_stuck():
            print("✅ Cleared all stuck files from history.")
        else:
            print("❌ Failed to clear stuck files.")
        sys.exit(0)

    scanner = PlexScanner(config)
    
    # Start Web UI in background
    web_thread = threading.Thread(target=run_web_server, args=(scanner,), daemon=True)
    web_thread.start()
    logger.info(f"🕸️ Web UI started on http://0.0.0.0:8000")
    
    if args.scan_now:
        if not scanner.plex:
            scanner.connect_to_plex()
        scanner.get_library_ids()
        
        path = args.scan_now
        if os.path.isfile(path):
            logger.info(f"Targeted scan for file: {path}")
            scanner.scan_file(path)
        elif os.path.isdir(path):
            logger.info(f"Targeted scan for directory: {path}")
            from .models import RunStats
            
            stats = RunStats(config)
            tracker = StuckFileTracker()
            folders_to_scan = set()
            lock = threading.Lock()
            
            scanner.scan_directory(path, stats, tracker, folders_to_scan, lock)
            
            if stats.total_missing > 0:
                sorted_folders = sorted(list(folders_to_scan), key=lambda x: x[1])
                for library_id, folder_path in sorted_folders:
                     scanner.trigger_scan(library_id, folder_path)
            
            stats.send_discord_summary()
            
        else:
            logger.error(f"Path not found: {path}")
        sys.exit(0)

    if args.watch or config.get('WATCH_MODE'):
        logger.info("🚀 Starting Real-time Watcher...")
        if not scanner.plex:
            scanner.connect_to_plex()
        scanner.get_library_ids()

        if config['RUN_ON_STARTUP']:
            logger.info("Running startup scan before starting watcher...")
            # Run in a separate thread to not delay watcher start? 
            # Or just run it. Let's run it synchronously to ensure clean state.
            scanner.run_scan()

        start_watcher(scanner)
        return

    # Default: Scheduled Mode
    logger.info(f"Will run every {BOLD}{config['RUN_INTERVAL']}{RESET} hours")
    
    if config.get('RUN_ON_STARTUP'):
        scanner.run_scan()

    if config['START_TIME']:
        try:
            start_hour, start_minute = map(int, config['START_TIME'].split(':'))
            for i in range(0, 24, config['RUN_INTERVAL']):
                hour = (start_hour + i) % 24
                time_str = f"{hour:02d}:{start_minute:02d}"
                schedule.every().day.at(time_str).do(scanner.run_scan)
        except ValueError:
            schedule.every(config['RUN_INTERVAL']).hours.do(scanner.run_scan)
    else:
        schedule.every(config['RUN_INTERVAL']).hours.do(scanner.run_scan)
    
    # Graceful Shutdown Handling
    import signal
    stop_event = threading.Event()

    def signal_handler(signum, frame):
        logger.info(f"🛑 Received signal {signum}, stopping...")
        stop_event.set()
        # Attempt to stop watcher if running
        # (Watcher runs in main thread if enabled, but here we are in main thread too?)
        # Actually start_watcher blocks if watch mode is on.
        # But if we are in schedule mode, we are in the loop below.
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    while not stop_event.is_set():
        schedule.run_pending()
        time.sleep(1)
    
    logger.info("👋 Omniscan shutdown complete.")

if __name__ == '__main__':
    main()
