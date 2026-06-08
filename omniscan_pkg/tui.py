import curses
import time
import requests
import argparse
import sys
import threading
import os
import configparser

# Global session for API requests
session = requests.Session()
api_url = "http://localhost:8000"
username = "admin"
password = "admin"

stats_data = {}
log_lines = []
lock = threading.Lock()
running = True

def load_local_config():
    """Attempt to load credentials and API port from local config.ini."""
    global api_url, username, password
    config = configparser.ConfigParser()
    if os.path.exists('config.ini'):
        try:
            config.read('config.ini')
            if config.has_section('web'):
                username = config.get('web', 'username', fallback=username)
                password = config.get('web', 'password', fallback=password)
        except Exception:
            pass

def authenticate():
    """Authenticate with the Omniscan Web API."""
    try:
        # Check if setup is completed first
        status_res = session.get(f"{api_url}/", timeout=3)
        if "/setup" in status_res.url:
            return False, "SETUP_REQUIRED"

        login_url = f"{api_url}/login"
        res = session.post(login_url, data={"username": username, "password": password}, timeout=3)
        if res.status_code == 200 and "login" not in res.url:
            return True, "Authenticated"
        return False, "Invalid username or password"
    except Exception as e:
        return False, f"Could not connect to API: {e}"

def api_worker():
    """Background worker to poll stats and logs from the API."""
    global stats_data, log_lines, running
    while running:
        try:
            # 1. Fetch Stats
            res = session.get(f"{api_url}/api/stats", timeout=2)
            if res.status_code == 200:
                with lock:
                    stats_data = res.json()
            
            # 2. Fetch Logs
            log_res = session.get(f"{api_url}/api/logs", timeout=2)
            if log_res.status_code == 200:
                with lock:
                    log_lines = log_res.json()
        except Exception:
            pass
        time.sleep(2)

def draw_tui(stdscr):
    global stats_data, log_lines, running
    
    # Hide cursor
    curses.curs_set(0)
    # Don't block on input
    stdscr.nodelay(True)
    # Enable keypad keys
    stdscr.keypad(True)
    
    # Define color pairs
    curses.start_color()
    curses.use_default_colors()
    curses.init_pair(1, curses.COLOR_CYAN, -1)      # Accent
    curses.init_pair(2, curses.COLOR_GREEN, -1)     # Success
    curses.init_pair(3, curses.COLOR_YELLOW, -1)    # Warning
    curses.init_pair(4, curses.COLOR_RED, -1)       # Error / Alert
    curses.init_pair(5, curses.COLOR_WHITE, -1)     # White text
    curses.init_pair(6, curses.COLOR_MAGENTA, -1)   # Purple/Magenta
    
    while running:
        stdscr.erase()
        h, w = stdscr.getmaxyx()
        
        if h < 24 or w < 80:
            stdscr.addstr(0, 0, "Terminal window is too small. Please resize to 80x24 minimum.", curses.color_pair(4))
            stdscr.refresh()
            time.sleep(0.5)
            continue
        
        with lock:
            data = stats_data.copy()
            logs = list(log_lines)
            
        # Draw Header
        stdscr.attron(curses.A_BOLD | curses.color_pair(1))
        stdscr.addstr(0, 2, " OMNISCAN TUI DASHBOARD ")
        stdscr.attroff(curses.A_BOLD | curses.color_pair(1))
        
        uptime_str = data.get('uptime', '00:00:00')
        stdscr.addstr(0, w - len(uptime_str) - 12, f"Uptime: {uptime_str}", curses.color_pair(5))
        stdscr.addstr(1, 0, "━" * w, curses.color_pair(5))
        
        # --- LEFT PANEL: System Health & Libraries ---
        mid_col = w // 2
        
        # Section: System Info
        stdscr.addstr(3, 2, "┌─ System Stats ────────────────────────┐", curses.color_pair(1))
        watch_count = data.get('watching_count', '--')
        queue_len = len(data.get('pending', []))
        corrupt_len = data.get('corrupt_count', 0)
        
        stdscr.addstr(4, 2, "│  Watching Paths: ", curses.color_pair(5))
        stdscr.addstr(f"{watch_count}".ljust(20), curses.color_pair(1) | curses.A_BOLD)
        stdscr.addstr("│")
        
        stdscr.addstr(5, 2, "│  Active Queue:   ", curses.color_pair(5))
        stdscr.addstr(f"{queue_len}".ljust(20), curses.color_pair(2) if queue_len == 0 else curses.color_pair(3))
        stdscr.addstr("│")
        
        stdscr.addstr(6, 2, "│  Corrupt Files:  ", curses.color_pair(5))
        stdscr.addstr(f"{corrupt_len}".ljust(20), curses.color_pair(2) if corrupt_len == 0 else curses.color_pair(4) | curses.A_BOLD)
        stdscr.addstr("│")
        
        stdscr.addstr(7, 2, "└───────────────────────────────────────┘", curses.color_pair(1))
        
        # Section: Storage Disks
        stdscr.addstr(9, 2, "┌─ Storage Status ──────────────────────┐", curses.color_pair(1))
        storage_list = data.get('storage', [])
        idx = 10
        if not storage_list:
            stdscr.addstr(idx, 2, "│  No disk storage stats available.    │", curses.color_pair(3))
            idx += 1
        else:
            for disk in storage_list[:3]:  # Max 3
                path = disk.get('path', '/')
                free = disk.get('free', '0B')
                pct = disk.get('percent', 0)
                # Render mini bar
                bar_len = 12
                filled = int((pct / 100) * bar_len)
                bar = "█" * filled + "░" * (bar_len - filled)
                stdscr.addstr(idx, 2, f"│  {path[:8].ljust(8)} [{bar}] {pct}% ({free} free)│", curses.color_pair(5))
                idx += 1
        stdscr.addstr(idx, 2, "└───────────────────────────────────────┘", curses.color_pair(1))
        
        # Section: Library distribution list
        lib_idx = idx + 2
        stdscr.addstr(lib_idx, 2, "┌─ Media Libraries ─────────────────────┐", curses.color_pair(1))
        lib_idx += 1
        libraries = data.get('libraries', [])
        if not libraries:
            stdscr.addstr(lib_idx, 2, "│  No library folders detected.         │", curses.color_pair(3))
            lib_idx += 1
        else:
            for lib in libraries[:5]:  # Max 5
                title = lib.get('title', 'Unknown')
                count = lib.get('count', 0)
                ltype = lib.get('type', 'movie')
                stdscr.addstr(lib_idx, 2, f"│  {title[:14].ljust(14)} ({ltype[:5].lower()}) : {count:,} files│", curses.color_pair(5))
                lib_idx += 1
        stdscr.addstr(lib_idx, 2, "└───────────────────────────────────────┘", curses.color_pair(1))
        
        # --- RIGHT PANEL: Scan Queue & Console Logs ---
        # Section: Active Queue List
        stdscr.addstr(3, mid_col, "┌─ Active Queue List ───────────────────┐", curses.color_pair(1))
        queue_items = data.get('pending', [])
        q_idx = 4
        if not queue_items:
            stdscr.addstr(q_idx, mid_col, "│  Queue is currently empty.            │", curses.color_pair(2))
            q_idx += 1
        else:
            for item in queue_items[:4]:  # Max 4
                path = item.get('path', '')
                rem = item.get('remaining', 0)
                stdscr.addstr(q_idx, mid_col, f"│  {os.path.basename(path)[:28].ljust(28)} ({rem}s)│", curses.color_pair(5))
                q_idx += 1
        stdscr.addstr(q_idx, mid_col, "└───────────────────────────────────────┘", curses.color_pair(1))
        
        # Section: Console Log Console
        log_start = q_idx + 1
        stdscr.addstr(log_start, mid_col, "┌─ System Log Output Console ───────────┐", curses.color_pair(1))
        
        log_area_h = h - log_start - 5
        visible_logs = logs[-log_area_h:] if len(logs) > log_area_h else logs
        
        for l_offset, log_line in enumerate(visible_logs):
            # Strip date formatting for readability if it exists
            clean_line = log_line
            if " | " in log_line:
                clean_line = log_line.split(" | ", 1)[1]
            
            # Determine color based on logging level
            color = curses.color_pair(5)
            if "ERROR" in log_line:
                color = curses.color_pair(4) | curses.A_BOLD
            elif "WARNING" in log_line:
                color = curses.color_pair(3)
            elif "INFO" in log_line:
                color = curses.color_pair(1)
            elif "DEBUG" in log_line:
                color = curses.color_pair(6)
                
            stdscr.addstr(log_start + 1 + l_offset, mid_col, "│ ", curses.color_pair(1))
            stdscr.addstr(clean_line[:36].ljust(36), color)
            stdscr.addstr(" │", curses.color_pair(1))
            
        for empty_row in range(len(visible_logs), log_area_h):
            stdscr.addstr(log_start + 1 + empty_row, mid_col, "│                                       │", curses.color_pair(1))
            
        stdscr.addstr(log_start + 1 + log_area_h, mid_col, "└───────────────────────────────────────┘", curses.color_pair(1))
        
        # --- BOTTOM BAR: Keyboard Shortcuts ---
        stdscr.addstr(h - 2, 0, "━" * w, curses.color_pair(5))
        stdscr.addstr(h - 1, 2, "[F] Run Full Scan", curses.color_pair(1) | curses.A_BOLD)
        stdscr.addstr("   ")
        stdscr.addstr("[R] Refresh Health Check", curses.color_pair(1) | curses.A_BOLD)
        stdscr.addstr("   ")
        stdscr.addstr("[Q] Quit Dashboard TUI", curses.color_pair(4) | curses.A_BOLD)
        
        stdscr.refresh()
        
        # Handle Keystrokes
        try:
            key = stdscr.getch()
            if key in [ord('q'), ord('Q')]:
                running = False
            elif key in [ord('f'), ord('F')]:
                session.post(f"{api_url}/api/scan-all", timeout=2)
            elif key in [ord('r'), ord('R')]:
                # Trigger server check or refresh connection
                session.post(f"{api_url}/api/check-connection", timeout=2)
        except Exception:
            pass
            
        time.sleep(0.1)

import getpass

def run_setup_wizard():
    print("\n==================================================")
    print("         OMNISCAN TERMINAL SETUP WIZARD")
    print("==================================================")
    print("Welcome to Omniscan! Please complete the initial configuration.\n")

    user = input("Web Admin Username [admin]: ").strip() or "admin"
    
    pwd = ""
    while not pwd:
        pwd = getpass.getpass("Web Admin Password: ").strip()
        if not pwd:
            print("Password cannot be empty!")

    server_type = ""
    while server_type not in ["plex", "jellyfin", "emby"]:
        server_type = input("Media Server Type (plex/jellyfin/emby) [plex]: ").strip().lower() or "plex"

    plex_server = ""
    plex_token = ""
    server_url = ""
    api_key = ""

    if server_type == "plex":
        plex_server = input("Plex Server URL [http://localhost:32400]: ").strip() or "http://localhost:32400"
        plex_token = input("Plex Token: ").strip()
    else:
        server_url = input(f"{server_type.capitalize()} Server URL [http://localhost:8096]: ").strip() or "http://localhost:8096"
        api_key = input(f"{server_type.capitalize()} API Key: ").strip()

    scan_directories = input("Media Scan Paths (comma-separated, e.g. /media/Movies, /media/TV Shows): ").strip()

    payload = {
        "username": user,
        "password": pwd,
        "server_type": server_type,
        "plex_server": plex_server,
        "plex_token": plex_token,
        "server_url": server_url,
        "api_key": api_key,
        "scan_directories": scan_directories
    }

    print("\nSaving configuration...")
    try:
        res = session.post(f"{api_url}/api/setup", json=payload, timeout=5)
        if res.status_code == 200:
            print("Configuration saved successfully!")
            # Update global credentials for subsequent authentication
            global username, password
            username = user
            password = pwd
        else:
            err = res.json().get("error", "Unknown error occurred")
            print(f"Configuration Failed: {err}")
            sys.exit(1)
    except Exception as e:
        print(f"Failed to communicate with setup endpoint: {e}")
        sys.exit(1)

def main():
    global api_url, username, password
    parser = argparse.ArgumentParser(description="Omniscan Command-Line TUI Console")
    parser.add_argument('--url', type=str, help="API server URL (e.g. http://localhost:8085)")
    parser.add_argument('--username', type=str, help="API login username")
    parser.add_argument('--password', type=str, help="API login password")
    args, _ = parser.parse_known_args()

    load_local_config()

    if args.url:
        api_url = args.url
    if args.username:
        username = args.username
    if args.password:
        password = args.password

    # Test link/auth first
    print("Connecting to Omniscan API...")
    success, msg = authenticate()
    if not success:
        if msg == "SETUP_REQUIRED":
            run_setup_wizard()
            success, msg = authenticate()
            if not success:
                print(f"Connection Failed after setup: {msg}")
                sys.exit(1)
        else:
            print(f"Connection Failed: {msg}")
            sys.exit(1)

    print("Success! Initializing interface...")
    
    # Spawn stats/log fetcher thread
    worker = threading.Thread(target=api_worker, daemon=True)
    worker.start()
    
    try:
        curses.wrapper(draw_tui)
    except KeyboardInterrupt:
        pass
    finally:
        global running
        running = False
        print("TUI closed.")

if __name__ == "__main__":
    main()
