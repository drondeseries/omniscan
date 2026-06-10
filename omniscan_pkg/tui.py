import curses
import time
import requests
import argparse
import sys
import threading
import os
import configparser
import re

ansi_escape = re.compile(r'\x1b\[[0-9;]*[a-zA-Z]')

# Global session for API requests
session = requests.Session()
api_url = "http://localhost:8000"
username = "admin"
password = "admin"

stats_data = {}
log_lines = []
lock = threading.Lock()
running = True

# View tabs: 'dashboard' or 'settings'
current_tab = "dashboard"
settings_scroll_idx = 0
local_config = {}

# Order of settings shown on screen
SETTINGS_KEYS = [
    ("server_type", "Server Type (plex/jellyfin/emby)"),
    ("plex_server", "Plex Server URL"),
    ("plex_token", "Plex Token / Password"),
    ("server_url", "Jellyfin/Emby URL"),
    ("api_key", "Jellyfin/Emby API Key"),
    ("scan_directories", "Library Scan Paths"),
    ("scan_workers", "Thread Workers"),
    ("scan_debounce", "Debounce Delay (s)"),
    ("scan_delay", "Sync Scan Delay (s)"),
    ("run_interval", "Periodic Interval (h)"),
    ("scan_since_days", "Scan History Depth (days)"),
    ("run_on_startup", "Run Scan on Startup (true/false)"),
    ("incremental_scan", "Incremental Scan (true/false)"),
    ("symlink_check", "Validate Symlinks (true/false)"),
    ("integrity_check", "Zero-byte Check (true/false)"),
    ("ffprobe_check", "Media ffprobe Check (true/false)"),
    ("watch_mode", "Real-time Watcher (true/false)"),
    ("use_polling", "Use Polling Observer (true/false)"),
    ("discord_webhook_url", "Discord Webhook URL"),
    ("notifications_enabled", "Enable Notifications (true/false)"),
    ("ignore_patterns", "Ignore Patterns"),
    ("path_rewrites", "Path Mappings (Rewrites)")
]

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
    """Bypass authentication for TUI local use."""
    return True, "Authenticated"

def api_worker():
    """Background worker to poll stats and logs from the API."""
    global stats_data, log_lines, running, local_config
    while running:
        try:
            res = session.get(f"{api_url}/api/stats", timeout=2)
            if res.status_code == 200:
                with lock:
                    stats_data = res.json()
                    # Pre-load local config if we don't have it initialized yet
                    if not local_config and 'config' in stats_data:
                        local_config = dict(stats_data['config'])
            
            log_res = session.get(f"{api_url}/api/logs", timeout=2)
            if log_res.status_code == 200:
                with lock:
                    log_lines = log_res.json()
        except Exception:
            pass
        time.sleep(2)

def get_input(stdscr, prompt, default_value=""):
    """Render a clean TUI input field at the bottom line of curses window."""
    h, w = stdscr.getmaxyx()
    stdscr.move(h - 1, 0)
    stdscr.clrtoeol()
    stdscr.addstr(h - 1, 2, f"{prompt}: ", curses.color_pair(1) | curses.A_BOLD)
    
    start_col = len(prompt) + 4
    stdscr.addstr(h - 1, start_col, default_value, curses.color_pair(5))
    
    curses.curs_set(1)
    stdscr.nodelay(False)
    
    val = list(default_value)
    pos = len(val)
    
    while True:
        stdscr.move(h - 1, start_col + pos)
        stdscr.refresh()
        ch = stdscr.getch()
        
        if ch in [10, 13]:  # Enter
            break
        elif ch in [27]:  # Escape
            val = None
            break
        elif ch in [8, 127, curses.KEY_BACKSPACE]:  # Backspace
            if pos > 0:
                pos -= 1
                val.pop(pos)
                stdscr.move(h - 1, start_col)
                stdscr.clrtoeol()
                stdscr.addstr(h - 1, start_col, "".join(val), curses.color_pair(5))
        elif 32 <= ch <= 126:  # Printable characters
            if start_col + pos < w - 2:
                val.insert(pos, chr(ch))
                pos += 1
                stdscr.move(h - 1, start_col)
                stdscr.clrtoeol()
                stdscr.addstr(h - 1, start_col, "".join(val), curses.color_pair(5))
            
    curses.curs_set(0)
    stdscr.nodelay(True)
    return "".join(val) if val is not None else None

def draw_tui(stdscr):
    global stats_data, log_lines, running, current_tab, settings_scroll_idx, local_config
    
    curses.curs_set(0)
    stdscr.nodelay(True)
    stdscr.keypad(True)
    
    curses.start_color()
    curses.use_default_colors()
    curses.init_pair(1, curses.COLOR_CYAN, -1)      # Cyan Accents
    curses.init_pair(2, curses.COLOR_GREEN, -1)     # Green Success
    curses.init_pair(3, curses.COLOR_YELLOW, -1)    # Yellow Warning
    curses.init_pair(4, curses.COLOR_RED, -1)       # Red Alerts
    curses.init_pair(5, curses.COLOR_WHITE, -1)     # White standard text
    curses.init_pair(6, curses.COLOR_BLACK, curses.COLOR_CYAN) # Highlighted item (reversed Cyan)
    curses.init_pair(7, curses.COLOR_MAGENTA, -1)   # Purple details
    
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
            
        # ─── BRAND HEADER BANNER ───
        stdscr.attron(curses.A_BOLD | curses.color_pair(1))
        stdscr.addstr(1, 2, "╔═╗╔╦╗╔╗╔╦╔═╗╔═╗╔═╗╔╗╔")
        stdscr.addstr(2, 2, "║║║║║║║║║║╚═╗║  ╠═╣║║║  MEDIA SERVER MONITOR")
        stdscr.addstr(3, 2, "╚═╝╩ ╩╝╚╝╩╚═╝╚═╝╩ ╩╝╚╝")
        stdscr.attroff(curses.A_BOLD | curses.color_pair(1))
        
        uptime_str = data.get('uptime', '00:00:00')
        stdscr.addstr(2, w - len(uptime_str) - 15, f"Uptime: {uptime_str}", curses.color_pair(5))
        stdscr.addstr(4, 0, "━" * w, curses.color_pair(5))
        
        mid_col = w // 2
        
        # ─── TAB: DASHBOARD VIEW ───
        if current_tab == "dashboard":
            # Column 1: System Info
            stdscr.addstr(6, 2, "╔═════ SYSTEM STATS ═══════════════════╗", curses.color_pair(1))
            watch_count = data.get('watching_count', '--')
            queue_len = len(data.get('pending', []))
            corrupt_len = data.get('corrupt_count', 0)
            missing_len = data.get('total_missing', 0)
            
            stdscr.addstr(7, 2, "║ ", curses.color_pair(1))
            stdscr.addstr("Watching Directories: ".ljust(22), curses.color_pair(5))
            stdscr.addstr(f"{watch_count}".ljust(14), curses.color_pair(1) | curses.A_BOLD)
            stdscr.addstr("║", curses.color_pair(1))
            
            stdscr.addstr(8, 2, "║ ", curses.color_pair(1))
            stdscr.addstr("Queue Count:          ".ljust(22), curses.color_pair(5))
            stdscr.addstr(f"{queue_len}".ljust(14), curses.color_pair(2) if queue_len == 0 else curses.color_pair(3) | curses.A_BOLD)
            stdscr.addstr("║", curses.color_pair(1))
            
            stdscr.addstr(9, 2, "║ ", curses.color_pair(1))
            stdscr.addstr("Corrupt Files Found:  ".ljust(22), curses.color_pair(5))
            stdscr.addstr(f"{corrupt_len}".ljust(14), curses.color_pair(2) if corrupt_len == 0 else curses.color_pair(4) | curses.A_BOLD)
            stdscr.addstr("║", curses.color_pair(1))
            
            stdscr.addstr(10, 2, "║ ", curses.color_pair(1))
            stdscr.addstr("Missing from Server:  ".ljust(22), curses.color_pair(5))
            stdscr.addstr(f"{missing_len}".ljust(14), curses.color_pair(2) if missing_len == 0 else curses.color_pair(3) | curses.A_BOLD)
            stdscr.addstr("║", curses.color_pair(1))
            
            stdscr.addstr(10, 2, "║ ", curses.color_pair(1))
            stdscr.addstr("Missing from Server:  ".ljust(22), curses.color_pair(5))
            stdscr.addstr(f"{missing_len}".ljust(14), curses.color_pair(2) if missing_len == 0 else curses.color_pair(3) | curses.A_BOLD)
            stdscr.addstr("║", curses.color_pair(1))
            stdscr.addstr(11, 2, "╚══════════════════════════════════════╝", curses.color_pair(1))

            # Disk Storage info
            stdscr.addstr(13, 2, "╔═════ STORAGE DISK STATUS ════════════╗", curses.color_pair(1))
            storage_list = data.get('storage', [])
            idx = 14
            if not storage_list:
                stdscr.addstr(idx, 2, "║  No disk storage stats available.    ║", curses.color_pair(3))
                idx += 1
            else:
                for disk in storage_list[:2]:
                    path = disk.get('path', '/')
                    free = disk.get('free', '0B')
                    pct = disk.get('percent', 0)
                    
                    bar_len = 10
                    filled = int((pct / 100) * bar_len)
                    bar = "█" * filled + "░" * (bar_len - filled)
                    stdscr.addstr(idx, 2, "║ ", curses.color_pair(1))
                    stdscr.addstr(f"{path[:8].ljust(8)} [{bar}] {pct}% ({free} free)".ljust(36), curses.color_pair(5))
                    stdscr.addstr("║", curses.color_pair(1))
                    idx += 1
            stdscr.addstr(idx, 2, "╚══════════════════════════════════════╝", curses.color_pair(1))
            
            # Libraries list
            lib_idx = idx + 2
            stdscr.addstr(lib_idx, 2, "╔═════ MEDIA LIBRARIES ════════════════╗", curses.color_pair(1))
            lib_idx += 1
            libraries = data.get('libraries', [])
            if not libraries:
                stdscr.addstr(lib_idx, 2, "║  No library folders detected.         ║", curses.color_pair(3))
                lib_idx += 1
            else:
                for lib in libraries[:5]:
                    title = lib.get('title', 'Unknown')
                    count = lib.get('count', 0)
                    ltype = lib.get('type', 'movie')
                    stdscr.addstr(lib_idx, 2, "║ ", curses.color_pair(1))
                    stdscr.addstr(f"{title[:14].ljust(14)} ({ltype[:5].lower()}) : {count:,} files".ljust(36), curses.color_pair(5))
                    stdscr.addstr("║", curses.color_pair(1))
                    lib_idx += 1
            stdscr.addstr(lib_idx, 2, "╚══════════════════════════════════════╝", curses.color_pair(1))
            
            # Watched Folders list
            watch_idx = lib_idx + 2
            stdscr.addstr(watch_idx, 2, "╔═════ WATCHED FOLDERS ════════════════╗", curses.color_pair(1))
            watch_idx += 1
            watching_paths = data.get('watching_paths', [])
            if not watching_paths:
                stdscr.addstr(watch_idx, 2, "║  No folders watched.                  ║", curses.color_pair(3))
                watch_idx += 1
            else:
                for path in watching_paths:
                    content = f"📂 {path}"
                    stdscr.addstr(watch_idx, 2, "║ ", curses.color_pair(1))
                    stdscr.addstr(content[:36].ljust(36), curses.color_pair(5))
                    stdscr.addstr("║", curses.color_pair(1))
                    watch_idx += 1
            stdscr.addstr(watch_idx, 2, "╚══════════════════════════════════════╝", curses.color_pair(1))
            
            # Right Panel: Active Queue List
            stdscr.addstr(6, mid_col, "╔═════ ACTIVE SCANS QUEUE ═════════════╗", curses.color_pair(1))
            queue_items = data.get('pending', [])
            q_idx = 7
            if not queue_items:
                stdscr.addstr(q_idx, mid_col, "║  Queue is currently empty.            ║", curses.color_pair(2))
                q_idx += 1
            else:
                for item in queue_items[:4]:
                    path = item.get('path', '')
                    rem = item.get('remaining', 0)
                    stdscr.addstr(q_idx, mid_col, "║ ", curses.color_pair(1))
                    stdscr.addstr(f"{os.path.basename(path)[:26].ljust(26)} ({rem}s)".ljust(36), curses.color_pair(5))
                    stdscr.addstr("║", curses.color_pair(1))
                    q_idx += 1
            stdscr.addstr(q_idx, mid_col, "╚══════════════════════════════════════╝", curses.color_pair(1))
            
            # System Logs Console
            log_start = q_idx + 1
            stdscr.addstr(log_start, mid_col, "╔═════ LIVE CONSOLE LOGS ══════════════╗", curses.color_pair(1))
            
            log_area_h = h - log_start - 5
            visible_logs = logs[-log_area_h:] if len(logs) > log_area_h else logs
            
            for l_offset, log_line in enumerate(visible_logs):
                clean_line = log_line
                if " | " in log_line:
                    clean_line = log_line.split(" | ", 1)[1]
                clean_line = ansi_escape.sub('', clean_line)
                
                color = curses.color_pair(5)
                if "ERROR" in log_line:
                    color = curses.color_pair(4) | curses.A_BOLD
                elif "WARNING" in log_line:
                    color = curses.color_pair(3)
                elif "INFO" in log_line:
                    color = curses.color_pair(1)
                elif "DEBUG" in log_line:
                    color = curses.color_pair(7)
                    
                stdscr.addstr(log_start + 1 + l_offset, mid_col, "║ ", curses.color_pair(1))
                stdscr.addstr(clean_line[:36].ljust(36), color)
                stdscr.addstr(" ║", curses.color_pair(1))
                
            for empty_row in range(len(visible_logs), log_area_h):
                stdscr.addstr(log_start + 1 + empty_row, mid_col, "║                                      ║", curses.color_pair(1))
                
            stdscr.addstr(log_start + 1 + log_area_h, mid_col, "╚══════════════════════════════════════╝", curses.color_pair(1))
            
        # ─── TAB: CONFIGURATION VIEW ───
        elif current_tab == "settings":
            stdscr.addstr(6, 2, "╔═════ CONFIGURATION SETTINGS EDITOR ══════════════════════════════════╗", curses.color_pair(1))
            
            visible_count = h - 13
            offset = 0
            if settings_scroll_idx >= visible_count:
                offset = settings_scroll_idx - visible_count + 1
                
            row_idx = 7
            for i, (cfg_key, display_name) in enumerate(SETTINGS_KEYS[offset:offset+visible_count]):
                item_idx = offset + i
                raw_val = local_config.get(cfg_key, "")
                
                # Format mappings/arrays nicely for inline view
                if isinstance(raw_val, list):
                    val_str = ",".join(map(str, raw_val))
                else:
                    val_str = str(raw_val)
                
                # Truncate values to fit screen
                max_val_w = w - 44
                if len(val_str) > max_val_w:
                    val_str = val_str[:max_val_w-3] + "..."
                    
                line_str = f" {display_name.ljust(32)}: {val_str.ljust(max_val_w)}"
                
                stdscr.addstr(row_idx, 2, "║", curses.color_pair(1))
                if item_idx == settings_scroll_idx:
                    stdscr.addstr(row_idx, 3, line_str[:w-6].ljust(w-6), curses.color_pair(6) | curses.A_BOLD)
                else:
                    stdscr.addstr(row_idx, 3, line_str[:w-6].ljust(w-6), curses.color_pair(5))
                stdscr.addstr(row_idx, w-3, "║", curses.color_pair(1))
                row_idx += 1
                
            for empty_row in range(row_idx, h - 5):
                stdscr.addstr(empty_row, 2, "║" + " " * (w - 6) + "║", curses.color_pair(1))
                
            stdscr.addstr(h - 5, 2, "╚══════════════════════════════════════════════════════════════════════╝", curses.color_pair(1))
            stdscr.addstr(h - 4, 4, "[UP/DOWN] Navigate  [ENTER] Modify Setting  [S] Save  [ESC] Cancel", curses.color_pair(7) | curses.A_BOLD)
            
        # ─── FOOTER COMMAND ACTIONS BAR ───
        stdscr.addstr(h - 2, 0, "━" * w, curses.color_pair(5))
        
        # Interactive Shortcuts menu
        if current_tab == "dashboard":
            stdscr.addstr(h - 1, 2, " F ", curses.color_pair(6))
            stdscr.addstr(" Scan All", curses.color_pair(5))
            stdscr.addstr("   ")
            stdscr.addstr(" R ", curses.color_pair(6))
            stdscr.addstr(" Refresh Connection", curses.color_pair(5))
            stdscr.addstr("   ")
            stdscr.addstr(" C ", curses.color_pair(6))
            stdscr.addstr(" Edit Config", curses.color_pair(5))
            stdscr.addstr("   ")
            stdscr.addstr(" Q ", curses.color_pair(4) | curses.A_REVERSE)
            stdscr.addstr(" Quit", curses.color_pair(5))
        else:
            stdscr.addstr(h - 1, 2, " Press [ESC] or [Q] to return to Dashboard ", curses.color_pair(6))
            
        stdscr.refresh()
        
        # Handle User Inputs
        try:
            key = stdscr.getch()
            if key in [ord('q'), ord('Q')]:
                if current_tab == "settings":
                    current_tab = "dashboard"
                else:
                    running = False
                    
            elif key in [ord('c'), ord('C')]:
                if current_tab == "dashboard":
                    current_tab = "settings"
                    settings_scroll_idx = 0
                    with lock:
                        if 'config' in stats_data:
                            local_config = dict(stats_data['config'])
                            
            elif key in [ord('f'), ord('F')] and current_tab == "dashboard":
                session.post(f"{api_url}/api/scan-all", timeout=2)
                
            elif key in [ord('r'), ord('R')] and current_tab == "dashboard":
                session.post(f"{api_url}/api/check-connection", timeout=2)
                
            elif key == curses.KEY_DOWN and current_tab == "settings":
                if settings_scroll_idx < len(SETTINGS_KEYS) - 1:
                    settings_scroll_idx += 1
                    
            elif key == curses.KEY_UP and current_tab == "settings":
                if settings_scroll_idx > 0:
                    settings_scroll_idx -= 1
                    
            elif key == 27:  # ESC
                if current_tab == "settings":
                    current_tab = "dashboard"
                    
            elif key in [10, 13] and current_tab == "settings":
                # Edit selected setting
                cfg_key, display_name = SETTINGS_KEYS[settings_scroll_idx]
                curr_val = local_config.get(cfg_key, "")
                if isinstance(curr_val, list):
                    curr_val_str = ",".join(map(str, curr_val))
                else:
                    curr_val_str = str(curr_val)
                    
                new_val = get_input(stdscr, f"Enter new value for [{display_name}]", curr_val_str)
                if new_val is not None:
                    # Cast value correctly based on original type
                    if isinstance(curr_val, bool):
                        local_config[cfg_key] = new_val.lower() == "true"
                    elif isinstance(curr_val, int):
                        try: local_config[cfg_key] = int(new_val)
                        except: pass
                    elif isinstance(curr_val, float):
                        try: local_config[cfg_key] = float(new_val)
                        except: pass
                    elif isinstance(curr_val, list):
                        local_config[cfg_key] = [p.strip() for p in new_val.split(",") if p.strip()]
                    else:
                        local_config[cfg_key] = new_val
                        
            elif key in [ord('s'), ord('S')] and current_tab == "settings":
                # Save settings back to API
                stdscr.move(h - 1, 0)
                stdscr.clrtoeol()
                stdscr.addstr(h - 1, 2, "Saving settings to API...", curses.color_pair(3) | curses.A_BOLD)
                stdscr.refresh()
                
                try:
                    res = session.post(f"{api_url}/api/settings", json=local_config, timeout=5)
                    if res.status_code == 200:
                        stdscr.addstr(h - 1, 2, "Settings Saved Successfully! Rebuilding Cache...", curses.color_pair(2) | curses.A_BOLD)
                        stdscr.refresh()
                        time.sleep(1.5)
                        current_tab = "dashboard"
                    else:
                        err = res.json().get("error", "Unknown API error")
                        stdscr.addstr(h - 1, 2, f"Failed to Save: {err}", curses.color_pair(4) | curses.A_BOLD)
                        stdscr.refresh()
                        time.sleep(2.5)
                except Exception as e:
                    stdscr.addstr(h - 1, 2, f"Communication Error: {e}", curses.color_pair(4) | curses.A_BOLD)
                    stdscr.refresh()
                    time.sleep(2.5)
        except Exception:
            pass
            
        time.sleep(0.1)

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
