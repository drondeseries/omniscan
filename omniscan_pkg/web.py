import os
import time
import logging
import json
import asyncio
import re
import pathlib
import requests
import secrets
from collections import deque
from datetime import datetime
from typing import Optional, List
from fastapi import FastAPI, Request, Depends, HTTPException, status, Form, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from starlette.middleware.sessions import SessionMiddleware
from pydantic import BaseModel
from nicegui import ui, app as nicegui_app
nicegui_app.config.socket_io_js_transports = ['polling', 'websocket']
from plexapi.server import PlexServer
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
from .config import get_webhook_token, load_config, normalize_emby_url
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from .webhook_parser import parse_webhook
from .ui import init_ui

logger = logging.getLogger(__name__)

# Monkeypatch python-engineio to prevent KeyError: 'REQUEST_METHOD' on client disconnects during connection setup.
try:
    import sys
    import engineio.async_drivers.asgi
    original_translate_request = engineio.async_drivers.asgi.translate_request

    async def patched_translate_request(scope, receive, send):
        environ = await original_translate_request(scope, receive, send)
        if not environ:
            # Return a dummy environ to prevent KeyError: 'REQUEST_METHOD'
            environ = {
                'wsgi.input': None,
                'wsgi.errors': sys.stderr,
                'wsgi.version': (1, 0),
                'wsgi.async': True,
                'wsgi.multithread': False,
                'wsgi.multiprocess': False,
                'wsgi.run_once': False,
                'SERVER_SOFTWARE': 'asgi',
                'REQUEST_METHOD': 'GET',
                'PATH_INFO': scope.get('path', ''),
                'QUERY_STRING': '',
                'RAW_URI': scope.get('path', ''),
                'SCRIPT_NAME': '',
                'SERVER_PROTOCOL': 'HTTP/1.1',
                'REMOTE_ADDR': '127.0.0.1',
                'REMOTE_PORT': '0',
                'SERVER_NAME': 'asgi',
                'SERVER_PORT': '0',
                'asgi.receive': receive,
                'asgi.send': send,
                'asgi.scope': scope,
            }
        return environ

    engineio.async_drivers.asgi.translate_request = patched_translate_request
except Exception as e:
    logger.warning(f"Failed to apply python-engineio translate_request patch: {e}")

app = FastAPI()

main_loop = None

@app.on_event("startup")
async def startup_event():
    global main_loop
    main_loop = asyncio.get_running_loop()
    logging.getLogger().addHandler(ws_handler)

SECRET_KEY = os.environ.get("SECRET_KEY")
if not SECRET_KEY:
    import secrets
    SECRET_KEY = secrets.token_hex(32)
app.add_middleware(SessionMiddleware, secret_key=SECRET_KEY, max_age=2592000)  # 30 days session lifetime

# Define paths
BASE_DIR = os.path.dirname(os.path.dirname(__file__))
ASSETS_PATH = os.path.join(BASE_DIR, 'assets')

if os.path.exists(ASSETS_PATH):
    app.mount("/assets", StaticFiles(directory=ASSETS_PATH), name="assets")

scanner_instance = None

def set_scanner(scanner):
    global scanner_instance
    scanner_instance = scanner

def is_setup_completed():
    if not scanner_instance: return False
    config_pass = scanner_instance.config.get('WEB_PASSWORD')
    return bool(config_pass and config_pass.strip())

class SettingsUpdate(BaseModel):
    server_type: str
    server_url: str
    api_key: str
    plex_server: str
    plex_token: str
    scan_directories: str
    scan_workers: int
    scan_debounce: int
    scan_delay: float
    watch_mode: bool
    run_interval: int
    run_on_startup: bool
    start_time: Optional[str] = None
    incremental_scan: bool
    scan_since_days: int
    symlink_check: bool
    empty_trash: bool
    deletion_threshold: int
    abort_on_mass_deletion: bool
    notifications_enabled: bool
    discord_webhook_url: str
    notification_group_window: int = 15
    ignore_patterns: str
    log_level: str
    path_rewrites: str
    integrity_check: bool
    ffprobe_check: bool

class SetupSubmit(BaseModel):
    username: str
    password: str
    server_type: str
    plex_server: str
    plex_token: str
    server_url: str
    api_key: str
    scan_directories: str

class LibraryScanRequest(BaseModel):
    library_id: str

def get_current_user(request: Request):
    if not is_setup_completed():
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Setup required")
    user = request.session.get("user")
    if user: return user
    raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Not authenticated")

def verify_credentials(username, password):
    if not scanner_instance: return False
    config_user = scanner_instance.config.get('WEB_USERNAME', 'admin')
    config_pass = scanner_instance.config.get('WEB_PASSWORD')
    if not config_pass:
        return False
    return secrets.compare_digest(username, config_user) and secrets.compare_digest(password, config_pass)

recent_logs = deque(maxlen=100)

# Regex to strip ANSI/VT100 escape sequences (e.g. \x1b[1m, [0m, [33m etc.)
_ANSI_RE = re.compile(r'(?:\x1b|\x9b)\[[\d;]*[A-Za-z]|\[\d+m')

# Patterns for sensitive data that must never appear in the UI log viewer
_SENSITIVE_PATTERNS = [
    # Discord webhook URLs: redact the token segment after /webhooks/<id>/
    (re.compile(r'(discord\.com/api/webhooks/\d+/)([\w\-]+)'), r'\1[REDACTED]'),
    # Generic URL query params: apikey=, api_key=, token=, password=, secret=
    (re.compile(r'(?i)((?:apikey|api_key|token|password|secret|auth)=)[^&\s"]+'), r'\1[REDACTED]'),
    # Plex token in URL: X-Plex-Token=<value>
    (re.compile(r'(?i)(X-Plex-Token=)[^&\s"]+'), r'\1[REDACTED]'),
    # Bearer / token header values
    (re.compile(r'(?i)(Bearer\s+)[\w\-\.]+'), r'\1[REDACTED]'),
]

def _sanitize_log(line: str) -> str:
    """Redact sensitive values (tokens, keys, webhook secrets) from a log line."""
    for pattern, replacement in _SENSITIVE_PATTERNS:
        line = pattern.sub(replacement, line)
    return line

class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []
    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
    def disconnect(self, websocket: WebSocket):
        try:
            self.active_connections.remove(websocket)
        except ValueError:
            pass
    async def broadcast_to_clients(self, message: str):
        for connection in self.active_connections:
            try: await connection.send_text(message)
            except: pass

manager = ConnectionManager()

class WebSocketLogHandler(logging.Handler):
    def emit(self, record):
        try:
            log_entry = _sanitize_log(_ANSI_RE.sub('', self.format(record)))
            recent_logs.append(log_entry)
            
            global main_loop
            if main_loop is None:
                try:
                    main_loop = asyncio.get_running_loop()
                except RuntimeError:
                    return

            if main_loop and main_loop.is_running():
                asyncio.run_coroutine_threadsafe(manager.broadcast_to_clients(log_entry), main_loop)
        except Exception:
            pass

ws_handler = WebSocketLogHandler()
ws_handler.setFormatter(logging.Formatter('%(asctime)s | %(message)s', datefmt='%d %b %Y | %I:%M:%S %p'))

def mask_s(v): return (v[:4] + "****" + v[-4:]) if v and len(v) >= 8 else "********"
def unmask_v(n, r): return r if n == mask_s(r) else n
def fmt_size(size):
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size < 1024: return f"{size:.1f} {unit}"
        size /= 1024
    return f"{size:.1f} PB"

def get_storage_info(paths):
    usage = {}
    for p in paths:
        try:
            p_obj = pathlib.Path(p)
            if p_obj.exists():
                stat = os.statvfs(p)
                total = stat.f_blocks * stat.f_frsize
                free = stat.f_bavail * stat.f_frsize
                used = total - free
                pct = (used / total) * 100 if total > 0 else 0
                usage[total] = {
                    "path": p,
                    "total": fmt_size(total),
                    "used": fmt_size(used),
                    "free": fmt_size(free),
                    "percent": f"{pct:.1f}%"
                }
        except Exception as e:
            logger.debug(f"Failed to get storage info for {p}: {e}")
    return list(usage.values())

# --- API Routes ---

@app.get("/health")
async def health_check():
    return {"status": "ok"} if scanner_instance else JSONResponse(status_code=503, content={"status": "init"})

security = HTTPBasic()

def authenticate_basic(credentials: HTTPBasicCredentials = Depends(security)):
    if not scanner_instance:
        raise HTTPException(status_code=503, detail="Initializing")
    username = scanner_instance.config.get('WEB_USERNAME', 'admin')
    password = scanner_instance.config.get('WEB_PASSWORD', '')
    
    is_user_ok = secrets.compare_digest(credentials.username, username)
    is_pass_ok = secrets.compare_digest(credentials.password, password)
    
    if is_user_ok and is_pass_ok:
        return credentials.username
    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Unauthorized",
        headers={"WWW-Authenticate": "Basic"},
    )

@app.get("/metrics")
async def metrics(username: str = Depends(authenticate_basic)): 
    return HTMLResponse(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)

@app.get("/api/logs")
async def get_logs_route(u: str = Depends(get_current_user)):
    return list(recent_logs)

@app.websocket("/ws/logs")
async def websocket_endpoint(websocket: WebSocket):
    user = None
    try:
        user = websocket.session.get("user")
    except AssertionError:
        pass
        
    if not user:
        await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
        return

    await manager.connect(websocket)
    try:
        await websocket.send_text("INFO | >> System: Connected to Log Stream")
        while True: await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)

@app.get("/api/history")
async def get_history(search: Optional[str] = None, offset: int = 0, u: str = Depends(get_current_user)):
    if not scanner_instance: return []
    return scanner_instance.history.get_history(limit=50, offset=offset, search=search)

@app.post("/api/history/clear")
async def clear_history(u: str = Depends(get_current_user)):
    if not scanner_instance: return JSONResponse({"error": "init"}, status_code=500)
    if scanner_instance.history.clear_all_events():
        return {"status": "success"}
    return JSONResponse({"error": "Failed to clear history database"}, status_code=500)

@app.get("/api/stats")
async def get_stats(u: str = Depends(get_current_user)):
    if not scanner_instance: return {"error": "init"}
    p = []
    with scanner_instance.pending_scans_lock:
        now = time.time()
        for (lid, path, _), (lt, metadata) in scanner_instance.pending_scans.items():
            name = metadata['name'] if metadata else os.path.basename(path)
            details = metadata['details'] if metadata else ""
            remaining = max(0, int(scanner_instance.config.get('SCAN_DEBOUNCE', 10) - (now - lt)))
            p.append({
                "path": path, 
                "name": name, 
                "details": details, 
                "remaining": remaining
            })
    
    lib_stats = []
    with scanner_instance.library_files_lock:
        for lib in scanner_instance.library_sections_cache:
            lid = lib['id']
            if lid not in scanner_instance.library_files:
                scanner_instance._trigger_cache_fill(lid)
            count = scanner_instance.library_counts.get(lid, len(scanner_instance.library_files.get(lid, [])))
            lib_stats.append({"title": lib['title'], "type": lib['type'], "count": count})

    cfg = scanner_instance.config
    storage = await asyncio.get_event_loop().run_in_executor(None, get_storage_info, cfg.get('SCAN_PATHS', []))

    corrupt_count = scanner_instance.history.get_corrupt_count()

    return {
        "libraries": lib_stats,
        "pending": p,
        "watching_count": len(cfg.get('SCAN_PATHS', [])),
        "watching_paths": cfg.get('SCAN_PATHS', []),
        "storage": storage,
        "corrupt_count": corrupt_count,
        "is_scanning": scanner_instance.is_scanning,
        "uptime": datetime.now().strftime("%H:%M:%S"),
        "config": {
            "server_type": cfg.get('SERVER_TYPE'),
            "server_url": mask_s(cfg.get('SERVER_URL', '')),
            "api_key": mask_s(cfg.get('API_KEY', '')),
            "plex_server": cfg.get('PLEX_URL'),
            "plex_token": mask_s(cfg.get('TOKEN', '')),
            "scan_directories": "\n".join(cfg.get('SCAN_PATHS', [])),
            "scan_workers": cfg.get('SCAN_WORKERS'),
            "scan_debounce": cfg.get('SCAN_DEBOUNCE'),
            "scan_delay": cfg.get('SCAN_DELAY'),
            "watch_mode": cfg.get('WATCH_MODE'),
            "run_interval": cfg.get('RUN_INTERVAL'),
            "run_on_startup": cfg.get('RUN_ON_STARTUP'),
            "start_time": cfg.get('START_TIME'),
            "incremental_scan": cfg.get('INCREMENTAL_SCAN'),
            "scan_since_days": cfg.get('SCAN_SINCE_DAYS'),
            "symlink_check": cfg.get('SYMLINK_CHECK'),
            "empty_trash": cfg.get('EMPTY_TRASH'),
            "deletion_threshold": cfg.get('DELETION_THRESHOLD'),
            "abort_on_mass_deletion": cfg.get('ABORT_ON_MASS_DELETION'),
            "notifications_enabled": cfg.get('NOTIFICATIONS_ENABLED'),
            "discord_webhook_url": mask_s(cfg.get('DISCORD_WEBHOOK_URL')),
            "notification_group_window": cfg.get('NOTIFICATION_GROUP_WINDOW', 15),
            "ignore_patterns": "\n".join(cfg.get('IGNORE_PATTERNS', [])),
            "log_level": cfg.get('LOG_LEVEL'),
            "path_rewrites": "\n".join([f"{src}:{dst}" for src, dst in cfg.get('PATH_REWRITES', [])]),
            "integrity_check": cfg.get('INTEGRITY_CHECK'),
            "ffprobe_check": cfg.get('FFPROBE_CHECK')
        }
    }

@app.post("/api/scan-all")
async def trigger_full_scan(u: str = Depends(get_current_user)):
    if not scanner_instance or scanner_instance.is_scanning: return JSONResponse({"error": "busy"}, status_code=409)
    import threading
    threading.Thread(target=scanner_instance.run_scan, daemon=True).start()
    return {"status": "success"}

@app.post("/api/scan-library")
async def scan_library(r: LibraryScanRequest, u: str = Depends(get_current_user)):
    if not scanner_instance: return JSONResponse({"error": "init"}, status_code=500)
    target_id = str(r.library_id)
    found = False
    sections = scanner_instance.library_sections_cache
    for section in sections:
        if str(section["id"]) == target_id:
            found = True
            for location in section["locations"]:
                scanner_instance.trigger_scan(target_id, location, force=True)
            break
    if not found: return JSONResponse({"error": "Library not found"}, status_code=404)
    return {"status": "success", "message": "Scan triggered"}

@app.post("/api/test-connection")
async def test_conn(s: SettingsUpdate, u: str = Depends(get_current_user)):
    rt = unmask_v(s.plex_token, scanner_instance.config.get('TOKEN', ''))
    rk = unmask_v(s.api_key, scanner_instance.config.get('API_KEY', ''))
    ru = unmask_v(s.server_url, scanner_instance.config.get('SERVER_URL', ''))
    try:
        if s.server_type == 'plex':
            plex = PlexServer(s.plex_server, rt)
            return {"status": "success", "message": f"Linked to {plex.friendlyName}"}
        else:
            r = requests.get(f"{ru}/System/Info", headers={"X-Emby-Token": rk}, timeout=5)
            r.raise_for_status()
            return {"status": "success", "message": f"Linked to {s.server_type.capitalize()}"}
    except Exception as e: return JSONResponse({"status": "error", "message": str(e)}, status_code=400)

@app.post("/api/test-connection-unauthenticated")
async def test_conn_unauth(s: SettingsUpdate):
    if is_setup_completed():
        raise HTTPException(status_code=403, detail="Forbidden")
    try:
        if s.server_type == 'plex':
            plex = PlexServer(s.plex_server, s.plex_token)
            return {"status": "success", "message": f"Linked to {plex.friendlyName}"}
        else:
            r = requests.get(f"{s.server_url}/System/Info", headers={"X-Emby-Token": s.api_key}, timeout=5)
            r.raise_for_status()
            return {"status": "success", "message": f"Linked to {s.server_type.capitalize()}"}
    except Exception as e: return JSONResponse({"status": "error", "message": str(e)}, status_code=400)

@app.post("/api/setup")
async def setup_submit(r: SetupSubmit, request: Request):
    if is_setup_completed():
        return JSONResponse({"status": "error", "error": "Setup already completed"}, status_code=400)
    if not r.password.strip():
        return JSONResponse({"status": "error", "error": "Password cannot be empty"}, status_code=400)
    
    c = scanner_instance.config
    c['WEB_USERNAME'] = r.username
    c['WEB_PASSWORD'] = r.password
    c['SERVER_TYPE'] = r.server_type
    c['PLEX_URL'] = r.plex_server
    c['TOKEN'] = r.plex_token
    c['SERVER_URL'] = normalize_emby_url(r.server_url, c['SERVER_TYPE'])
    c['API_KEY'] = r.api_key
    c['SCAN_PATHS'] = [p.strip() for p in r.scan_directories.replace(',', '\n').split('\n') if p.strip()]

    try:
        cfg = configparser.ConfigParser()
        cfg.read('config.ini')
        
        sections_to_check = ['web', 'server', 'plex', 'scan']
        for sec in sections_to_check:
            if not cfg.has_section(sec):
                cfg.add_section(sec)
        
        cfg.set('web', 'username', str(c['WEB_USERNAME']))
        cfg.set('web', 'password', str(c['WEB_PASSWORD']))
        cfg.set('server', 'type', str(c['SERVER_TYPE']))
        cfg.set('server', 'url', str(c['SERVER_URL']))
        cfg.set('server', 'api_key', str(c['API_KEY']))
        cfg.set('plex', 'server', str(c['PLEX_URL']))
        cfg.set('plex', 'token', str(c['TOKEN']))
        cfg.set('scan', 'directories', ",".join(c['SCAN_PATHS']))

        with open('config.ini', 'w') as f:
            cfg.write(f)

        request.session["user"] = r.username
        
        # Connect & reload
        if c['SERVER_TYPE'] == 'plex':
            scanner_instance.connect_to_plex(retry=False)
            scanner_instance.get_library_ids()
            
        return {"status": "success"}
    except Exception as e:
        logger.error(f"Setup save error: {e}")
        return JSONResponse({"status": "error", "error": str(e)}, status_code=500)

@app.post("/api/settings")
async def update_settings(s: SettingsUpdate, u: str = Depends(get_current_user)):
    if not scanner_instance: return JSONResponse({"error": "init"}, status_code=500)
    c = scanner_instance.config
    c['SERVER_TYPE'] = s.server_type
    c['SERVER_URL'] = normalize_emby_url(unmask_v(s.server_url, c.get('SERVER_URL', '')), c['SERVER_TYPE'])
    c['API_KEY'] = unmask_v(s.api_key, c.get('API_KEY', ''))
    c['PLEX_URL'] = s.plex_server
    c['TOKEN'] = unmask_v(s.plex_token, c.get('TOKEN', ''))
    c['SCAN_PATHS'] = [p.strip() for p in s.scan_directories.replace(',', '\n').split('\n') if p.strip()]
    c['SCAN_WORKERS'] = s.scan_workers
    c['SCAN_DEBOUNCE'] = s.scan_debounce
    c['SCAN_DELAY'] = s.scan_delay
    c['WATCH_MODE'] = s.watch_mode
    c['RUN_INTERVAL'] = s.run_interval
    c['RUN_ON_STARTUP'] = s.run_on_startup
    c['START_TIME'] = s.start_time
    c['INCREMENTAL_SCAN'] = s.incremental_scan
    c['SCAN_SINCE_DAYS'] = s.scan_since_days
    c['SYMLINK_CHECK'] = s.symlink_check
    c['EMPTY_TRASH'] = s.empty_trash
    c['INTEGRITY_CHECK'] = s.integrity_check
    c['FFPROBE_CHECK'] = s.ffprobe_check
    c['DELETION_THRESHOLD'] = s.deletion_threshold
    c['ABORT_ON_MASS_DELETION'] = s.abort_on_mass_deletion
    c['NOTIFICATIONS_ENABLED'] = s.notifications_enabled
    c['DISCORD_WEBHOOK_URL'] = unmask_v(s.discord_webhook_url, c.get('DISCORD_WEBHOOK_URL', ''))
    c['NOTIFICATION_GROUP_WINDOW'] = s.notification_group_window
    c['IGNORE_PATTERNS'] = [p.strip() for p in s.ignore_patterns.replace(',', '\n').split('\n') if p.strip()]
    c['LOG_LEVEL'] = s.log_level
    
    c['PATH_REWRITES'] = []
    for line in s.path_rewrites.replace(',', '\n').split('\n'):
        line = line.strip()
        if not line: continue
        if ':' in line:
            parts = line.split(':', 1)
            c['PATH_REWRITES'].append((parts[0].strip(), parts[1].strip()))

    try:
        cfg = configparser.ConfigParser()
        cfg.read('config.ini')
        for sec in ['server', 'plex', 'behaviour', 'notifications', 'scan', 'ignore', 'logs', 'rewrite']:
            if not cfg.has_section(sec): cfg.add_section(sec)
        cfg.set('server', 'type', str(c['SERVER_TYPE']))
        cfg.set('server', 'url', str(c['SERVER_URL']))
        cfg.set('server', 'api_key', str(c['API_KEY']))
        cfg.set('plex', 'server', str(c['PLEX_URL']))
        cfg.set('plex', 'token', str(c['TOKEN']))
        cfg.set('behaviour', 'scan_workers', str(c['SCAN_WORKERS']))
        cfg.set('behaviour', 'scan_debounce', str(c['SCAN_DEBOUNCE']))
        cfg.set('behaviour', 'scan_delay', str(c['SCAN_DELAY']))
        cfg.set('behaviour', 'watch', str(c['WATCH_MODE']).lower())
        cfg.set('behaviour', 'run_interval', str(c['RUN_INTERVAL']))
        cfg.set('behaviour', 'run_on_startup', str(c['RUN_ON_STARTUP']).lower())
        cfg.set('behaviour', 'start_time', c['START_TIME'] if c['START_TIME'] else "")
        cfg.set('behaviour', 'incremental_scan', str(c['INCREMENTAL_SCAN']).lower())
        cfg.set('behaviour', 'scan_since_days', str(c['SCAN_SINCE_DAYS']))
        cfg.set('behaviour', 'symlink_check', str(c['SYMLINK_CHECK']).lower())
        cfg.set('behaviour', 'empty_trash', str(c['EMPTY_TRASH']).lower())
        cfg.set('behaviour', 'integrity_check', str(c['INTEGRITY_CHECK']).lower())
        cfg.set('behaviour', 'ffprobe_check', str(c['FFPROBE_CHECK']).lower())
        cfg.set('behaviour', 'deletion_threshold', str(c['DELETION_THRESHOLD']))
        cfg.set('behaviour', 'abort_on_mass_deletion', str(c['ABORT_ON_MASS_DELETION']).lower())
        cfg.set('notifications', 'enabled', str(c['NOTIFICATIONS_ENABLED']).lower())
        cfg.set('notifications', 'discord_webhook_url', str(c['DISCORD_WEBHOOK_URL']))
        cfg.set('behaviour', 'notification_group_window', str(c['NOTIFICATION_GROUP_WINDOW']))
        cfg.set('ignore', 'patterns', ",".join(c['IGNORE_PATTERNS']))
        cfg.set('logs', 'loglevel', str(c['LOG_LEVEL']))
        cfg.set('rewrite', 'mappings', ",".join([f"{src}:{dst}" for src, dst in c['PATH_REWRITES']]))
        
        with open('config.ini', 'w') as f: cfg.write(f)
        
        if c['SERVER_TYPE'] == 'plex': 
            scanner_instance.connect_to_plex(retry=False)
            scanner_instance.get_library_ids()
            
        return {"status": "success"}
    except Exception as e: return JSONResponse({"error": str(e)}, status_code=500)

@app.post("/api/restart")
async def restart_system(u: str = Depends(get_current_user)):
    import threading
    threading.Thread(target=lambda: (time.sleep(1), os._exit(0))).start()
    return {"status": "success"}

@app.get("/api/browser/list")
async def list_f(path: str = None, query: str = None, u: str = Depends(get_current_user)):
    if not scanner_instance: return {"error": "init"}
    sp = scanner_instance.config.get('SCAN_PATHS', [])
    
    # Handle Keyword Search
    if query and query.strip():
        search_query = query.strip().lower()
        results = []
        search_roots = []
        
        if path:
            rp = pathlib.Path(path).resolve()
            allowed = False
            for s in sp:
                bp = pathlib.Path(s).resolve()
                if rp == bp or bp in rp.parents: allowed = True; break
            if allowed: search_roots = [str(rp)]
        else:
            search_roots = sp

        def perform_search():
            matches = []
            for root in search_roots:
                for r, dirs, files in os.walk(root):
                    for name in dirs + files:
                        if search_query in name.lower():
                            full_path = os.path.join(r, name)
                            try:
                                stat = os.stat(full_path)
                                matches.append({
                                    "name": name, "path": full_path, "is_dir": os.path.isdir(full_path),
                                    "size": stat.st_size if os.path.isfile(full_path) else 0,
                                    "size_fmt": fmt_size(stat.st_size) if os.path.isfile(full_path) else "",
                                    "date": datetime.fromtimestamp(stat.st_mtime).strftime('%Y-%m-%d %H:%M'),
                                    "ext": os.path.splitext(name)[1].lower() if os.path.isfile(full_path) else None
                                })
                            except Exception as e:
                                logger.debug(f"Failed to stat file during search: {full_path} - {e}")
                                continue
                        if len(matches) >= 100: return matches
            return matches

        results = await asyncio.get_event_loop().run_in_executor(None, perform_search)
        return {"current_path": f"Search results for: {query}", "items": results, "is_search": True}

    if not path: return {"current_path": "", "is_root": True, "items": [{"name": p, "path": p, "is_dir": True, "size_fmt": "", "date": ""} for p in sp]}
    try:
        rp = pathlib.Path(path).resolve(); allowed = False
        for s in sp:
            bp = pathlib.Path(s).resolve()
            if rp == bp or bp in rp.parents: allowed = True; break
        if not allowed: return JSONResponse({"error": "denied"}, status_code=403)
        it = []; roots = [pathlib.Path(s).resolve() for s in sp]
        it.append({"name": "..", "path": str(rp.parent) if rp not in roots else "", "is_dir": True, "is_back": True})
        with os.scandir(rp) as s:
            for e in s:
                if not e.name.startswith('.'):
                    try:
                        stat = e.stat()
                        it.append({
                            "name": e.name, "path": e.path, "is_dir": e.is_dir(),
                            "size": stat.st_size if e.is_file() else 0,
                            "size_fmt": fmt_size(stat.st_size) if e.is_file() else "",
                            "date": datetime.fromtimestamp(stat.st_mtime).strftime('%Y-%m-%d %H:%M'),
                            "ext": os.path.splitext(e.name)[1].lower() if e.is_file() else None
                        })
                    except Exception:
                        pass
        return {"current_path": str(rp), "items": it}
    except Exception as e:
        logger.error(f"Failed to list directory: {e}")
        return JSONResponse({"error": "fail"}, status_code=500)

@app.post("/api/browser/action")
async def browser_act(d: dict, u: str = Depends(get_current_user)):
    a = d.get('action'); p = d.get('path')
    if not scanner_instance or not p: return JSONResponse({"error": "invalid"}, status_code=400)
    try:
        rp = pathlib.Path(p).resolve(); allowed = False
        for s in scanner_instance.config.get('SCAN_PATHS', []):
            bp = pathlib.Path(s).resolve()
            if rp == bp or bp in rp.parents: allowed = True; break
        if not allowed or not rp.exists(): return JSONResponse({"error": "denied"}, status_code=403)
        p = str(rp)
    except Exception as e:
        logger.error(f"Invalid path requested in browser action: {p} - {e}")
        return JSONResponse({"error": "invalid"}, status_code=400)
    
    if a == 'scan':
        logger.info(f"Manual scan request for: {p}")
        if os.path.isfile(p): 
            scanner_instance.scan_file(p)
        else:
            lid, title, _ = scanner_instance.get_library_id_for_path(p)
            if lid: 
                scanner_instance.trigger_scan(lid, p)
            else:
                logger.warning(f"Scan ignored: Path not in any known library: {p}")
        return {"status": "success"}
    return {"error": "unknown"}

@app.post("/api/test-webhook")
async def test_webhook(data: dict, u: str = Depends(get_current_user)):
    url = data.get('url')
    if not url: return JSONResponse({"error": "No URL provided"}, status_code=400)
    
    real_url = scanner_instance.config.get('DISCORD_WEBHOOK_URL', '')
    if url == mask_s(real_url): 
        url = real_url
        
    if not url or not url.startswith("http"): 
        return JSONResponse({"error": "Invalid URL"}, status_code=400)
        
    try:
        from .notifications import send_discord_webhook_sync
        from discord import Embed, Color
        
        embed = Embed(
            title="✅ Omniscan Test Message",
            description="Your notification configuration is working correctly!",
            color=Color.green(),
            timestamp=datetime.now()
        )
        embed.set_footer(text="Omniscan Media Monitor")
        
        if send_discord_webhook_sync(url, embed, scanner_instance.config):
            return {"status": "success"}
        else:
            return JSONResponse({"error": "Failed to send webhook. Check logs for details."}, status_code=400)
    except Exception as e: 
        return JSONResponse({"error": str(e)}, status_code=400)

@app.post("/api/validate-paths")
async def validate_paths(data: dict, u: str = Depends(get_current_user)):
    paths = data.get('paths', [])
    if isinstance(paths, str): paths = [p.strip() for p in paths.split('\n') if p.strip()]
    results = []
    for p in paths:
        exists = os.path.isdir(p)
        results.append({"path": p, "valid": exists})
    return {"results": results}

@app.post("/api/check-connection")
async def check_conn_status(u: str = Depends(get_current_user)):
    if not scanner_instance: return JSONResponse({"error": "init"}, status_code=503)
    c = scanner_instance.config
    st = c.get('SERVER_TYPE', 'plex')
    url = c.get('PLEX_URL') if st == 'plex' else c.get('SERVER_URL')
    token = c.get('TOKEN') if st == 'plex' else c.get('API_KEY')
    try:
        if st == 'plex':
            p = PlexServer(url, token)
            return {"status": "success", "message": f"{p.friendlyName}", "server": "Plex"}
        else:
            h = {"X-Emby-Token": token}
            r = requests.get(f"{url}/System/Info", headers=h, timeout=5)
            r.raise_for_status()
            return {"status": "success", "message": "Online", "server": st.capitalize()}
    except Exception as e: return JSONResponse({"status": "error", "message": str(e)}, status_code=400)

@app.post("/api/webhook")
async def webhook_trigger(request: Request, apikey: Optional[str] = None):
    if not scanner_instance: return JSONResponse({"error": "init"}, status_code=500)
    
    # Authenticate webhook request
    expected_token = get_webhook_token(scanner_instance.config.get('WEB_PASSWORD', 'admin'))
    if apikey != expected_token:
        raise HTTPException(status_code=401, detail="Unauthorized")
    try:
        data = await request.json()
        logger.info(f"Webhook received: {data.keys()}")
        
        if data.get('eventType') == 'Test':
            logger.info("Test webhook received successfully.")
            return {"status": "success", "message": "Test webhook received successfully"}
        
        paths_to_scan = set()
        raw_paths = set()
        
        # 1. Generic 'path' or 'paths'
        if 'path' in data: raw_paths.add(data['path'])
        if 'paths' in data and isinstance(data['paths'], list): raw_paths.update(data['paths'])
        
        # 2. Sonarr/Radarr (Grab/Download/Rename)
        # Movie
        if 'movie' in data and 'folderPath' in data['movie']: raw_paths.add(data['movie']['folderPath'])
        if 'movieFile' in data and 'path' in data['movieFile']: raw_paths.add(data['movieFile']['path'])
        
        # Series
        if 'series' in data and 'path' in data['series']: raw_paths.add(data['series']['path'])
        if 'episodeFile' in data and 'path' in data['episodeFile']: raw_paths.add(data['episodeFile']['path'])
        
        # 3. Lidarr (Music)
        if 'artist' in data and 'path' in data['artist']: raw_paths.add(data['artist']['path'])
        if 'trackFile' in data and 'path' in data['trackFile']: raw_paths.add(data['trackFile']['path'])
        if 'trackFiles' in data and isinstance(data['trackFiles'], list):
            for tf in data['trackFiles']:
                if isinstance(tf, dict) and 'path' in tf:
                    raw_paths.add(tf['path'])
        
        # 4. Readarr (Books)
        if 'author' in data and 'path' in data['author']: raw_paths.add(data['author']['path'])
        if 'bookFile' in data and 'path' in data['bookFile']: raw_paths.add(data['bookFile']['path'])
        
        # Rename (source/dest)
        if 'sourcePath' in data: raw_paths.add(data['sourcePath'])
        if 'destPath' in data: raw_paths.add(data['destPath'])

        # Apply path rewrites (Autopulse feature)
        rewrites = scanner_instance.config.get('PATH_REWRITES', [])
        rewritten_paths = set()
        for p in raw_paths:
            if not p: continue
            rewrote = False
            for src, dst in rewrites:
                if p.startswith(src):
                    new_p = os.path.normpath(p.replace(src, dst, 1))
                    logger.info(f"Webhook path rewrote: {p} -> {new_p}")
                    rewritten_paths.add(new_p)
                    rewrote = True
                    break
            if not rewrote:
                rewritten_paths.add(os.path.normpath(p))

        # De-duplicate: If we have /Show/Season/Ep and /Show, keep only /Show/Season/Ep
        sorted_paths = sorted([p for p in rewritten_paths if p], key=len, reverse=True)
        for p in sorted_paths:
            is_redundant = False
            for existing in paths_to_scan:
                if existing.startswith(p + os.sep) or existing == p:
                    is_redundant = True
                    break
            if not is_redundant:
                paths_to_scan.add(p)

        if not paths_to_scan:
            return JSONResponse({"status": "ignored", "message": "No paths found in payload"}, status_code=200)

        triggered = 0
        metadata = parse_webhook(data)
        for p in paths_to_scan:
            if not p: continue
            logger.info(f"Webhook trigger for: {p}")
            
            # Retry logic for filesystem latency (e.g. rclone mounts)
            exists = False
            for i in range(30):  # Increase to 30 seconds for slower mounts
                if os.path.exists(p):
                    exists = True
                    break
                if i % 5 == 0 and i > 0:
                    logger.debug(f"Waiting for path to appear ({i}s): {p}")
                await asyncio.sleep(1)
            
            if exists:
                if os.path.isfile(p):
                    scanner_instance.submit_file_event('created', p, metadata=metadata)
                    triggered += 1
                elif os.path.isdir(p):
                    lid, _, _ = scanner_instance.get_library_id_for_path(p)
                    if lid:
                        scanner_instance.trigger_scan(lid, p, metadata=metadata)
                        triggered += 1
                    else:
                        logger.warning(f"Webhook path not in library: {p}")
            else:
                # If path doesn't exist, try falling back to parent folder
                parent = os.path.dirname(p)
                lid, _, library_type = scanner_instance.get_library_id_for_path(p)
                
                # Only fallback if parent exists AND is not the library root
                if os.path.isdir(parent) and not scanner_instance.is_library_root(lid, parent):
                    if library_type == 'show' and scanner_instance.is_entity_root(parent):
                        logger.info(f"Webhook path missing, but parent is Show Root. Stopping fallback to avoid broad scan: {parent}")
                    else:
                        logger.info(f"Webhook path missing, falling back to parent: {parent}")
                        if lid:
                            scanner_instance.trigger_scan(lid, parent, metadata=metadata)
                            triggered += 1
                else:
                    logger.warning(f"Webhook path does not exist: {p}")

        return {"status": "success", "triggered": triggered}
        
    except Exception as e:
        logger.error(f"Webhook error: {e}")
        return JSONResponse({"error": str(e)}, status_code=400)

@app.post("/login")
async def login_route(request: Request, username: str = Form(...), password: str = Form(...)):
    if not is_setup_completed():
        return RedirectResponse(url="/setup", status_code=status.HTTP_302_FOUND)
    if verify_credentials(username, password):
        request.session["user"] = username
        return RedirectResponse(url="/", status_code=status.HTTP_303_SEE_OTHER)
    return RedirectResponse(url="/login?error=Invalid Credentials", status_code=status.HTTP_303_SEE_OTHER)

@app.get("/logout")
async def logout_route(request: Request):
    request.session.clear()
    return RedirectResponse(url="/login", status_code=status.HTTP_303_SEE_OTHER)

# --- Server Start ---

def run_web_server(scanner, host="0.0.0.0", port=8000):
    import uvicorn
    set_scanner(scanner)
    init_ui(app, scanner)  # Register NiceGUI pages
    ui.run_with(app, title="Omniscan")
    uvicorn.run(app, host=host, port=port, log_level="error")
