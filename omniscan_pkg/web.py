from fastapi import FastAPI, Request, Depends, HTTPException, status, Form, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from starlette.middleware.sessions import SessionMiddleware
from pydantic import BaseModel
from typing import List, Optional
import os
import time
import logging
import configparser
import requests
import secrets
import pathlib
import asyncio
import sqlite3
import shutil
from datetime import datetime
from collections import deque
from plexapi.server import PlexServer
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
from .metrics import HEALTH_CHECKS_TOTAL

logger = logging.getLogger(__name__)

app = FastAPI()

main_loop = None

@app.on_event("startup")
async def startup_event():
    global main_loop
    main_loop = asyncio.get_running_loop()
    logging.getLogger().addHandler(ws_handler)

SECRET_KEY = secrets.token_urlsafe(32)
app.add_middleware(SessionMiddleware, secret_key=SECRET_KEY)

# Define paths
BASE_DIR = os.path.dirname(os.path.dirname(__file__))
ASSETS_PATH = os.path.join(BASE_DIR, 'assets')
TEMPLATES_PATH = os.path.join(BASE_DIR, 'omniscan_pkg', 'templates')

if os.path.exists(ASSETS_PATH):
    app.mount("/assets", StaticFiles(directory=ASSETS_PATH), name="assets")

scanner_instance = None

def set_scanner(scanner):
    global scanner_instance
    scanner_instance = scanner

def get_current_user(request: Request):
    user = request.session.get("user")
    if user: return user
    if request.url.path.startswith("/api"): raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Not authenticated")
    raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Not authenticated")

def verify_credentials(username, password):
    if not scanner_instance: return False
    config_user = scanner_instance.config.get('WEB_USERNAME', 'admin')
    config_pass = scanner_instance.config.get('WEB_PASSWORD')
    if not config_pass:
        return False
    return secrets.compare_digest(username, config_user) and secrets.compare_digest(password, config_pass)

def load_template(filename):
    try:
        with open(os.path.join(TEMPLATES_PATH, filename), 'r') as f:
            return f.read()
    except Exception as e:
        logger.error(f"Error loading template {filename}: {e}")
        return f"Error loading template: {e}"

recent_logs = deque(maxlen=100)

class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []
    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)
    async def broadcast_to_clients(self, message: str):
        for connection in self.active_connections:
            try: await connection.send_text(message)
            except: pass

manager = ConnectionManager()

class WebSocketLogHandler(logging.Handler):
    def emit(self, record):
        try:
            log_entry = self.format(record)
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

@app.get("/api/logs")
async def get_logs(u: str = Depends(get_current_user)):
    return list(recent_logs)

@app.websocket("/ws/logs")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        await websocket.send_text("INFO | >> System: Connected to Log Stream")
        while True: await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)

ws_handler = WebSocketLogHandler()
ws_handler.setFormatter(logging.Formatter('%(asctime)s | %(message)s', datefmt='%d %b %Y | %I:%M:%S %p'))

@app.get("/login", response_class=HTMLResponse)
async def login_page(request: Request, error: Optional[str] = None):
    html = load_template("login.html")
    err_h = f'<div class="bg-red-500/10 p-4 mb-6 rounded-xl border border-red-500/20 text-red-400 text-xs font-bold uppercase">{error}</div>' if error else ""
    return html.replace("__ERROR__", err_h)

@app.post("/login")
async def login(request: Request, username: str = Form(...), password: str = Form(...)):
    if verify_credentials(username, password):
        request.session["user"] = username
        return RedirectResponse(url="/", status_code=status.HTTP_303_SEE_OTHER)
    return RedirectResponse(url="/login?error=Invalid Credentials", status_code=status.HTTP_303_SEE_OTHER)

@app.get("/logout")
async def logout(request: Request):
    request.session.clear()
    return RedirectResponse(url="/login", status_code=status.HTTP_303_SEE_OTHER)

@app.get("/health")
async def health_check():
    return {"status": "ok"} if scanner_instance else JSONResponse(status_code=503, content={"status": "init"})

@app.get("/metrics")
async def metrics(): return HTMLResponse(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)

@app.get("/api/history")
async def get_history(search: Optional[str] = None, offset: int = 0, u: str = Depends(get_current_user)):
    if not scanner_instance: return []
    return scanner_instance.history.get_history(limit=50, offset=offset, search=search)

@app.post("/api/history/clear")
async def clear_history(u: str = Depends(get_current_user)):
    if not scanner_instance: return JSONResponse({"error": "init"}, status_code=500)
    try:
        conn = sqlite3.connect('history.db')
        c = conn.cursor()
        c.execute("DELETE FROM events")
        conn.commit()
        conn.close()
        return {"status": "success"}
    except Exception as e: return JSONResponse({"error": str(e)}, status_code=500)

class SettingsUpdate(BaseModel):
    server_type: str; server_url: str; api_key: str; plex_server: str; plex_token: str; scan_directories: str
    scan_workers: int; scan_debounce: int; scan_delay: float; use_polling: bool; watch_mode: bool; run_interval: int; run_on_startup: bool
    start_time: Optional[str] = None; incremental_scan: bool; scan_since_days: int; health_check: bool; symlink_check: bool
    ignore_samples: bool; min_duration: int; deletion_threshold: int; abort_on_mass_deletion: bool
    notifications_enabled: bool; discord_webhook_url: str; ignore_patterns: str; log_level: str

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
            if not os.path.exists(p): continue
            mount = os.path.abspath(p)
            while not os.path.ismount(mount) and mount != '/': mount = os.path.dirname(mount)
            if mount in usage: continue
            total, used, free = shutil.disk_usage(mount)
            usage[mount] = {"path": mount, "total": fmt_size(total), "used": fmt_size(used), "free": fmt_size(free), "percent": int((used/total)*100)}
        except: pass
    return list(usage.values())

@app.get("/api/stats")
async def get_stats(u: str = Depends(get_current_user)):
    if not scanner_instance: return {"error": "init"}
    p = []
    with scanner_instance.pending_scans_lock:
        now = time.time()
        for (lid, path), lt in scanner_instance.pending_scans.items():
            p.append({"path": os.path.basename(path), "full_path": path, "remaining": max(0, int(scanner_instance.config.get('SCAN_DEBOUNCE', 10) - (now - lt)))})
    
    lib_stats = []
    with scanner_instance.library_files_lock:
        for lib in scanner_instance.library_sections_cache:
            count = len(scanner_instance.library_files.get(lib['id'], []))
            lib_stats.append({"title": lib['title'], "type": lib['type'], "count": count})

    hs = {"healthy": 0, "corrupt": 0, "timeout": 0}
    try:
        conn = sqlite3.connect('history.db'); c = conn.cursor()
        c.execute("SELECT status, COUNT(*) FROM events WHERE event_type LIKE 'Health Check%' GROUP BY status")
        for row in c.fetchall():
            s = row[0].lower()
            if 'healthy' in s: hs['healthy'] = row[1]
            elif 'corrupt' in s or 'failed' in s: hs['corrupt'] += row[1]
            elif 'timeout' in s: hs['timeout'] = row[1]
        conn.close()
    except: pass
    
    cfg = scanner_instance.config
    storage = await asyncio.get_event_loop().run_in_executor(None, get_storage_info, cfg.get('SCAN_PATHS', []))

    return {
        "libraries": lib_stats, "pending": p, "watching_count": len(cfg.get('SCAN_PATHS', [])), "watching_paths": cfg.get('SCAN_PATHS', []),
        "health": {"recent": scanner_instance.last_health_results, "total": int(HEALTH_CHECKS_TOTAL._value.get()), "stats": hs},
        "storage": storage,
        "is_scanning": scanner_instance.is_scanning, "uptime": datetime.now().strftime("%H:%M:%S"),
        "config": {
            "server_type": cfg.get('SERVER_TYPE'), "server_url": mask_s(cfg.get('SERVER_URL', '')), "api_key": mask_s(cfg.get('API_KEY', '')),
            "plex_server": cfg.get('PLEX_URL'), "plex_token": mask_s(cfg.get('TOKEN', '')), "scan_directories": "\n".join(cfg.get('SCAN_PATHS', [])),
            "scan_workers": cfg.get('SCAN_WORKERS'), "scan_debounce": cfg.get('SCAN_DEBOUNCE'), "scan_delay": cfg.get('SCAN_DELAY'),
            "use_polling": cfg.get('USE_POLLING'), "watch_mode": cfg.get('WATCH_MODE'), "run_interval": cfg.get('RUN_INTERVAL'), "run_on_startup": cfg.get('RUN_ON_STARTUP'),
            "start_time": cfg.get('START_TIME'), "incremental_scan": cfg.get('INCREMENTAL_SCAN'), "scan_since_days": cfg.get('SCAN_SINCE_DAYS'),
            "health_check": cfg.get('HEALTH_CHECK'), "symlink_check": cfg.get('SYMLINK_CHECK'), "ignore_samples": cfg.get('IGNORE_SAMPLES'),
            "min_duration": cfg.get('MIN_DURATION'), "deletion_threshold": cfg.get('DELETION_THRESHOLD'), "abort_on_mass_deletion": cfg.get('ABORT_ON_MASS_DELETION'),
            "notifications_enabled": cfg.get('NOTIFICATIONS_ENABLED'),
            "discord_webhook_url": mask_s(cfg.get('DISCORD_WEBHOOK_URL')), "ignore_patterns": "\n".join(cfg.get('IGNORE_PATTERNS', [])), "log_level": cfg.get('LOG_LEVEL')
        }
    }

@app.post("/api/scan-all")
async def trigger_full_scan(u: str = Depends(get_current_user)):
    if not scanner_instance or scanner_instance.is_scanning: return JSONResponse({"error": "busy"}, status_code=409)
    import threading; threading.Thread(target=scanner_instance.run_scan, daemon=True).start()
    return {"status": "success"}

class LibraryScanRequest(BaseModel):
    library_id: str

@app.post("/api/scan-library")
async def scan_library(r: LibraryScanRequest, u: str = Depends(get_current_user)):
    if not scanner_instance: return JSONResponse({"error": "init"}, status_code=500)
    target_id = str(r.library_id)
    found = False
    # Access cache safely
    sections = scanner_instance.library_sections_cache
    for section in sections:
        if str(section['id']) == target_id:
            found = True
            for location in section['locations']:
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
            plex = PlexServer(s.plex_server, rt); return {"status": "success", "message": f"Linked to {plex.friendlyName}"}
        else:
            r = requests.get(f"{ru}/System/Info", headers={"X-Emby-Token": rk}, timeout=5); r.raise_for_status()
            return {"status": "success", "message": f"Linked to {s.server_type.capitalize()}"}
    except Exception as e: return JSONResponse({"status": "error", "message": str(e)}, status_code=400)

@app.post("/api/settings")
async def update_settings(s: SettingsUpdate, u: str = Depends(get_current_user)):
    if not scanner_instance: return JSONResponse({"error": "init"}, status_code=500)
    c = scanner_instance.config
    c['SERVER_TYPE'] = s.server_type; c['SERVER_URL'] = unmask_v(s.server_url, c.get('SERVER_URL', '')); c['API_KEY'] = unmask_v(s.api_key, c.get('API_KEY', ''))
    c['PLEX_URL'] = s.plex_server; c['TOKEN'] = unmask_v(s.plex_token, c.get('TOKEN', ''))
    c['SCAN_PATHS'] = [p.strip() for p in s.scan_directories.replace(',', '\n').split('\n') if p.strip()]
    c['SCAN_WORKERS'] = s.scan_workers; c['SCAN_DEBOUNCE'] = s.scan_debounce; c['SCAN_DELAY'] = s.scan_delay
    c['USE_POLLING'] = s.use_polling; c['WATCH_MODE'] = s.watch_mode; c['RUN_INTERVAL'] = s.run_interval; c['RUN_ON_STARTUP'] = s.run_on_startup; c['START_TIME'] = s.start_time
    c['INCREMENTAL_SCAN'] = s.incremental_scan; c['SCAN_SINCE_DAYS'] = s.scan_since_days; c['HEALTH_CHECK'] = s.health_check
    c['SYMLINK_CHECK'] = s.symlink_check; c['IGNORE_SAMPLES'] = s.ignore_samples; c['MIN_DURATION'] = s.min_duration
    c['DELETION_THRESHOLD'] = s.deletion_threshold; c['ABORT_ON_MASS_DELETION'] = s.abort_on_mass_deletion
    c['NOTIFICATIONS_ENABLED'] = s.notifications_enabled; c['DISCORD_WEBHOOK_URL'] = unmask_v(s.discord_webhook_url, c.get('DISCORD_WEBHOOK_URL', ''))
    c['IGNORE_PATTERNS'] = [p.strip() for p in s.ignore_patterns.replace(',', '\n').split('\n') if p.strip()]; c['LOG_LEVEL'] = s.log_level
    try:
        cfg = configparser.ConfigParser(); cfg.read('config.ini')
        for sec in ['server', 'plex', 'behaviour', 'notifications', 'scan', 'ignore', 'logs']:
            if not cfg.has_section(sec): cfg.add_section(sec)
        cfg.set('server', 'type', str(c['SERVER_TYPE'])); cfg.set('server', 'url', str(c['SERVER_URL'])); cfg.set('server', 'api_key', str(c['API_KEY']))
        cfg.set('plex', 'server', str(c['PLEX_URL'])); cfg.set('plex', 'token', str(c['TOKEN']))
        cfg.set('scan', 'directories', ",".join(c['SCAN_PATHS']))
        cfg.set('behaviour', 'scan_workers', str(c['SCAN_WORKERS'])); cfg.set('behaviour', 'scan_debounce', str(c['SCAN_DEBOUNCE'])); cfg.set('behaviour', 'scan_delay', str(c['SCAN_DELAY']))
        cfg.set('behaviour', 'use_polling', str(c['USE_POLLING']).lower()); cfg.set('behaviour', 'watch', str(c['WATCH_MODE']).lower()); cfg.set('behaviour', 'run_interval', str(c['RUN_INTERVAL'])); cfg.set('behaviour', 'run_on_startup', str(c['RUN_ON_STARTUP']).lower())
        cfg.set('behaviour', 'start_time', c['START_TIME'] if c['START_TIME'] else ""); cfg.set('behaviour', 'incremental_scan', str(c['INCREMENTAL_SCAN']).lower()); cfg.set('behaviour', 'scan_since_days', str(c['SCAN_SINCE_DAYS']))
        cfg.set('behaviour', 'health_check', str(c['HEALTH_CHECK']).lower()); cfg.set('behaviour', 'symlink_check', str(c['SYMLINK_CHECK']).lower()); cfg.set('behaviour', 'ignore_samples', str(c['IGNORE_SAMPLES']).lower()); cfg.set('behaviour', 'min_duration', str(c['MIN_DURATION']))
        cfg.set('behaviour', 'deletion_threshold', str(c['DELETION_THRESHOLD'])); cfg.set('behaviour', 'abort_on_mass_deletion', str(c['ABORT_ON_MASS_DELETION']).lower())
        cfg.set('notifications', 'enabled', str(c['NOTIFICATIONS_ENABLED']).lower()); cfg.set('notifications', 'discord_webhook_url', str(c['DISCORD_WEBHOOK_URL']))
        cfg.set('ignore', 'patterns', ",".join(c['IGNORE_PATTERNS'])); cfg.set('logs', 'loglevel', str(c['LOG_LEVEL']))
        with open('config.ini', 'w') as f: cfg.write(f)
        if c['SERVER_TYPE'] == 'plex': scanner_instance.connect_to_plex(retry=False); scanner_instance.get_library_ids()
        return {"status": "success"}
    except Exception as e: return JSONResponse({"error": str(e)}, status_code=500)

@app.post("/api/restart")
async def restart_system(u: str = Depends(get_current_user)):
    import threading; threading.Thread(target=lambda: (time.sleep(1), os._exit(0))).start()
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
            # Search within specific allowed path
            rp = pathlib.Path(path).resolve()
            allowed = False
            for s in sp:
                bp = pathlib.Path(s).resolve()
                if rp == bp or bp in rp.parents: allowed = True; break
            if allowed: search_roots = [str(rp)]
        else:
            # Global search in all media roots
            search_roots = sp

        def perform_search():
            matches = []
            for root in search_roots:
                for r, dirs, files in os.walk(root):
                    # Check for matches in both dirs and files
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
                            except: continue
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
                    stat = e.stat()
                    it.append({
                        "name": e.name, "path": e.path, "is_dir": e.is_dir(),
                        "size": stat.st_size if e.is_file() else 0,
                        "size_fmt": fmt_size(stat.st_size) if e.is_file() else "",
                        "date": datetime.fromtimestamp(stat.st_mtime).strftime('%Y-%m-%d %H:%M'),
                        "ext": os.path.splitext(e.name)[1].lower() if e.is_file() else None
                    })
        it.sort(key=lambda x: (not x.get('is_back'), not x.get('is_dir'), x['name'].lower()))
        return {"current_path": str(rp), "items": it}
    except: return JSONResponse({"error": "fail"}, status_code=500)

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
    except: return JSONResponse({"error": "invalid"}, status_code=400)
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
    elif a == 'health' and os.path.isfile(p):
        _, res = await asyncio.get_event_loop().run_in_executor(None, scanner_instance.check_file_health, p)
        return {"status": "success", "data": res}
    return {"error": "unknown"}

@app.post("/api/test-webhook")
async def test_webhook(data: dict, u: str = Depends(get_current_user)):
    url = data.get('url')
    if not url: return JSONResponse({"error": "No URL provided"}, status_code=400)
    real_url = scanner_instance.config.get('DISCORD_WEBHOOK_URL', '')
    if url == mask_s(real_url): url = real_url
    if not url.startswith("http"): return JSONResponse({"error": "Invalid URL"}, status_code=400)
    try:
        r = requests.post(url, json={"content": "✅ **Omniscan Test Message**\nYour notification configuration is working correctly!"}, timeout=5)
        r.raise_for_status()
        return {"status": "success"}
    except Exception as e: return JSONResponse({"error": str(e)}, status_code=400)

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
            r = requests.get(f"{url}/System/Info", headers=h, timeout=5); r.raise_for_status()
            return {"status": "success", "message": "Online", "server": st.capitalize()}
    except Exception as e: return JSONResponse({"status": "error", "message": str(e)}, status_code=400)

@app.post("/api/webhook")
async def webhook_trigger(request: Request):
    if not scanner_instance: return JSONResponse({"error": "init"}, status_code=500)
    try:
        data = await request.json()
        logger.info(f"Webhook received: {data.keys()}")
        
        paths_to_scan = set()
        
        # 1. Generic 'path' or 'paths'
        if 'path' in data: paths_to_scan.add(data['path'])
        if 'paths' in data and isinstance(data['paths'], list): paths_to_scan.update(data['paths'])
        
        # 2. Sonarr/Radarr (Grab/Download/Rename)
        # Movie
        if 'movie' in data and 'folderPath' in data['movie']: paths_to_scan.add(data['movie']['folderPath'])
        if 'movieFile' in data and 'path' in data['movieFile']: paths_to_scan.add(data['movieFile']['path'])
        
        # Series
        if 'series' in data and 'path' in data['series']: paths_to_scan.add(data['series']['path'])
        if 'episodeFile' in data and 'path' in data['episodeFile']: paths_to_scan.add(data['episodeFile']['path'])
        
        # Rename (source/dest)
        if 'sourcePath' in data: paths_to_scan.add(data['sourcePath'])
        if 'destPath' in data: paths_to_scan.add(data['destPath'])

        if not paths_to_scan:
            return JSONResponse({"status": "ignored", "message": "No paths found in payload"}, status_code=200)

        triggered = 0
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
                    scanner_instance.submit_file_event('created', p)
                    triggered += 1
                elif os.path.isdir(p):
                    lid, _, _ = scanner_instance.get_library_id_for_path(p)
                    if lid:
                        scanner_instance.trigger_scan(lid, p)
                        triggered += 1
                    else:
                        logger.warning(f"Webhook path not in library: {p}")
            else:
                # If path doesn't exist, try falling back to parent folder
                parent = os.path.dirname(p)
                lid, _, _ = scanner_instance.get_library_id_for_path(p)
                
                # Only fallback if parent exists AND is not the library root
                if os.path.isdir(parent) and not scanner_instance.is_library_root(lid, parent):
                    logger.info(f"Webhook path missing, falling back to parent: {parent}")
                    if lid:
                        scanner_instance.trigger_scan(lid, parent)
                        triggered += 1
                else:
                    logger.warning(f"Webhook path does not exist: {p}")

        return {"status": "success", "triggered": triggered}
        
    except Exception as e:
        logger.error(f"Webhook error: {e}")
        return JSONResponse({"error": str(e)}, status_code=400)

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    if not request.session.get("user"): return RedirectResponse(url="/login", status_code=status.HTTP_302_FOUND)
    return load_template("index.html")

def run_web_server(scanner, host="0.0.0.0", port=8000):
    import uvicorn
    set_scanner(scanner)
    uvicorn.run(app, host=host, port=port, log_level="error")
