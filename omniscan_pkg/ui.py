from nicegui import ui, app as nicegui_app
from fastapi import Request
from fastapi.responses import RedirectResponse
import os
import time
import json
import pathlib
import requests
import configparser
import logging
import asyncio
from .config import get_webhook_token
from datetime import datetime
from collections import defaultdict
from plexapi.server import PlexServer

logger = logging.getLogger(__name__)

def apply_theme():
    ui.dark_mode().enable()
    ui.add_head_html('''
        <link href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css" rel="stylesheet">
        <style>
            @import url('https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;500;600;700;800;900&display=swap');
            
            body { 
                background-color: #030712 !important; 
                color: #f3f4f6; 
                font-family: 'Outfit', sans-serif !important; 
            }
            
            .glass-card {
                background: rgba(17, 24, 39, 0.45) !important;
                backdrop-filter: blur(16px);
                -webkit-backdrop-filter: blur(16px);
                border: 1px solid rgba(255, 255, 255, 0.04) !important;
                box-shadow: 0 4px 30px rgba(0, 0, 0, 0.4);
                transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
                /* Kill Quasar's default centering */
                align-items: flex-start !important;
            }

            /* Quasar card section default padding reset */
            .glass-card > .q-card__section {
                padding: 0 !important;
                align-items: flex-start !important;
            }

            /* Browser list row items — never center */
            .browser-row {
                display: flex;
                align-items: center;
                justify-content: space-between;
                width: 100%;
                background: rgba(17, 24, 39, 0.45);
                border: 1px solid rgba(255, 255, 255, 0.04);
                border-radius: 0.75rem;
                padding: 10px 16px;
                transition: background 0.2s;
                box-sizing: border-box;
            }
            .browser-row:hover {
                background: rgba(255,255,255,0.04);
                border-color: rgba(6, 182, 212, 0.2);
            }
            
            ::-webkit-scrollbar {
                width: 6px;
                height: 6px;
            }
            ::-webkit-scrollbar-track {
                background: #030712;
            }
            ::-webkit-scrollbar-thumb {
                background: #1f2937;
                border-radius: 9999px;
            }
            ::-webkit-scrollbar-thumb:hover {
                background: #374151;
            }
        </style>
    ''')

# Global check logic
def is_setup_completed(scanner):
    if not scanner: return False
    config_pass = scanner.config.get('WEB_PASSWORD')
    return bool(config_pass and config_pass.strip())

def get_gateway():
    try:
        with open("/proc/net/route") as f:
            for line in f:
                fields = line.strip().split()
                if len(fields) > 2 and fields[1] == '00000000': # Default route
                    import socket
                    import struct
                    return socket.inet_ntoa(struct.pack("<L", int(fields[2], 16)))
    except Exception:
        pass
    return None

async def discover_media_server():
    async def check_emby_jellyfin(url):
        try:
            def request_func():
                return requests.get(f"{url}/System/Info/Public", timeout=0.8, allow_redirects=False)
            r = await asyncio.get_event_loop().run_in_executor(None, request_func)
            if r.status_code != 200:
                def request_root():
                    return requests.get(f"{url}/", timeout=0.8, allow_redirects=False)
                r = await asyncio.get_event_loop().run_in_executor(None, request_root)
            
            server_id = None
            stype = 'jellyfin'
            server_header = r.headers.get('Server', '').lower()
            headers_str = r.headers.get('Access-Control-Allow-Headers', '').lower()
            is_emby = ('emby' in server_header or 'emby' in url.lower() or 'emby' in headers_str or 'upnp/1.0 dlnadoc' in server_header)
            
            if r.status_code == 200:
                try:
                    data = r.json()
                    server_id = data.get('Id') or data.get('id')
                    version = data.get('Version', '')
                    if version.startswith('10.'):
                        stype = 'jellyfin'
                    elif version.startswith('3.') or version.startswith('4.'):
                        stype = 'emby'
                    else:
                        stype = 'emby' if is_emby else 'jellyfin'
                except Exception:
                    stype = 'emby' if is_emby else 'jellyfin'
            elif r.status_code in (301, 302):
                loc = r.headers.get('Location', '')
                if 'web/index.html' in loc or 'web/' in loc:
                    stype = 'emby' if ('emby' in loc.lower() or is_emby) else 'jellyfin'
            else:
                if 'x-emby-token' in headers_str or 'x-mediabrowser-token' in headers_str:
                    stype = 'emby' if is_emby else 'jellyfin'
            return stype, url, server_id
        except Exception:
            pass
        return None

    async def check_plex(url):
        try:
            def request_func():
                return requests.get(f"{url}/identity", timeout=0.8)
            r = await asyncio.get_event_loop().run_in_executor(None, request_func)
            if r.status_code == 200:
                import re
                server_id = None
                match = re.search(r'machineIdentifier="([^"]+)"', r.text)
                if match:
                    server_id = match.group(1)
                return 'plex', url, server_id
        except Exception:
            pass
        return None

    import socket
    hostnames = ['localhost', '127.0.0.1', 'plex', 'jellyfin', 'emby', 'embyserver', 'plex-server', 'jellyfin-server']
    
    gw = get_gateway()
    if gw and gw not in hostnames:
        hostnames.append(gw)
        
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        local_ip = s.getsockname()[0]
        s.close()
        if local_ip and (local_ip.startswith('172.') or local_ip.startswith('10.') or local_ip.startswith('192.168.')):
            parts = local_ip.split('.')
            if len(parts) == 4:
                prefix = f"{parts[0]}.{parts[1]}.{parts[2]}."
                for i in range(1, 41):
                    ip = f"{prefix}{i}"
                    if ip != local_ip:
                        hostnames.append(ip)
    except Exception:
        pass

    unique_hosts = []
    seen = set()
    for h in hostnames:
        if h not in seen:
            seen.add(h)
            unique_hosts.append(h)
            
    tasks = []
    for host in unique_hosts:
        tasks.append(check_plex(f'http://{host}:32400'))
        tasks.append(check_emby_jellyfin(f'http://{host}:8096'))
        
    results = await asyncio.gather(*tasks)
    
    best_by_url = {}
    for res in results:
        if res:
            stype, url, server_id = res
            if url not in best_by_url or (server_id and not best_by_url[url][2]):
                best_by_url[url] = (stype, url, server_id)
                
    groups = {}
    for stype, url, server_id in best_by_url.values():
        import urllib.parse
        parsed = urllib.parse.urlparse(url)
        netloc = parsed.netloc or url
        key = (stype, server_id) if server_id else (stype, netloc)
        if key not in groups:
            groups[key] = []
        groups[key].append(url)
            
    def url_priority(url):
        import urllib.parse
        parsed = urllib.parse.urlparse(url)
        host = parsed.hostname or ''
        if any(k in host.lower() for k in ['plex', 'jellyfin', 'emby', 'embyserver']):
            return 1
        if host in ('localhost', '127.0.0.1'):
            return 2
        is_ip = False
        try:
            socket.inet_aton(host)
            is_ip = True
        except Exception:
            pass
        if not is_ip:
            return 3
        if host.startswith('172.18.') or host.startswith('172.17.'):
            if not host.endswith('.1'):
                return 4
        return 5

    discovered = []
    validated = []
    unvalidated = []
    for (stype, server_id), urls in groups.items():
        urls.sort(key=lambda u: (url_priority(u), len(u)))
        best_url = urls[0]
        if isinstance(server_id, str) and len(server_id) > 5:
            validated.append((stype, best_url, server_id))
        else:
            unvalidated.append((stype, best_url, None))
            
    validated_types = set()
    for stype, url, server_id in validated:
        discovered.append((stype, url))
        validated_types.add(stype)
        
    for stype, url, _ in unvalidated:
        if stype not in validated_types:
            discovered.append((stype, url))
            
    return discovered

def auto_cancel(timer):
    from contextlib import nullcontext
    orig_get_context = timer._get_context
    def safe_get_context():
        try:
            return orig_get_context()
        except RuntimeError as e:
            if 'The parent slot of the element has been deleted' in str(e):
                timer.cancel()
                return nullcontext()
            raise
    timer._get_context = safe_get_context

    try:
        client = ui.context.client
        client.on_disconnect(lambda *_: timer.cancel())
    except Exception:
        pass
    return timer


def check_auth(request: Request, scanner):
    if not is_setup_completed(scanner):
        return RedirectResponse('/setup')
    if not request.session.get("user"):
        return RedirectResponse('/login')
    return None

def verify_credentials(scanner, username, password):
    if not scanner: return False
    config_user = scanner.config.get('WEB_USERNAME', 'admin')
    config_pass = scanner.config.get('WEB_PASSWORD')
    if not config_pass: return False
    import secrets
    return secrets.compare_digest(username, config_user) and secrets.compare_digest(password, config_pass)

def mask_s(v): return (v[:4] + "****" + v[-4:]) if v and len(v) >= 8 else "********"
def unmask_v(n, r): return r if n == mask_s(r) else n

def fmt_size(size):
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size < 1024: return f"{size:.1f} {unit}"
        size /= 1024
    return f"{size:.1f} PB"

def add_menu(active_tab):
    with ui.left_drawer(value=True).classes('bg-[#090d16]/80 border-r border-white/5 backdrop-blur-xl w-64 flex flex-col'):
        ui.label('Omniscan').classes('text-white text-2xl font-black p-8 tracking-tight bg-gradient-to-r from-cyan-400 to-blue-500 bg-clip-text text-transparent')
        
        menu_items = [
            ('Dashboard', '/', 'fa-chart-pie'),
            ('Browser', '/browser', 'fa-folder-open'),
            ('Logs', '/logs', 'fa-terminal'),
            ('Settings', '/settings', 'fa-sliders')
        ]
        
        with ui.column().classes('w-full px-4 gap-2'):
            for label, url, icon in menu_items:
                is_active = url == active_tab
                bg_cls = 'bg-white/5 text-white border-l-4 border-cyan-500 font-bold' if is_active else 'text-slate-400 hover:bg-white/5 hover:text-white border-l-4 border-transparent'
                with ui.link(target=url).classes(f'w-full flex items-center gap-4 p-3 rounded-xl transition-all {bg_cls}'):
                    ui.html(f'<i class="fas {icon} text-sm"></i>')
                    ui.label(label).classes('font-bold text-sm')
        
        # Add Logout button
        # Redirecting to /logout route (defined in web.py)
        with ui.row().classes('w-full mt-auto p-4'):
            ui.button('Logout', on_click=lambda: ui.navigate.to('/logout')).classes('w-full bg-red-950/20 text-red-400 p-3 hover:bg-red-950/40 rounded-xl border border-red-500/10 font-bold uppercase tracking-wider text-xs')

def create_card(title, value, color='text-white', icon='', link=None):
    card_classes = 'glass-card p-6 rounded-2xl flex flex-col justify-between items-start grow min-w-[200px]'
    if link:
        card_classes += ' cursor-pointer hover:bg-white/10 transition-all border border-cyan-500/20'
    with ui.card().classes(card_classes) as card:
        if link:
            card.on('click', lambda: ui.navigate.to(link))
        with ui.row().classes('w-full justify-between items-center mb-2'):
            ui.label(title).classes('text-slate-400 text-[10px] font-black uppercase tracking-widest')
            if icon:
                ui.html(f'<i class="fas {icon} text-slate-500 text-sm"></i>')
        ui.label(value).classes(f'text-3xl font-extrabold {color}')

def create_connection_banner(scanner):
    c = scanner.config
    st = c.get('SERVER_TYPE', 'plex')
    connected = False
    details = ""
    if st == 'plex':
        if scanner.plex:
            connected = True
            details = f"Connected to Plex: {scanner.plex.friendlyName}"
        else:
            details = "Plex Connection Offline"
    else:
        url = c.get('SERVER_URL')
        if url:
            connected = True
            details = f"Connected to {st.capitalize()}: {url}"
        else:
            details = f"{st.capitalize()} Connection Offline"
            
    badge_color = 'bg-emerald-500/10 text-emerald-400 border border-emerald-500/20' if connected else 'bg-rose-500/10 text-rose-400 border border-rose-500/20'
    with ui.row().classes('w-full justify-between items-center bg-[#0d1527]/50 border border-white/5 p-4 rounded-2xl mb-8 backdrop-blur-md'):
        with ui.row().classes('items-center gap-3'):
            ui.html(f'<div class="w-3 h-3 rounded-full {"bg-emerald-400 animate-pulse" if connected else "bg-rose-400"}"></div>')
            ui.label(details).classes('text-sm font-semibold text-slate-300')
        with ui.row().classes('gap-3'):
            ui.label(st.upper()).classes(f'px-3 py-1 rounded-full text-[10px] font-black uppercase tracking-wider {badge_color}')

def init_ui(app, scanner):
    
    @ui.page('/')
    async def index(request: Request):
        if r := check_auth(request, scanner): return r
        apply_theme()
        
        add_menu('/')
        
        with ui.column().classes('w-full p-8 md:p-10'):
            create_connection_banner(scanner)
            
            ui.label('Dashboard Overview').classes('text-3xl font-black text-white mb-6')
            
            # Statistics grid
            with ui.row().classes('w-full gap-6 flex-wrap mb-8'):
                watched_paths_count = str(len(scanner.config.get('SCAN_PATHS', [])))
                create_card('Watching Paths', watched_paths_count, 'text-cyan-400', 'fa-folder-tree')
                
                active_queue_count = str(len(scanner.pending_scans))
                create_card('Active Queue', active_queue_count, 'text-amber-400', 'fa-hourglass-half')
                
                corrupt_count = str(scanner.history.get_corrupt_count())
                create_card('Corrupt Media', corrupt_count, 'text-rose-400', 'fa-triangle-exclamation')
                
                missing_count = str(sum(scanner.library_missing_counts.values()))
                create_card('Missing Files', missing_count, 'text-yellow-400', 'fa-magnifying-glass-minus', link='/browser?show=missing')

                stuck_count = str(scanner.history.get_truly_stuck_count())
                create_card('Stuck Files', stuck_count, 'text-rose-400', 'fa-circle-exclamation', link='/browser?show=stuck')

            # Scanner Control & Active queue
            with ui.row().classes('w-full gap-6 flex-wrap mb-8 items-stretch'):
                # Control Card
                with ui.card().classes('glass-card p-6 rounded-2xl grow min-w-[300px] flex flex-col gap-4'):
                    with ui.column().classes('gap-1'):
                        ui.label('Scanner Controls').classes('text-sm font-bold text-cyan-400 uppercase tracking-wider')
                        status_lbl = ui.label('Scanner Status: ' + ('SCANNING...' if scanner.is_scanning else 'IDLE')).classes('text-sm font-semibold text-slate-300')
                    
                    ui.space()
                    
                    with ui.row().classes('w-full items-center gap-4'):
                        force_full_checkbox = ui.checkbox('Force Full Scan (Bypass 2-day limit)').classes('text-xs font-semibold text-slate-400')
                    with ui.row().classes('w-full gap-4'):
                        async def handle_scan_all():
                            if scanner.is_scanning:
                                ui.notify('Scanner is currently busy', type='warning')
                                return
                            is_full = force_full_checkbox.value
                            ui.notify(('Full Sweep Scan Started' if is_full else 'Incremental Scan Started') + ' in background', type='info')
                            import threading
                            threading.Thread(target=scanner.run_scan, args=(is_full,), daemon=True).start()
                            status_lbl.text = 'Scanner Status: SCANNING...'
                            
                        async def handle_clear_stuck():
                            if scanner.history.clear_all_stuck():
                                ui.notify('Cleared all stuck files from database', type='positive')
                            else:
                                ui.notify('Failed to clear stuck files', type='negative')
                                
                        ui.button('Scan All Libraries', on_click=handle_scan_all).classes('bg-gradient-to-r from-blue-600 to-indigo-600 hover:from-blue-500 hover:to-indigo-500 text-white font-bold px-6 py-2 rounded-xl text-xs uppercase tracking-wider')
                        ui.button('Clear Stuck Queue', on_click=handle_clear_stuck).classes('bg-red-950/20 text-red-400 hover:bg-red-950/40 rounded-xl border border-red-500/10 font-bold px-6 py-2 text-xs uppercase tracking-wider')

                # Webhook URL Card
                with ui.card().classes('glass-card p-6 rounded-2xl grow min-w-[300px] flex flex-col gap-4'):
                    with ui.column().classes('gap-3 w-full'):
                        ui.label('Arr Webhook URL').classes('text-sm font-bold text-cyan-400 uppercase tracking-wider')
                        
                        scheme = request.headers.get('x-forwarded-proto', request.url.scheme)
                        base_url = str(request.base_url).rstrip('/')
                        if scheme == 'https' and base_url.startswith('http://'):
                            base_url = base_url.replace('http://', 'https://', 1)
                        token = get_webhook_token(scanner.config.get('WEB_PASSWORD', 'admin'))
                        webhook_url = f"{base_url}/api/webhook?apikey={token}"
                        
                        with ui.row().classes('w-full items-center gap-2 bg-slate-950/40 p-2 rounded-xl border border-white/5'):
                            ui.input(value=webhook_url).props('readonly borderless').classes('grow text-xs font-mono text-cyan-400 bg-transparent')
                            
                            def copy_url_dash():
                                ui.run_javascript(f"navigator.clipboard.writeText('{webhook_url}')")
                                ui.notify('Webhook URL copied to clipboard!', type='positive')
                                
                            ui.button(icon='content_copy', on_click=copy_url_dash).props('flat round dense').classes('text-slate-400 hover:text-white')
                    
                    ui.space()
                    
                    ui.markdown(
                        "Add this URL to **Sonarr/Radarr** -> **Settings** -> **Connect** -> **Webhook** (using POST method on import) to trigger instant scans."
                    ).classes('text-[11px] text-slate-400 leading-relaxed')



            # Libraries list section
            ui.label('Media Server Libraries').classes('text-xl font-bold text-white mb-4')
            with ui.row().classes('w-full gap-6 flex-wrap mb-8'):
                if not scanner.library_sections_cache:
                    with ui.card().classes('glass-card p-6 rounded-2xl w-full text-center'):
                        ui.label('No library sections found or Plex disconnected').classes('text-sm text-slate-500 font-semibold')
                else:
                    for lib in scanner.library_sections_cache:
                        lid = lib['id']
                        title = lib['title']
                        ltype = lib['type']
                        paths = lib['locations']
                        
                        missing = scanner.library_missing_counts.get(lid, 0)
                        count = scanner.library_counts.get(lid, len(scanner.library_files.get(lid, [])))
                        
                        with ui.card().classes('glass-card p-6 rounded-2xl min-w-[280px] flex flex-col justify-between grow'):
                            with ui.row().classes('w-full justify-between items-center mb-3'):
                                with ui.column().classes('gap-0'):
                                    ui.label(title).classes('text-md font-bold text-white')
                                    ui.label(ltype.capitalize()).classes('text-[10px] text-slate-500 font-black uppercase')
                                ui.html('<i class="fas ' + ('fa-film' if ltype == 'movie' else 'fa-tv' if ltype == 'show' else 'fa-music') + ' text-slate-600 text-sm"></i>')
                            
                            with ui.column().classes('gap-1 mb-4'):
                                ui.label(f'Files Indexed: {count}').classes('text-xs font-semibold text-slate-300')
                                ui.label(f'Missing Files: {missing}').classes(f'text-xs font-bold {"text-yellow-400" if missing > 0 else "text-slate-500"}')
                                with ui.column().classes('gap-0 mt-1'):
                                    for p in paths:
                                        ui.label(p).classes('text-[9px] text-slate-500 font-mono truncate max-w-xs')
                            
                            async def trigger_lib_scan(lib_id=lid, paths=paths):
                                ui.notify(f'Triggered scan for library: {title}', type='info')
                                for loc in paths:
                                    scanner.trigger_scan(lib_id, loc, force=True)
                                    
                            ui.button('Scan Library', on_click=trigger_lib_scan).classes('w-full bg-cyan-600/10 hover:bg-cyan-600 text-cyan-400 hover:text-white rounded-xl border border-cyan-500/20 font-bold text-xs uppercase tracking-wider p-2')

            # History / Events Table
            with ui.row().classes('w-full items-center justify-between mb-4'):
                ui.label('Recent Events Log').classes('text-xl font-bold text-white')
                search_field = ui.input('Search...').classes('w-48').props('outlined dense')

            with ui.card().classes('glass-card p-6 rounded-2xl w-full'):
                PAGE_SIZE = 10
                page_state = {'page': 0}

                columns = [
                    {'name': 'timestamp', 'label': 'Timestamp', 'field': 'timestamp', 'align': 'left', 'sortable': True},
                    {'name': 'event_type', 'label': 'Event', 'field': 'event_type', 'align': 'left', 'sortable': True},
                    {'name': 'details', 'label': 'Details', 'field': 'details', 'align': 'left'},
                    {'name': 'status', 'label': 'Status', 'field': 'status', 'align': 'left'},
                ]

                table = ui.table(columns=columns, rows=[], row_key='timestamp').classes('w-full bg-transparent border-none text-slate-300')
                table.props('flat dark bordered')

                # Pagination controls
                with ui.row().classes('w-full items-center justify-between mt-4'):
                    page_label = ui.label('Page 1').classes('text-xs text-slate-500 font-semibold')
                    with ui.row().classes('gap-2'):
                        prev_btn = ui.button(icon='chevron_left', on_click=lambda: go_page(-1)).props('flat round dense').classes('text-slate-400 hover:text-white')
                        next_btn = ui.button(icon='chevron_right', on_click=lambda: go_page(+1)).props('flat round dense').classes('text-slate-400 hover:text-white')

                def load_history_data():
                    try:
                        q = search_field.value or ''
                        offset = page_state['page'] * PAGE_SIZE
                        events = scanner.history.get_history(limit=PAGE_SIZE, offset=offset, search=q if q else None)
                        rows = [{
                            'timestamp': e[0],
                            'event_type': e[1],
                            'details': e[2],
                            'status': e[3]
                        } for e in events]
                        table.rows = rows
                        # Update label & button states
                        page_label.text = f'Page {page_state["page"] + 1}'
                        prev_btn.set_enabled(page_state['page'] > 0)
                        next_btn.set_enabled(len(events) == PAGE_SIZE)
                    except RuntimeError:
                        pass

                def go_page(delta):
                    page_state['page'] = max(0, page_state['page'] + delta)
                    load_history_data()

                def on_search(_):
                    page_state['page'] = 0
                    load_history_data()

                load_history_data()
                search_field.on_value_change(on_search)
                auto_cancel(ui.timer(5.0, load_history_data))

    @ui.page('/browser')
    async def browser(request: Request):
        if r := check_auth(request, scanner): return r
        apply_theme()
        add_menu('/browser')
        
        with ui.column().classes('w-full p-8 md:p-10 gap-0 items-start'):
            # ─── Browser State ────────────────────────────────────────────────
            show_param = request.query_params.get('show', '')
            show_missing_init = show_param == 'missing'
            show_stuck_init   = show_param == 'stuck'
            state = {
                'current_path':    '',
                'search_query':    '',
                'show_missing_only': show_missing_init,
                'show_stuck_only':   show_stuck_init,
                'selected_paths':    set()
            }

            # ─── Header Row ───────────────────────────────────────────────────
            with ui.row().classes('w-full items-center justify-between mb-6 flex-wrap gap-4'):
                with ui.column().classes('gap-1 items-start'):
                    ui.label('Media Browser').classes('text-3xl font-black text-white')
                    ui.label('Browse, search and trigger scans on your media directories').classes('text-xs text-slate-500')

                # Filter pills
                with ui.row().classes('gap-2 items-center flex-wrap'):
                    missing_count_val = sum(scanner.library_missing_counts.values())
                    stuck_count_val   = scanner.history.get_truly_stuck_count()

                    def pill_cls(active):
                        return ('px-4 py-1.5 rounded-full text-xs font-bold uppercase tracking-wider border transition-all cursor-pointer '
                                + ('bg-yellow-400/10 text-yellow-400 border-yellow-500/30' if active else
                                   'bg-white/5 text-slate-400 border-white/5 hover:border-white/10 hover:text-white'))

                    all_pill     = ui.button('All Files',      on_click=lambda: set_filter('all')).classes(pill_cls(not show_missing_init and not show_stuck_init))
                    missing_pill = ui.button(f'Missing  ({missing_count_val})', on_click=lambda: set_filter('missing')).classes(pill_cls(show_missing_init))
                    stuck_pill   = ui.button(f'Stuck  ({stuck_count_val})',   on_click=lambda: set_filter('stuck')).classes(pill_cls(show_stuck_init))

            # ─── Search + Breadcrumb ─────────────────────────────────────────
            search_input  = ui.input(placeholder='Search files & folders...').classes('w-full mb-4').props('outlined dense').style('font-size:13px')
            breadcrumb_row = ui.row().classes('w-full gap-1 items-center mb-4 text-xs font-semibold text-slate-400 flex-wrap')

            files_container = ui.column().classes('w-full gap-2 items-start')

            # ─── Filter helpers ───────────────────────────────────────────────
            def _apply_filter(mode):
                """Switch the active filter mode: 'all', 'missing', 'stuck'."""
                state['show_missing_only'] = mode == 'missing'
                state['show_stuck_only']   = mode == 'stuck'
                state['selected_paths'] = set()
                if mode != 'all':
                    state['current_path'] = ''
                    search_input.value = ''
                    search_input.disable()
                    breadcrumb_row.set_visibility(False)
                else:
                    search_input.enable()
                    breadcrumb_row.set_visibility(True)
                # Update pill styles
                all_pill.classes(replace=pill_cls(mode == 'all'))
                missing_pill.classes(replace=pill_cls(mode == 'missing'))
                stuck_pill.classes(replace=pill_cls(mode == 'stuck'))
                render_browser()

            def set_filter(mode):
                _apply_filter(mode)

            # Apply initial state
            if show_missing_init:
                search_input.disable()
                breadcrumb_row.set_visibility(False)
            elif show_stuck_init:
                search_input.disable()
                breadcrumb_row.set_visibility(False)

            # ─── Render ───────────────────────────────────────────────────────
            def render_browser():
                breadcrumb_row.clear()
                files_container.clear()

                paths_to_check = scanner.config.get('SCAN_PATHS', [])

                # Breadcrumb
                if state['current_path']:
                    with breadcrumb_row:
                        async def click_root():
                            state['current_path'] = ''
                            render_browser()
                        ui.button('Root', on_click=click_root).classes(
                            'text-cyan-400 p-0 min-h-0 bg-transparent shadow-none hover:text-white text-xs font-bold uppercase')
                        parts = state['current_path'].strip('/').split('/')
                        current_acc = '/'
                        for idx, p in enumerate(parts):
                            current_acc = os.path.join(current_acc, p)
                            ui.html('<span class="text-slate-600 mx-1">/</span>')
                            async def click_part(path_val=current_acc):
                                state['current_path'] = path_val
                                render_browser()
                            ui.button(p, on_click=click_part).classes(
                                'text-cyan-400 p-0 min-h-0 bg-transparent shadow-none hover:text-white text-xs font-bold')

                # Gather items
                q = (search_input.value or '').strip()

                if state['show_missing_only']:
                    items = []
                    with scanner.library_files_lock:
                        for lib_id, missing_paths in scanner.library_missing_files.items():
                            for p in missing_paths:
                                items.append({'name': os.path.basename(p), 'path': p,
                                              'is_dir': False, 'size_fmt': '', 'extra': None, 'mode': 'missing'})
                    items.sort(key=lambda x: x['path'])

                elif state['show_stuck_only']:
                    stuck_rows = scanner.history.get_truly_stuck()
                    items = []
                    for s_path, s_attempts, s_last in stuck_rows:
                        items.append({'name': os.path.basename(s_path), 'path': s_path,
                                      'is_dir': False, 'size_fmt': '',
                                      'extra': {'attempts': s_attempts, 'last_seen': s_last}, 'mode': 'stuck'})
                    items.sort(key=lambda x: x['path'])

                elif not state['current_path'] and not q:
                    # Root view
                    with files_container:
                        for p in paths_to_check:
                            with ui.card().classes('glass-card p-4 rounded-xl w-full flex justify-between items-center'):
                                with ui.row().classes('items-center gap-3 grow cursor-pointer'):
                                    async def open_root(p_val=p):
                                        state['current_path'] = p_val
                                        render_browser()
                                    ui.html('<i class="fas fa-hard-drive text-cyan-400 text-lg"></i>')
                                    with ui.column().classes('gap-0'):
                                        ui.label(p).classes('text-sm font-bold text-white').on('click', open_root)
                                        ui.label('Root directory').classes('text-[10px] text-slate-500')
                                with ui.row().classes('gap-2'):
                                    async def scan_root(p_val=p):
                                        ui.notify(f'Scanning: {p_val}', type='info')
                                        scanner.scan_folder_async(p_val, force_full=False)
                                    async def sweep_root(p_val=p):
                                        ui.notify(f'Full Sweep started for: {p_val}', type='info')
                                        scanner.scan_folder_async(p_val, force_full=True)
                                    ui.button('Scan', on_click=scan_root).classes(
                                        'bg-cyan-600/10 hover:bg-cyan-600 text-cyan-400 hover:text-white '
                                        'rounded-xl border border-cyan-500/20 font-bold text-xs px-4 py-2')
                                    ui.button('Full Sweep', on_click=sweep_root).classes(
                                        'bg-indigo-600/10 hover:bg-indigo-600 text-indigo-400 hover:text-white '
                                        'rounded-xl border border-indigo-500/20 font-bold text-xs px-4 py-2')
                    return

                elif q:
                    search_query = q.lower()
                    results = []
                    for root in paths_to_check:
                        for r, dirs, files in os.walk(root):
                            for name in dirs + files:
                                if search_query in name.lower():
                                    full_path = os.path.join(r, name)
                                    results.append({
                                        'name': name, 'path': full_path,
                                        'is_dir': os.path.isdir(full_path),
                                        'size_fmt': fmt_size(os.path.getsize(full_path)) if os.path.isfile(full_path) else '',
                                        'extra': None, 'mode': 'normal'
                                    })
                                if len(results) >= 50: break
                            if len(results) >= 50: break
                        if len(results) >= 50: break
                    items = results

                else:
                    # Directory listing
                    curr = state['current_path']
                    items = []
                    try:
                        curr_parent  = str(pathlib.Path(curr).parent)
                        roots_paths  = [str(pathlib.Path(p).resolve()) for p in paths_to_check]
                        parent_path  = curr_parent if curr not in roots_paths else ''
                        if parent_path:
                            async def click_back():
                                state['current_path'] = parent_path
                                render_browser()
                            with files_container:
                                with ui.card().classes('glass-card p-3 rounded-xl w-full flex items-center gap-3 cursor-pointer hover:bg-white/5'):
                                    ui.html('<i class="fas fa-turn-up text-slate-500 text-sm"></i>')
                                    ui.label('.. Parent Directory').classes('text-xs font-bold text-slate-400 hover:text-slate-200').on('click', click_back)
                        with os.scandir(curr) as scan_items:
                            for e in scan_items:
                                if not e.name.startswith('.'):
                                    try:
                                        stat = e.stat()
                                        items.append({
                                            'name': e.name, 'path': e.path,
                                            'is_dir': e.is_dir(),
                                            'size_fmt': fmt_size(stat.st_size) if e.is_file() else '',
                                            'extra': None, 'mode': 'normal'
                                        })
                                    except Exception: pass
                        items.sort(key=lambda x: (not x['is_dir'], x['name'].lower()))
                    except Exception as ex:
                        ui.notify(f'Failed to read folder: {ex}', type='negative')
                        return

                # ─── Render item list ─────────────────────────────────────────
                with files_container:
                    if not items:
                        with ui.column().classes('w-full items-center py-16 gap-3'):
                            ui.html('<i class="fas fa-folder-open text-slate-700 text-5xl"></i>')
                            ui.label('Nothing here').classes('text-slate-500 font-semibold text-sm')
                            if state['show_missing_only']:
                                ui.label('No files missing from your media server — great!').classes('text-xs text-slate-600')
                            elif state['show_stuck_only']:
                                ui.label('No stuck files in the queue — all clear!').classes('text-xs text-slate-600')
                        return

                    # Summary bar
                    mode_label = ('Missing Files' if state['show_missing_only'] else
                                  'Stuck Files'   if state['show_stuck_only'] else
                                  f'Search results for "{q}"' if q else
                                  os.path.basename(state['current_path']) or 'Root')
                    with ui.row().classes('w-full items-center justify-between mb-3 px-1'):
                        ui.label(f'{mode_label}').classes('text-xs font-black text-slate-400 uppercase tracking-widest')
                        ui.label(f'{len(items)} item{"s" if len(items) != 1 else ""}').classes('text-xs text-slate-600')

                    # Bulk actions row (only for missing/stuck modes)
                    if (state['show_missing_only'] or state['show_stuck_only']) and items:
                        file_paths = [it['path'] for it in items if not it['is_dir']]
                        if file_paths:
                            selected_in_view = state['selected_paths'] & set(file_paths)
                            all_selected = len(file_paths) > 0 and len(selected_in_view) == len(file_paths)
                            is_disabled = len(selected_in_view) == 0

                            with ui.card().classes('glass-card p-3 rounded-xl w-full mb-3'):
                                with ui.row().classes('w-full items-center justify-between flex-wrap gap-3'):
                                    with ui.row().classes('items-center gap-2'):
                                        async def on_select_all_change(e):
                                            any_change = False
                                            if e.value:
                                                for p in file_paths:
                                                    if p not in state['selected_paths']:
                                                        state['selected_paths'].add(p)
                                                        any_change = True
                                            else:
                                                for p in file_paths:
                                                    if p in state['selected_paths']:
                                                        state['selected_paths'].discard(p)
                                                        any_change = True
                                            if any_change:
                                                render_browser()

                                        ui.checkbox('Select All', value=all_selected, on_change=on_select_all_change).classes('text-xs font-bold text-slate-300')
                                        ui.label(f'({len(selected_in_view)} of {len(file_paths)} selected)').classes('text-xs text-slate-500 font-semibold')

                                    with ui.row().classes('gap-2'):
                                        async def scan_selected():
                                            sel = list(state['selected_paths'] & set(file_paths))
                                            if not sel:
                                                ui.notify('No files selected', type='warning')
                                                return
                                            ui.notify(f'Scanning {len(sel)} files...', type='info')

                                            def do_bulk_scan():
                                                for p_val in sel:
                                                    if state['show_stuck_only']:
                                                        scanner.history.clear_entry(p_val)
                                                    scanner.scan_file(p_val)

                                            import threading
                                            threading.Thread(target=do_bulk_scan, daemon=True).start()
                                            state['selected_paths'].difference_update(sel)
                                            render_browser()

                                        ui.button('Scan Selected', on_click=scan_selected).classes(
                                            'bg-cyan-600/10 hover:bg-cyan-600 text-cyan-400 hover:text-white '
                                            'rounded-xl border border-cyan-500/20 font-bold text-xs px-3 py-1.5'
                                        ).props('disable' if is_disabled else '')

                                        if state['show_stuck_only']:
                                            async def clear_selected():
                                                sel = list(state['selected_paths'] & set(file_paths))
                                                if not sel:
                                                    ui.notify('No files selected', type='warning')
                                                    return
                                                for p_val in sel:
                                                    scanner.history.clear_entry(p_val)
                                                ui.notify(f'Cleared {len(sel)} stuck entries', type='positive')
                                                state['selected_paths'].difference_update(sel)
                                                render_browser()

                                            ui.button('Clear Selected', on_click=clear_selected).classes(
                                                'bg-rose-950/20 hover:bg-rose-600 text-rose-400 hover:text-white '
                                                'rounded-xl border border-rose-500/20 font-bold text-xs px-3 py-1.5'
                                            ).props('disable' if is_disabled else '')

                    for it in items:
                        it_path  = it['path']
                        is_dir   = it['is_dir']
                        name     = it['name']
                        size_fmt = it['size_fmt']
                        extra    = it.get('extra')
                        mode     = it.get('mode', 'normal')

                        # Library presence check (skip for stuck/missing — already categorised)
                        in_lib = False
                        if not is_dir and mode == 'normal':
                            in_lib = scanner.is_in_library(it_path)

                        # Build leading icon html
                        if is_dir:
                            icon_html = '<i class="fas fa-folder text-cyan-400 text-base"></i>'
                        elif mode == 'stuck':
                            icon_html = '<i class="fas fa-circle-exclamation text-rose-400 text-base"></i>'
                        elif mode == 'missing':
                            icon_html = '<i class="fas fa-circle-xmark text-yellow-400 text-base"></i>'
                        elif in_lib:
                            icon_html = '<i class="fas fa-circle-check text-emerald-400 text-base"></i>'
                        else:
                            icon_html = '<i class="fas fa-circle-xmark text-yellow-400 text-base"></i>'

                        # Render using plain div — avoids Quasar alignment overrides
                        with ui.element('div').classes('browser-row'):
                            # Left: icon + text block
                            with ui.row().classes('items-center gap-3 grow overflow-hidden min-w-0'):
                                if (state['show_missing_only'] or state['show_stuck_only']) and not is_dir:
                                    async def toggle_select(e, p_val=it_path):
                                        is_in = p_val in state['selected_paths']
                                        if e.value != is_in:
                                            if e.value:
                                                state['selected_paths'].add(p_val)
                                            else:
                                                state['selected_paths'].discard(p_val)
                                            render_browser()
                                    ui.checkbox(value=it_path in state['selected_paths'], on_change=toggle_select).classes('mr-1')

                                ui.html(icon_html)
                                with ui.element('div').style('display:flex;flex-direction:column;align-items:flex-start;gap:2px;min-width:0;flex:1'):
                                    if is_dir:
                                        async def click_folder(p_val=it_path):
                                            state['current_path'] = p_val
                                            render_browser()
                                        ui.label(name).classes(
                                            'text-sm font-bold text-white truncate cursor-pointer hover:text-cyan-300 w-full'
                                        ).on('click', click_folder)
                                    else:
                                        ui.label(name).classes('text-sm font-bold text-slate-100 truncate w-full')

                                    # Meta row: path + size + library status
                                    with ui.element('div').style('display:flex;flex-wrap:wrap;align-items:center;gap:8px;'):
                                        ui.label(it_path).classes('text-[10px] text-slate-500 font-mono').style('max-width:480px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap')
                                        if size_fmt:
                                            ui.html(f'<span class="text-[10px] text-slate-600 font-semibold">{size_fmt}</span>')
                                        if mode == 'normal' and not is_dir:
                                            lib_color = '#34d399' if in_lib else '#fbbf24'
                                            lib_lbl   = 'In Library' if in_lib else 'Not in Library'
                                            ui.html(f'<span style="color:{lib_color};font-size:10px;font-weight:700">{lib_lbl}</span>')

                                    # Stuck reason row
                                    if extra:
                                        attempts = extra['attempts']
                                        with ui.element('div').style('display:flex;flex-wrap:wrap;align-items:center;gap:6px;margin-top:3px'):
                                            ui.html(
                                                f'<span style="display:inline-flex;align-items:center;gap:4px;background:rgba(239,68,68,0.1);'
                                                f'border:1px solid rgba(239,68,68,0.25);color:#f87171;font-size:10px;font-weight:700;'
                                                f'padding:2px 8px;border-radius:9999px">'
                                                f'<i class="fas fa-rotate-right" style="font-size:8px"></i>'
                                                f' {attempts} failed attempt{"s" if attempts != 1 else ""}</span>'
                                            )
                                            ui.html(
                                                '<span style="font-size:10px;color:#64748b">'
                                                'File on disk but Plex/Jellyfin failed to import after repeated scans'
                                                '</span>'
                                            )
                                            ui.html(f'<span style="font-size:10px;color:#475569">Last: {extra["last_seen"]}</span>')

                            # Right: action buttons
                            with ui.element('div').style('display:flex;gap:8px;align-items:center;flex-shrink:0;margin-left:12px'):
                                async def scan_item(p_val=it_path, is_folder=is_dir, mode_val=mode, force_full=False):
                                    if force_full:
                                        ui.notify(f'Full Sweep: {os.path.basename(p_val)}', type='info')
                                    else:
                                        ui.notify(f'Scanning: {os.path.basename(p_val)}', type='info')
                                    if mode_val == 'stuck':
                                        scanner.history.clear_entry(p_val)
                                    if not is_folder:
                                        scanner.scan_file(p_val)
                                    else:
                                        scanner.scan_folder_async(p_val, force_full)
                                    render_browser()
                                ui.button('Scan', on_click=lambda p=it_path, f=is_dir, m=mode: scan_item(p, f, m, force_full=False)).classes(
                                    'bg-cyan-600/10 hover:bg-cyan-600 text-cyan-400 hover:text-white '
                                    'rounded-xl border border-cyan-500/20 font-bold text-xs px-3 py-1.5')
                                if is_dir:
                                    ui.button('Full Sweep', on_click=lambda p=it_path, f=is_dir, m=mode: scan_item(p, f, m, force_full=True)).classes(
                                        'bg-indigo-600/10 hover:bg-indigo-600 text-indigo-400 hover:text-white '
                                        'rounded-xl border border-indigo-500/20 font-bold text-xs px-3 py-1.5')

                                if mode == 'stuck':
                                     async def clear_stuck_entry(p_val=it_path):
                                         scanner.history.clear_entry(p_val)
                                         ui.notify(f'Cleared: {os.path.basename(p_val)}', type='positive')
                                         render_browser()
                                     ui.button('Clear', on_click=clear_stuck_entry).classes(
                                         'bg-rose-950/20 hover:bg-rose-600 text-rose-400 hover:text-white '
                                         'rounded-xl border border-rose-500/20 font-bold text-xs px-3 py-1.5')

            async def on_search_change():
                state['selected_paths'] = set()
                render_browser()

            render_browser()
            search_input.on_value_change(on_search_change)

    @ui.page('/logs')
    async def logs(request: Request):
        if r := check_auth(request, scanner): return r
        apply_theme()
        add_menu('/logs')
        
        with ui.column().classes('w-full p-8 md:p-10 gap-6'):
            # Header
            with ui.row().classes('w-full items-center justify-between flex-wrap gap-4'):
                with ui.column().classes('gap-1'):
                    ui.label('System Logs').classes('text-3xl font-black text-white')
                    ui.label('Live log stream — auto-refreshes every 2 seconds').classes('text-xs text-slate-500')
                with ui.row().classes('gap-2 items-center'):
                    ui.html('<span class="w-2 h-2 rounded-full bg-emerald-400 animate-pulse inline-block"></span>')
                    ui.label('Live').classes('text-xs font-bold text-emerald-400 uppercase tracking-widest')

            # Controls bar
            with ui.card().classes('glass-card p-4 rounded-2xl w-full'):
                with ui.row().classes('w-full items-center gap-4 flex-wrap justify-between'):
                    with ui.row().classes('gap-2 items-center flex-wrap'):
                        ui.label('Level:').classes('text-xs font-bold text-slate-400 uppercase tracking-wider')
                        log_level_select = ui.select(
                            options={'ALL': 'All', 'DEBUG': 'Debug', 'INFO': 'Info', 'WARNING': 'Warning', 'ERROR': 'Error'},
                            value='ALL'
                        ).classes('w-32').props('outlined dense')

                        ui.label('Search:').classes('text-xs font-bold text-slate-400 uppercase tracking-wider ml-2')
                        log_search = ui.input(placeholder='Filter logs...').classes('w-52').props('outlined dense')

                    with ui.row().classes('gap-2 items-center'):
                        async def clear_logs():
                            log_box.clear()
                            ui.notify('Log view cleared', type='info')
                        ui.button(icon='delete_outline', on_click=clear_logs).props('flat round dense').classes('text-slate-500 hover:text-rose-400').tooltip('Clear view')

                        async def copy_logs():
                            from .web import recent_logs
                            text = '\n'.join(list(recent_logs))
                            ui.run_javascript(f"navigator.clipboard.writeText({repr(text)})")
                            ui.notify('Logs copied to clipboard', type='positive')
                        ui.button(icon='content_copy', on_click=copy_logs).props('flat round dense').classes('text-slate-500 hover:text-cyan-400').tooltip('Copy all')

            # Log viewer
            with ui.card().classes('glass-card rounded-2xl w-full overflow-hidden p-0'):
                # Custom styled log box
                ui.add_head_html('''
                <style>
                .log-line-error   { color: #f87171 !important; }
                .log-line-warning { color: #fbbf24 !important; }
                .log-line-info    { color: #94a3b8 !important; }
                .log-line-debug   { color: #64748b !important; }
                .nicegui-log .log-row { font-family: "JetBrains Mono", "Fira Code", monospace !important; font-size: 11px !important; line-height: 1.7 !important; padding: 0 4px !important; border-bottom: 1px solid rgba(255,255,255,0.02) !important; }
                .nicegui-log { background: #020617 !important; border-radius: 0 0 1rem 1rem !important; }
                </style>
                ''')

                log_box = ui.log(max_lines=300).classes(
                    'w-full h-[calc(100vh-320px)] min-h-[400px] font-mono text-[11px] bg-slate-950 '
                    'nicegui-log'
                )

            def _line_color(line: str) -> str:
                l = line.upper()
                if 'ERROR' in l or 'CRITICAL' in l: return 'ERROR'
                if 'WARNING' in l or 'WARN' in l:   return 'WARNING'
                if 'DEBUG' in l:                     return 'DEBUG'
                return 'INFO'

            def refresh_logs():
                try:
                    log_box.clear()
                    lvl    = log_level_select.value
                    term   = (log_search.value or '').lower()
                    from .web import recent_logs
                    for line in list(recent_logs):
                        line_lvl = _line_color(line)
                        if lvl != 'ALL' and line_lvl != lvl:
                            continue
                        if term and term not in line.lower():
                            continue
                        log_box.push(line)
                except RuntimeError:
                    pass

            refresh_logs()
            log_level_select.on_value_change(refresh_logs)
            log_search.on_value_change(refresh_logs)
            auto_cancel(ui.timer(2.0, refresh_logs))

    @ui.page('/settings')
    async def settings(request: Request):
        if r := check_auth(request, scanner): return r
        apply_theme()
        add_menu('/settings')

        c = scanner.config

        values = {
            'server_type': c.get('SERVER_TYPE', 'plex'),
            'plex_server': c.get('PLEX_URL', ''),
            'plex_token': mask_s(c.get('TOKEN', '')),
            'server_url': c.get('SERVER_URL', ''),
            'api_key': mask_s(c.get('API_KEY', '')),
            'scan_directories': "\n".join(c.get('SCAN_PATHS', [])),

            'scan_workers': c.get('SCAN_WORKERS', 4),
            'scan_debounce': c.get('SCAN_DEBOUNCE', 10),
            'scan_delay': c.get('SCAN_DELAY', 0.0),
            'use_polling': c.get('USE_POLLING', False),
            'watch_mode': c.get('WATCH_MODE', False),
            'run_interval': c.get('RUN_INTERVAL', 24),
            'run_on_startup': c.get('RUN_ON_STARTUP', True),
            'start_time': c.get('START_TIME', ''),
            'incremental_scan': c.get('INCREMENTAL_SCAN', False),
            'scan_since_days': c.get('SCAN_SINCE_DAYS', 7),
            'symlink_check': c.get('SYMLINK_CHECK', False),
            'empty_trash': c.get('EMPTY_TRASH', False),
            'integrity_check': c.get('INTEGRITY_CHECK', False),
            'ffprobe_check': c.get('FFPROBE_CHECK', False),
            'deletion_threshold': c.get('DELETION_THRESHOLD', 50),
            'abort_on_mass_deletion': c.get('ABORT_ON_MASS_DELETION', True),
            'notifications_enabled': c.get('NOTIFICATIONS_ENABLED', True),
            'discord_webhook_url': mask_s(c.get('DISCORD_WEBHOOK_URL', '')),
            'notification_group_window': c.get('NOTIFICATION_GROUP_WINDOW', 15),
            'ignore_patterns': "\n".join(c.get('IGNORE_PATTERNS', [])),
            'log_level': c.get('LOG_LEVEL', 'INFO'),
            'path_rewrites': "\n".join([f"{src}:{dst}" for src, dst in c.get('PATH_REWRITES', [])])
        }

        def section_header(title, icon, subtitle=None):
            with ui.row().classes('items-center gap-3 pb-3 border-b border-white/5 w-full mb-1'):
                ui.html(f'<i class="fas {icon} text-cyan-400 text-sm"></i>')
                with ui.column().classes('gap-0'):
                    ui.label(title).classes('text-sm font-bold text-cyan-400 uppercase tracking-wider')
                    if subtitle:
                        ui.label(subtitle).classes('text-[10px] text-slate-500')

        with ui.column().classes('w-full p-6 md:p-10 gap-6 max-w-5xl mx-auto'):
            ui.label('System Settings').classes('text-3xl font-black text-white')

            # ─── SECTION 1: Media Server ───────────────────────────────────────
            with ui.card().classes('glass-card p-6 rounded-2xl w-full flex flex-col gap-5'):
                section_header('Media Server Connection', 'fa-server', 'Connect to Plex, Jellyfin or Emby')

                server_type = ui.select(
                    options={'plex': 'Plex', 'jellyfin': 'Jellyfin', 'emby': 'Emby'},
                    value=values['server_type'],
                    label='Server Type'
                ).classes('w-full').props('outlined')

                plex_container = ui.column().classes('w-full gap-4')
                with plex_container:
                    with ui.row().classes('w-full gap-4 flex-wrap'):
                        plex_url = ui.input('Plex Server URL', value=values['plex_server']).classes('grow').props('outlined')
                        plex_token = ui.input('Plex Token', value=values['plex_token']).classes('grow').props('outlined type=password')

                generic_container = ui.column().classes('w-full gap-4 hidden')
                with generic_container:
                    with ui.row().classes('w-full gap-4 flex-wrap'):
                        server_url = ui.input('Server API URL', value=values['server_url']).classes('grow').props('outlined')
                        api_key = ui.input('API Key', value=values['api_key']).classes('grow').props('outlined type=password')

                def toggle_servers(val):
                    if val == 'plex':
                        plex_container.classes(remove='hidden')
                        generic_container.classes(add='hidden')
                    else:
                        plex_container.classes(add='hidden')
                        generic_container.classes(remove='hidden')

                toggle_servers(server_type.value)
                server_type.on_value_change(lambda e: toggle_servers(e.value))

                async def test_conn():
                    ui.notify('Testing connection...', type='info')
                    rt = unmask_v(plex_token.value, scanner.config.get('TOKEN', ''))
                    rk = unmask_v(api_key.value, scanner.config.get('API_KEY', ''))
                    ru = unmask_v(server_url.value, scanner.config.get('SERVER_URL', ''))
                    try:
                        if server_type.value == 'plex':
                            plex = PlexServer(plex_url.value, rt)
                            ui.notify(f'Connected successfully: {plex.friendlyName}', type='positive')
                        else:
                            r = requests.get(f"{ru}/System/Info", headers={"X-Emby-Token": rk}, timeout=5)
                            r.raise_for_status()
                            ui.notify(f'Connected to {server_type.value.capitalize()}', type='positive')
                    except Exception as ex:
                        ui.notify(f'Connection failed: {ex}', type='negative')

                ui.button('Test Connection', on_click=test_conn).classes(
                    'w-full bg-cyan-600/10 hover:bg-cyan-600 text-cyan-400 hover:text-white '
                    'rounded-xl border border-cyan-500/20 font-bold text-xs uppercase tracking-wider py-3 transition-all')

            # ─── SECTION 2: Arr Webhook ────────────────────────────────────────
            with ui.card().classes('glass-card p-6 rounded-2xl w-full flex flex-col gap-4'):
                section_header('Arr App Integration Webhook', 'fa-plug', 'For Radarr, Sonarr, Lidarr, Readarr')

                ui.markdown(
                    'Configure your **Radarr**, **Sonarr**, or other Arr apps to send a webhook '
                    'to Omniscan on import, triggering an immediate library scan.'
                ).classes('text-xs text-slate-400 leading-relaxed')

                scheme = request.headers.get('x-forwarded-proto', request.url.scheme)
                base_url = str(request.base_url).rstrip('/')
                if scheme == 'https' and base_url.startswith('http://'):
                    base_url = base_url.replace('http://', 'https://', 1)
                token = get_webhook_token(scanner.config.get('WEB_PASSWORD', 'admin'))
                webhook_url = f"{base_url}/api/webhook?apikey={token}"

                with ui.row().classes('w-full items-center gap-3 bg-slate-950/50 p-3 rounded-xl border border-white/5'):
                    ui.html('<i class="fas fa-link text-cyan-400 text-xs"></i>')
                    ui.label('Webhook URL').classes('text-xs font-bold text-slate-400 shrink-0')
                    ui.input(value=webhook_url).props('readonly borderless').classes('grow text-xs font-mono text-cyan-300 bg-transparent')
                    def copy_url():
                        ui.run_javascript(f"navigator.clipboard.writeText('{webhook_url}')")
                        ui.notify('Copied to clipboard!', type='positive')
                    ui.button(icon='content_copy', on_click=copy_url).props('flat round dense').classes('text-slate-400 hover:text-cyan-400')

                with ui.expansion('Setup Instructions', icon='help_outline').classes('w-full rounded-xl bg-white/5 border border-white/5'):
                    ui.markdown(
                        '1. In Radarr/Sonarr go to **Settings → Connect**\n'
                        '2. Click **+** → **Webhook**\n'
                        '3. Set **Method** to `POST`\n'
                        '4. Paste the URL above into the **URL** field\n'
                        '5. Enable **On Import** (and **On Rename** if desired)\n'
                        '6. Click **Test** then **Save**'
                    ).classes('text-[11px] text-slate-400 leading-loose p-2')

            # ─── SECTION 3: Scan Directories ───────────────────────────────────
            with ui.card().classes('glass-card p-6 rounded-2xl w-full flex flex-col gap-5'):
                section_header('Media Directories & Paths', 'fa-folder-tree', 'Directories Omniscan will monitor')

                scan_directories = ui.textarea(
                    'Directories to Monitor',
                    value=values['scan_directories'],
                    placeholder='/media/video/movies\n/media/video/tv'
                ).classes('w-full font-mono').props('outlined rows=4')



                async def verify_paths():
                    paths_raw = [p.strip() for p in scan_directories.value.replace(',', '\n').split('\n') if p.strip()]
                    invalid = [p for p in paths_raw if not os.path.isdir(p)]
                    if invalid:
                        ui.notify(f'Invalid: {", ".join(invalid)}', type='negative')
                    else:
                        ui.notify('All directories verified!', type='positive')

                ui.button('Verify Paths', on_click=verify_paths).classes(
                    'bg-cyan-600/10 hover:bg-cyan-600 text-cyan-400 hover:text-white '
                    'rounded-xl border border-cyan-500/20 font-bold text-xs uppercase tracking-wider px-5 py-2 transition-all')

                with ui.row().classes('w-full gap-4 flex-wrap'):
                    with ui.column().classes('grow gap-2'):
                        ui.label('Ignore Patterns').classes('text-xs font-bold text-slate-400 uppercase tracking-wider')
                        ignore_patterns = ui.textarea(
                            value=values['ignore_patterns'],
                            placeholder='*.tmp\n*.log\n@eaDir'
                        ).classes('w-full font-mono').props('outlined rows=3')

                    with ui.column().classes('grow gap-2'):
                        ui.label('Path Rewrites').classes('text-xs font-bold text-slate-400 uppercase tracking-wider')
                        path_rewrites = ui.textarea(
                            value=values['path_rewrites'],
                            placeholder='/source/path:/destination/path'
                        ).classes('w-full font-mono').props('outlined rows=3')

            # ─── SECTION 4: Scanner Behaviour ─────────────────────────────────
            with ui.card().classes('glass-card p-6 rounded-2xl w-full flex flex-col gap-5'):
                section_header('Scanner Behaviour', 'fa-sliders', 'Scan scheduling, performance and modes')

                ui.label('Timing & Performance').classes('text-[10px] font-black text-slate-500 uppercase tracking-widest')
                with ui.row().classes('w-full gap-4 flex-wrap'):
                    scan_workers = ui.number('Scanner Threads', value=values['scan_workers']).classes('grow').props('outlined dense')
                    scan_debounce = ui.number('Debounce Delay (s)', value=values['scan_debounce']).classes('grow').props('outlined dense')
                    scan_delay = ui.number('Delay Between Files (s)', value=values['scan_delay']).classes('grow').props('outlined dense')
                    run_interval = ui.number('Scheduled Run Interval (h)', value=values['run_interval']).classes('grow').props('outlined dense')
                    start_time = ui.input('Start Time (HH:MM)', value=values['start_time']).classes('grow').props('outlined dense')
                    log_level = ui.select(
                        options={'DEBUG': 'Debug', 'INFO': 'Info', 'WARNING': 'Warning', 'ERROR': 'Error'},
                        value=values['log_level'], label='Log Level'
                    ).classes('grow').props('outlined dense')

                ui.separator().classes('opacity-5')

                ui.label('Scan Modes').classes('text-[10px] font-black text-slate-500 uppercase tracking-widest')
                with ui.grid(columns=3).classes('w-full gap-4'):
                    watch_mode = ui.switch('Watch Mode', value=values['watch_mode'])
                    use_polling = ui.switch('Use Polling Observer', value=values['use_polling'])
                    run_on_startup = ui.switch('Run Scan on Startup', value=values['run_on_startup'])
                    incremental_scan = ui.switch('Incremental Scan', value=values['incremental_scan'])
                    symlink_check = ui.switch('Symlink Check', value=values['symlink_check'])
                    empty_trash = ui.switch('Empty Trash on Deletion', value=values['empty_trash'])

                with ui.row().classes('w-full gap-4 flex-wrap items-center'):
                    scan_since_days = ui.number('Incremental Window (days)', value=values['scan_since_days']).classes('grow').props('outlined dense')

                ui.separator().classes('opacity-5')

                ui.label('Integrity & Safety').classes('text-[10px] font-black text-slate-500 uppercase tracking-widest')
                with ui.grid(columns=3).classes('w-full gap-4'):
                    integrity_check = ui.switch('File Integrity Verification', value=values['integrity_check'])
                    ffprobe_check = ui.switch('FFprobe Corruption Check', value=values['ffprobe_check'])
                    abort_on_mass_deletion = ui.switch('Abort on Mass Deletion', value=values['abort_on_mass_deletion'])

                with ui.row().classes('w-full gap-4 flex-wrap items-center'):
                    deletion_threshold = ui.number('Mass Deletion Limit (%)', value=values['deletion_threshold']).classes('grow').props('outlined dense')

            # ─── SECTION 5: Notifications ──────────────────────────────────────
            with ui.card().classes('glass-card p-6 rounded-2xl w-full flex flex-col gap-5'):
                section_header('Discord Notifications', 'fa-bell', 'Get notified on scan events via Discord webhook')

                notifications_enabled = ui.switch('Enable Notifications', value=values['notifications_enabled'])

                with ui.row().classes('w-full gap-4 flex-wrap items-end'):
                    discord_webhook_url = ui.input(
                        'Discord Webhook URL',
                        value=values['discord_webhook_url']
                    ).classes('grow').props('outlined type=password')
                    with ui.column().classes('gap-1'):
                        with ui.row().classes('items-center gap-1'):
                            ui.label('Group Window (s)').classes('text-xs text-slate-400')
                            with ui.element('span'):
                                ui.tooltip(
                                    'Events are buffered for this many seconds before being '
                                    'flushed as a single Discord message. Increase to reduce '
                                    'message count during busy scans (default: 15).'
                                ).classes('text-xs max-w-xs')
                                ui.html('<i class="fas fa-circle-info text-slate-500 text-xs cursor-help"></i>')
                        notification_group_window = ui.number(
                            value=values['notification_group_window'], min=5, max=300
                        ).classes('w-32').props('outlined dense suffix=s')

                async def test_webhook_btn():
                    ui.notify('Sending test notification...', type='info')
                    webhook_to_use = unmask_v(discord_webhook_url.value, scanner.config.get('DISCORD_WEBHOOK_URL', ''))
                    if not webhook_to_use:
                        ui.notify('No Discord Webhook URL configured', type='warning')
                        return
                    try:
                        from .notifications import send_discord_webhook_sync
                        from discord import Embed, Color
                        embed = Embed(
                            title='✅ Omniscan Test Message',
                            description='Your Discord notifications are working!',
                            color=Color.green(),
                            timestamp=datetime.now()
                        )
                        embed.set_footer(text='Omniscan Media Monitor')
                        if send_discord_webhook_sync(webhook_to_use, embed, scanner.config):
                            ui.notify('Test notification sent!', type='positive')
                        else:
                            ui.notify('Failed to send. Check logs.', type='negative')
                    except Exception as ex:
                        ui.notify(f'Error: {ex}', type='negative')

                ui.button('Send Test Notification', on_click=test_webhook_btn).classes(
                    'bg-indigo-600/10 hover:bg-indigo-600 text-indigo-400 hover:text-white '
                    'rounded-xl border border-indigo-500/20 font-bold text-xs uppercase tracking-wider px-5 py-2 transition-all')

            # ─── STICKY SAVE BUTTON ────────────────────────────────────────────
            with ui.row().classes('w-full justify-end pt-4 sticky bottom-6 z-50'):
                async def handle_save_settings():
                    ui.notify('Applying and saving configurations...', type='info')

                    unmasked_token = unmask_v(plex_token.value, scanner.config.get('TOKEN', ''))
                    unmasked_api_key = unmask_v(api_key.value, scanner.config.get('API_KEY', ''))
                    unmasked_webhook = unmask_v(discord_webhook_url.value, scanner.config.get('DISCORD_WEBHOOK_URL', ''))
                    unmasked_server_url = unmask_v(server_url.value, scanner.config.get('SERVER_URL', ''))

                    c = scanner.config
                    c['SERVER_TYPE'] = server_type.value
                    c['SERVER_URL'] = unmasked_server_url
                    c['API_KEY'] = unmasked_api_key
                    c['PLEX_URL'] = plex_url.value
                    c['TOKEN'] = unmasked_token
                    c['SCAN_PATHS'] = [p.strip() for p in scan_directories.value.replace(',', '\n').split('\n') if p.strip()]
                    c['WATCH_DIRECTORIES'] = []
                    c['SCAN_WORKERS'] = int(scan_workers.value)
                    c['SCAN_DEBOUNCE'] = int(scan_debounce.value)
                    c['SCAN_DELAY'] = float(scan_delay.value)
                    c['USE_POLLING'] = use_polling.value
                    c['WATCH_MODE'] = watch_mode.value
                    c['RUN_INTERVAL'] = int(run_interval.value)
                    c['RUN_ON_STARTUP'] = run_on_startup.value
                    c['START_TIME'] = start_time.value
                    c['INCREMENTAL_SCAN'] = incremental_scan.value
                    c['SCAN_SINCE_DAYS'] = int(scan_since_days.value)
                    c['SYMLINK_CHECK'] = symlink_check.value
                    c['EMPTY_TRASH'] = empty_trash.value
                    c['INTEGRITY_CHECK'] = integrity_check.value
                    c['FFPROBE_CHECK'] = ffprobe_check.value
                    c['DELETION_THRESHOLD'] = int(deletion_threshold.value)
                    c['ABORT_ON_MASS_DELETION'] = abort_on_mass_deletion.value
                    c['NOTIFICATIONS_ENABLED'] = notifications_enabled.value
                    c['DISCORD_WEBHOOK_URL'] = unmasked_webhook
                    c['NOTIFICATION_GROUP_WINDOW'] = int(notification_group_window.value)
                    c['IGNORE_PATTERNS'] = [p.strip() for p in ignore_patterns.value.replace(',', '\n').split('\n') if p.strip()]
                    c['LOG_LEVEL'] = log_level.value

                    c['PATH_REWRITES'] = []
                    for line in path_rewrites.value.replace(',', '\n').split('\n'):
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
                        cfg.set('behaviour', 'use_polling', str(c['USE_POLLING']).lower())
                        cfg.set('behaviour', 'watch', str(c['WATCH_MODE']).lower())
                        cfg.set('behaviour', 'run_interval', str(c['RUN_INTERVAL']))
                        cfg.set('behaviour', 'run_on_startup', str(c['RUN_ON_STARTUP']).lower())
                        cfg.set('behaviour', 'start_time', c['START_TIME'] if c['START_TIME'] else '')
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
                        cfg.set('scan', 'directories', ','.join(c['SCAN_PATHS']))
                        cfg.set('scan', 'watch_directories', '')
                        cfg.set('ignore', 'patterns', ','.join(c['IGNORE_PATTERNS']))
                        cfg.set('logs', 'loglevel', str(c['LOG_LEVEL']))
                        cfg.set('rewrite', 'mappings', ','.join([f'{src}:{dst}' for src, dst in c['PATH_REWRITES']]))

                        with open('config.ini', 'w') as f: cfg.write(f)

                        if c['SERVER_TYPE'] == 'plex':
                            scanner.connect_to_plex(retry=False)
                            scanner.get_library_ids()

                        ui.notify('Settings saved successfully!', type='positive')
                    except Exception as ex:
                        ui.notify(f'Failed to save: {ex}', type='negative')

                ui.button('💾  Save Settings', on_click=handle_save_settings).classes(
                    'px-10 py-3 bg-gradient-to-r from-blue-600 to-indigo-600 hover:from-blue-500 hover:to-indigo-500 '
                    'text-white font-bold rounded-full shadow-xl shadow-blue-900/30 transition-all text-xs uppercase tracking-widest')

    @ui.page('/login')
    async def login_page(request: Request):
        if is_setup_completed(scanner) and request.session.get("user"):
            return RedirectResponse('/')
        if not is_setup_completed(scanner):
            return RedirectResponse('/setup')
        
        apply_theme()
        
        error_msg = request.query_params.get('error')
        
        with ui.column().classes('w-full min-h-screen items-center justify-center bg-[#030712] relative overflow-hidden p-4'):
            # Glows
            ui.html('<div class="absolute w-[400px] h-[400px] bg-blue-500/10 rounded-full blur-[120px] -top-20 -left-20 pointer-events-none"></div>')
            ui.html('<div class="absolute w-[400px] h-[400px] bg-indigo-500/10 rounded-full blur-[120px] -bottom-20 -right-20 pointer-events-none"></div>')
            
            with ui.card().classes('w-full max-w-md bg-[#090d16]/60 border border-white/5 rounded-3xl p-8 shadow-2xl backdrop-blur-xl flex flex-col gap-6'):
                with ui.column().classes('items-center w-full mb-4'):
                    ui.html('<img src="https://raw.githubusercontent.com/drondeseries/omniscan/master/assets/logo.png" class="w-16 h-16 rounded-2xl mb-4 shadow-xl shadow-blue-500/10">')
                    ui.label('Omniscan').classes('text-3xl font-black text-white tracking-tight bg-gradient-to-r from-white to-slate-400 bg-clip-text text-transparent')
                    ui.label('System Access').classes('text-slate-500 text-[10px] font-black uppercase tracking-widest mt-1')
                
                if error_msg:
                    with ui.row().classes('w-full bg-rose-500/10 text-rose-400 border border-rose-500/20 p-4 rounded-xl items-center gap-2 mb-2'):
                        ui.html('<i class="fas fa-triangle-exclamation"></i>')
                        ui.label(error_msg).classes('text-xs font-semibold')
                
                with ui.element('form').props('action="/login" method="POST"').classes('w-full flex flex-col gap-6'):
                    ui.input('Username').classes('w-full').props('outlined font-medium name="username"')
                    ui.input('Password').classes('w-full').props('outlined type=password font-medium name="password"')
                    
                    ui.button('Authenticate').props('type="submit"').classes('w-full py-3 bg-gradient-to-r from-blue-600 to-indigo-600 hover:from-blue-500 hover:to-indigo-500 text-white font-bold rounded-xl shadow-lg transition-all')

    @ui.page('/setup')
    async def setup_page(request: Request):
        if is_setup_completed(scanner):
            return RedirectResponse('/login')
            
        apply_theme()
        
        with ui.column().classes('w-full min-h-screen items-center justify-center bg-[#030712] p-4 relative overflow-hidden'):
            # Ambient glows
            ui.html('<div class="absolute w-[450px] h-[450px] bg-blue-500/10 rounded-full blur-[130px] -top-20 -left-20 pointer-events-none"></div>')
            ui.html('<div class="absolute w-[450px] h-[450px] bg-indigo-500/10 rounded-full blur-[130px] -bottom-20 -right-20 pointer-events-none"></div>')
            
            with ui.card().classes('w-full max-w-2xl bg-[#090d16]/60 border border-white/5 rounded-3xl p-8 shadow-2xl backdrop-blur-xl'):
                with ui.column().classes('items-center w-full mb-8'):
                    ui.label('Welcome to Omniscan').classes('text-3xl font-black text-white tracking-tight bg-gradient-to-r from-white to-slate-400 bg-clip-text text-transparent')
                    ui.label('Let\'s configure your media library monitor').classes('text-slate-400 text-xs font-semibold mt-1')
                
                step = ui.number(value=1).classes('hidden') # hidden variable to track step
                
                # STEP 1 Container
                step1_container = ui.column().classes('w-full gap-4')
                with step1_container:
                    ui.label('Step 1: Administrator Account').classes('text-lg font-bold text-cyan-400')
                    username_field = ui.input('Username', value='admin').classes('w-full').props('outlined')
                    password_field = ui.input('Password').classes('w-full').props('outlined type=password')
                    confirm_password = ui.input('Confirm Password').classes('w-full').props('outlined type=password')
                
                # STEP 2 Container
                step2_container = ui.column().classes('w-full gap-4 hidden')
                with step2_container:
                    ui.label('Step 2: Media Server Connection').classes('text-lg font-bold text-cyan-400')
                    
                    # Discover banner
                    discover_banner = ui.row().classes('w-full bg-cyan-500/10 text-cyan-400 border border-cyan-500/20 p-4 rounded-xl items-center justify-between hidden')
                    with discover_banner:
                        with ui.row().classes('items-center gap-2'):
                            ui.html('<i class="fas fa-magic text-sm animate-pulse text-cyan-400"></i>')
                            discover_text = ui.label('')
                        discover_apply_btn = ui.button('Apply', on_click=None).classes('bg-cyan-500 hover:bg-cyan-400 text-white font-bold p-2 text-xs rounded-lg')
                    
                    server_type_select = ui.select(options={'plex': 'Plex Server', 'jellyfin': 'Jellyfin Server', 'emby': 'Emby Server'}, value='plex').classes('w-full').props('outlined')
                    
                    # Plex fields
                    plex_fields = ui.column().classes('w-full gap-4')
                    with plex_fields:
                        plex_server_field = ui.input('Plex URL', value='http://localhost:32400').classes('w-full').props('outlined')
                        plex_token_field = ui.input('Plex Token').classes('w-full').props('outlined type=password')
                    
                    # Jellyfin/Emby fields
                    generic_fields = ui.column().classes('w-full gap-4 hidden')
                    with generic_fields:
                        server_url_field = ui.input('Server URL').classes('w-full').props('outlined')
                        api_key_field = ui.input('API Token/Key').classes('w-full').props('outlined type=password')
                        
                    def handle_server_change(e):
                        if e.value == 'plex':
                            plex_fields.classes(remove='hidden')
                            generic_fields.classes(add='hidden')
                        else:
                            plex_fields.classes(add='hidden')
                            generic_fields.classes(remove='hidden')
                            
                    server_type_select.on_value_change(handle_server_change)
                    
                    # Test connection button
                    async def test_conn():
                        ui.notify('Testing connection...', type='info')
                        try:
                            if server_type_select.value == 'plex':
                                plex = PlexServer(plex_server_field.value, plex_token_field.value)
                                ui.notify(f'Connected successfully to Plex: {plex.friendlyName}', type='positive')
                            else:
                                h = {"X-Emby-Token": api_key_field.value}
                                r = requests.get(f"{server_url_field.value}/System/Info", headers=h, timeout=5)
                                r.raise_for_status()
                                ui.notify(f'Connected successfully to {server_type_select.value.capitalize()}', type='positive')
                        except Exception as ex:
                            ui.notify(f'Connection failed: {ex}', type='negative')
                            
                    ui.button('Test Connection Link', on_click=test_conn).classes('w-full py-2 bg-cyan-600/10 hover:bg-cyan-600 text-cyan-400 hover:text-white rounded-xl border border-cyan-500/20 font-bold text-xs uppercase tracking-wider')
                
                # STEP 3 Container
                step3_container = ui.column().classes('w-full gap-4 hidden')
                with step3_container:
                    ui.label('Step 3: Monitor Paths').classes('text-lg font-bold text-cyan-400')
                    scan_directories_field = ui.textarea('Scan Directories (one path per line)').classes('w-full').props('outlined rows=5 font-mono placeholder="/media/video/movies\\n/media/video/tv"')
                    
                # Navigation buttons
                with ui.row().classes('w-full justify-between mt-8'):
                    back_btn = ui.button('Back', icon='arrow_back').classes('bg-slate-800 text-slate-300 font-bold p-3 rounded-xl hidden')
                    next_btn = ui.button('Next Step', icon='arrow_forward').classes('bg-blue-600 hover:bg-blue-500 text-white font-bold p-3 rounded-xl ml-auto')
                    
                    async def handle_next():
                        if step.value == 1:
                            if not password_field.value:
                                ui.notify('Password is required', type='warning')
                                return
                            if password_field.value != confirm_password.value:
                                ui.notify('Passwords do not match', type='warning')
                                return
                            step.set_value(2)
                            step1_container.classes(add='hidden')
                            step2_container.classes(remove='hidden')
                            back_btn.classes(remove='hidden')
                        elif step.value == 2:
                            ui.notify('Fetching library paths from media server...', type='info')
                            paths = []
                            try:
                                if server_type_select.value == 'plex':
                                    # Use a separate thread executor or runs synchronously in async context
                                    plex = await asyncio.get_event_loop().run_in_executor(
                                        None, lambda: PlexServer(plex_server_field.value, plex_token_field.value)
                                    )
                                    sections = await asyncio.get_event_loop().run_in_executor(
                                        None, lambda: plex.library.sections()
                                    )
                                    for section in sections:
                                        for location in section.locations:
                                            paths.append(location)
                                else:
                                    h = {"X-Emby-Token": api_key_field.value}
                                    r = await asyncio.get_event_loop().run_in_executor(
                                        None, lambda: requests.get(f"{server_url_field.value}/Library/VirtualFolders", headers=h, timeout=5)
                                    )
                                    r.raise_for_status()
                                    for item in r.json():
                                        for location in item.get('Locations', []):
                                            paths.append(location)
                                            
                                if paths:
                                    paths = list(set(paths))
                                    paths.sort()
                                    scan_directories_field.set_value("\n".join(paths))
                                    ui.notify(f'Successfully imported {len(paths)} directories from server!', type='positive')
                                else:
                                    ui.notify('Connected, but no library directories were found on the server.', type='warning')
                            except Exception as ex:
                                logger.error(f"Failed to fetch library paths: {ex}")
                                ui.notify(f'Could not fetch paths automatically: {ex}. Please enter them manually.', type='warning')
                                
                            step.set_value(3)
                            step2_container.classes(add='hidden')
                            step3_container.classes(remove='hidden')
                            next_btn.text = 'Finish & Start'
                            next_btn.props('icon=check')
                        elif step.value == 3:
                            c = scanner.config
                            c['WEB_USERNAME'] = username_field.value
                            c['WEB_PASSWORD'] = password_field.value
                            c['SERVER_TYPE'] = server_type_select.value
                            c['PLEX_URL'] = plex_server_field.value
                            c['TOKEN'] = plex_token_field.value
                            c['SERVER_URL'] = server_url_field.value
                            c['API_KEY'] = api_key_field.value
                            c['SCAN_PATHS'] = [p.strip() for p in scan_directories_field.value.replace(',', '\n').split('\n') if p.strip()]
                            
                            try:
                                cfg = configparser.ConfigParser()
                                cfg.read('config.ini')
                                for sec in ['web', 'server', 'plex', 'scan']:
                                    if not cfg.has_section(sec): cfg.add_section(sec)
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
                                    
                                if c['SERVER_TYPE'] == 'plex':
                                    scanner.connect_to_plex(retry=False)
                                    scanner.get_library_ids()
                                    
                                ui.notify('Setup completed successfully! Please log in.', type='positive')
                                ui.navigate.to('/login')
                            except Exception as ex:
                                ui.notify(f'Failed to complete setup: {ex}', type='negative')
                                
                    async def handle_back():
                        if step.value == 2:
                            step.set_value(1)
                            step2_container.classes(add='hidden')
                            step1_container.classes(remove='hidden')
                            back_btn.classes(add='hidden')
                        elif step.value == 3:
                            step.set_value(2)
                            step3_container.classes(add='hidden')
                            step2_container.classes(remove='hidden')
                            next_btn.text = 'Next Step'
                            next_btn.props('icon=arrow_forward')
                            
                    next_btn.on_click(handle_next)
                    back_btn.on_click(handle_back)
                    
                    async def run_discovery():
                        try:
                            servers = await discover_media_server()
                            if servers:
                                discover_banner.clear()
                                with discover_banner:
                                    with ui.row().classes('items-center gap-2'):
                                        ui.html('<i class="fas fa-magic text-sm animate-pulse text-cyan-400"></i>')
                                        if len(servers) == 1:
                                            stype, url = servers[0]
                                            ui.label(f"Auto-detected local {stype.capitalize()} server at {url}!")
                                        else:
                                            ui.label(f"Auto-detected {len(servers)} local media servers!")
                                    
                                    with ui.row().classes('items-center gap-2'):
                                        for stype, url in servers:
                                            def make_click_handler(s_type, s_url):
                                                async def apply_discovered():
                                                    server_type_select.set_value(s_type)
                                                    if s_type == 'plex':
                                                        plex_server_field.set_value(s_url)
                                                    else:
                                                        server_url_field.set_value(s_url)
                                                    ui.notify(f'Pre-filled settings for {s_type.capitalize()}!', type='positive')
                                                    discover_banner.classes(add='hidden')
                                                return apply_discovered
                                                
                                            btn_label = f"Apply {stype.capitalize()}" if len(servers) > 1 else "Apply"
                                            ui.button(btn_label, on_click=make_click_handler(stype, url)).classes('bg-cyan-500 hover:bg-cyan-400 text-white font-bold p-2 text-xs rounded-lg')
                                            
                                discover_banner.classes(remove='hidden')
                        except RuntimeError:
                            pass
                            
                    auto_cancel(ui.timer(0.5, run_discovery, once=True))
