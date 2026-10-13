import os
import configparser
import logging
import secrets
import stat
import tempfile


def get_config_val(
    config, env_key, config_section, config_key, fallback=None, cast_func=None
):
    """Get config value from env var or config.ini, with optional type casting."""
    # Try environment variable first
    val = os.getenv(env_key)

    # Try config.ini second
    if val is None:
        try:
            val = config.get(config_section, config_key, fallback=fallback)
        except (configparser.NoSectionError, configparser.NoOptionError):
            val = fallback

    # Return fallback if still None (and fallback was explicitly passed as None)
    if val is None:
        return None

    # Cast value if needed
    if cast_func:
        try:
            return cast_func(val)
        except (ValueError, TypeError):
            logging.warning(
                f"Invalid value for {env_key}/{config_key}: {val}. Using fallback: {fallback}"
            )
            return fallback
    return val


def normalize_emby_url(url, server_type=None):
    """Return URL without trailing slashes or extra whitespace, without forcing /emby prefix."""
    if not url:
        return ""
    return str(url).strip().rstrip("/")


def get_jellyfin_headers(api_key=""):
    """Build standard Jellyfin/Emby API headers.

    Sends Authorization (MediaBrowser scheme with Client, Device, DeviceId, Version, Token),
    X-MediaBrowser-Token, X-Emby-Authorization, and X-Emby-Token for 100% compliance across
    all Jellyfin versions (including >=10.9 with legacy authorization disabled) and Emby.
    """
    token = (api_key or "").strip()
    return {
        "X-Emby-Token": token,
        "X-MediaBrowser-Token": token,
        "X-Emby-Authorization": f'MediaBrowser Client="Omniscan", Device="Omniscan", DeviceId="omniscan", Version="1.0.0", Token="{token}"',
        "Authorization": f'MediaBrowser Client="Omniscan", Device="Omniscan", DeviceId="omniscan", Version="1.0.0", Token="{token}"',
        "Accept": "application/json",
        "Content-Type": "application/json",
    }


def get_jellyfin_params(api_key=""):
    """Build query parameters for Jellyfin/Emby authentication fallback.

    Provides ApiKey (standard Jellyfin query parameter) and api_key (Emby / legacy Jellyfin)
    as a fallback in case reverse proxies or intermediaries strip Authorization headers.
    """
    token = (api_key or "").strip()
    if not token:
        return {}
    return {
        "ApiKey": token,
        "api_key": token,
    }


def load_config(config_path="config.ini"):
    config = configparser.ConfigParser(interpolation=None)
    config.read(config_path)

    cfg = {}

    # Load config with Env Var overrides
    cfg["SERVER_TYPE"] = get_config_val(
        config, "SERVER_TYPE", "server", "type", "plex"
    ).lower()
    cfg["PLEX_URL"] = get_config_val(config, "PLEX_SERVER", "plex", "server")
    cfg["TOKEN"] = get_config_val(config, "PLEX_TOKEN", "plex", "token")

    # Generic Server support (Emby/Jellyfin)
    server_url = get_config_val(config, "SERVER_URL", "server", "url")
    cfg["SERVER_URL"] = normalize_emby_url(server_url, cfg["SERVER_TYPE"])
    cfg["API_KEY"] = get_config_val(config, "API_KEY", "server", "api_key")

    cfg["LOG_LEVEL"] = get_config_val(config, "LOG_LEVEL", "logs", "loglevel", "INFO")
    cfg["SCAN_INTERVAL"] = get_config_val(
        config, "SCAN_INTERVAL", "behaviour", "scan_interval", 15, int
    )
    cfg["RUN_INTERVAL"] = max(
        1,
        get_config_val(
            config, "RUN_INTERVAL", "behaviour", "run_interval", 24, int
        ),
    )
    cfg["RUN_INTERVAL_UNIT"] = get_config_val(
        config, "RUN_INTERVAL_UNIT", "behaviour", "run_interval_unit", "hours"
    )
    cfg["MAX_RETRIES"] = max(
        1,
        get_config_val(config, "MAX_RETRIES", "behaviour", "max_retries", 3, int),
    )
    cfg["DISCORD_WEBHOOK_URL"] = get_config_val(
        config, "DISCORD_WEBHOOK_URL", "notifications", "discord_webhook_url"
    )
    cfg["DISCORD_AVATAR_URL"] = (
        "https://raw.githubusercontent.com/drondeseries/omniscan/master/assets/logo.png"
    )
    cfg["DISCORD_WEBHOOK_NAME"] = "Omniscan"
    cfg["DISCORD_MENTION_USERS"] = [
        u.strip()
        for u in get_config_val(
            config, "DISCORD_MENTION_USERS", "notifications", "mention_users", ""
        ).split(",")
        if u.strip()
    ]
    cfg["DISCORD_MENTION_ROLES"] = [
        r.strip()
        for r in get_config_val(
            config, "DISCORD_MENTION_ROLES", "notifications", "mention_roles", ""
        ).split(",")
        if r.strip()
    ]
    cfg["DISCORD_MENTION_EVERYONE"] = get_config_val(
        config,
        "DISCORD_MENTION_EVERYONE",
        "notifications",
        "mention_everyone",
        "false",
        lambda x: str(x).lower() == "true",
    )
    cfg["DISCORD_MENTION_HERE"] = get_config_val(
        config,
        "DISCORD_MENTION_HERE",
        "notifications",
        "mention_here",
        "false",
        lambda x: str(x).lower() == "true",
    )
    cfg["DISCORD_MENTION_EVENTS"] = [
        e.strip().lower()
        for e in get_config_val(
            config,
            "DISCORD_MENTION_EVENTS",
            "notifications",
            "mention_events",
            "corrupt,stuck",
        ).split(",")
        if e.strip()
    ]

    cfg["SYMLINK_CHECK"] = get_config_val(
        config,
        "SYMLINK_CHECK",
        "behaviour",
        "symlink_check",
        "false",
        lambda x: str(x).lower() == "true",
    )
    cfg["EMPTY_TRASH"] = get_config_val(
        config,
        "EMPTY_TRASH",
        "behaviour",
        "empty_trash",
        "false",
        lambda x: str(x).lower() == "true",
    )
    cfg["NOTIFICATIONS_ENABLED"] = get_config_val(
        config,
        "NOTIFICATIONS_ENABLED",
        "notifications",
        "enabled",
        "true",
        lambda x: str(x).lower() == "true",
    )
    cfg["START_TIME"] = get_config_val(config, "START_TIME", "behaviour", "start_time")
    cfg["RUN_ON_STARTUP"] = get_config_val(
        config,
        "RUN_ON_STARTUP",
        "behaviour",
        "run_on_startup",
        "true",
        lambda x: str(x).lower() == "true",
    )
    cfg["DRY_RUN"] = get_config_val(
        config,
        "DRY_RUN",
        "behaviour",
        "dry_run",
        "false",
        lambda x: str(x).lower() == "true",
    )
    cfg["SCAN_WORKERS"] = get_config_val(
        config, "SCAN_WORKERS", "behaviour", "scan_workers", 4, int
    )
    cfg["SCAN_DEBOUNCE"] = get_config_val(
        config, "SCAN_DEBOUNCE", "behaviour", "scan_debounce", 30, int
    )
    cfg["NOTIFICATION_GROUP_WINDOW"] = get_config_val(
        config,
        "NOTIFICATION_GROUP_WINDOW",
        "behaviour",
        "notification_group_window",
        15,
        int,
    )
    cfg["WATCH_MODE"] = get_config_val(
        config,
        "WATCH_MODE",
        "behaviour",
        "watch",
        "false",
        lambda x: str(x).lower() == "true",
    )
    cfg["CLEANUP_DAYS"] = get_config_val(
        config, "CLEANUP_DAYS", "behaviour", "cleanup_days", 10, int
    )
    cfg["PLEX_ANALYZE"] = get_config_val(
        config,
        "PLEX_ANALYZE",
        "behaviour",
        "plex_analyze",
        "false",
        lambda x: str(x).lower() == "true",
    )
    cfg["PLEX_REFRESH"] = get_config_val(
        config,
        "PLEX_REFRESH",
        "behaviour",
        "plex_refresh",
        "false",
        lambda x: str(x).lower() == "true",
    )

    # New Features
    cfg["INCREMENTAL_SCAN"] = get_config_val(
        config,
        "INCREMENTAL_SCAN",
        "behaviour",
        "incremental_scan",
        "false",
        lambda x: str(x).lower() == "true",
    )
    cfg["SCAN_SINCE_DAYS"] = get_config_val(
        config, "SCAN_SINCE_DAYS", "behaviour", "scan_since_days", 7, int
    )
    cfg["SCAN_DELAY"] = get_config_val(
        config, "SCAN_DELAY", "behaviour", "scan_delay", 0.0, float
    )
    cfg["DELETION_THRESHOLD"] = get_config_val(
        config, "DELETION_THRESHOLD", "behaviour", "deletion_threshold", 50, int
    )
    cfg["ABORT_ON_MASS_DELETION"] = get_config_val(
        config,
        "ABORT_ON_MASS_DELETION",
        "behaviour",
        "abort_on_mass_deletion",
        "true",
        lambda x: str(x).lower() == "true",
    )
    cfg["INTEGRITY_CHECK"] = get_config_val(
        config,
        "INTEGRITY_CHECK",
        "behaviour",
        "integrity_check",
        "false",
        lambda x: str(x).lower() == "true",
    )
    cfg["FFPROBE_CHECK"] = get_config_val(
        config,
        "FFPROBE_CHECK",
        "behaviour",
        "ffprobe_check",
        "false",
        lambda x: str(x).lower() == "true",
    )

    # Web Security
    cfg["WEB_USERNAME"] = get_config_val(
        config, "WEB_USERNAME", "web", "username", "admin"
    )
    cfg["WEB_PASSWORD"] = get_config_val(config, "WEB_PASSWORD", "web", "password")
    # Set WEB_AUTH_DISABLED=true to allow access without login (e.g. trusted local network)
    cfg["WEB_AUTH_DISABLED"] = get_config_val(
        config,
        "WEB_AUTH_DISABLED",
        "web",
        "auth_disabled",
        "false",
        lambda x: str(x).lower() == "true",
    )
    cfg["WEBHOOK_TOKEN"] = get_config_val(
        config,
        "WEBHOOK_TOKEN",
        "web",
        "webhook_token",
        None,
    )
    if not cfg["WEBHOOK_TOKEN"]:
        cfg["WEBHOOK_TOKEN"] = secrets.token_urlsafe(32)
        if config_path:
            try:
                if not config.has_section("web"):
                    config.add_section("web")
                config.set("web", "webhook_token", cfg["WEBHOOK_TOKEN"])
                fd, temporary_path = tempfile.mkstemp(
                    prefix=".config.", dir=os.path.dirname(os.path.abspath(config_path)) or ".", text=True
                )
                os.fchmod(fd, stat.S_IRUSR | stat.S_IWUSR)
                with os.fdopen(fd, "w") as handle:
                    config.write(handle)
                    handle.flush()
                    os.fsync(handle.fileno())
                os.replace(temporary_path, config_path)
                os.chmod(config_path, stat.S_IRUSR | stat.S_IWUSR)
            except OSError:
                try:
                    os.unlink(temporary_path)
                except (OSError, UnboundLocalError):
                    pass

    # Parse Directories
    directories_raw = get_config_val(
        config, "SCAN_DIRECTORIES", "scan", "directories", ""
    )
    cfg["SCAN_PATHS"] = [
        path.strip()
        for path in directories_raw.replace("\n", ",").split(",")
        if path.strip()
    ]
    if cfg["SCAN_PATHS"]:
        cfg["SCAN_PATHS"].sort()

    # Parse Watch Directories (folders to enable real-time watching on)
    watch_dirs_raw = get_config_val(
        config, "WATCH_DIRECTORIES", "scan", "watch_directories", ""
    )
    cfg["WATCH_DIRECTORIES"] = [
        path.strip()
        for path in watch_dirs_raw.replace("\n", ",").split(",")
        if path.strip()
    ]
    if cfg["WATCH_DIRECTORIES"]:
        cfg["WATCH_DIRECTORIES"].sort()

    # Parse Path Rewrites (from Autopulse features)
    rewrites_raw = get_config_val(config, "PATH_REWRITES", "rewrite", "mappings", "")
    cfg["PATH_REWRITES"] = []
    for line in rewrites_raw.replace(",", "\n").split("\n"):
        line = line.strip()
        if not line:
            continue
        if ":" in line:
            parts = line.split(":", 1)
            cfg["PATH_REWRITES"].append((parts[0].strip(), parts[1].strip()))

    # Parse Ignore Patterns
    ignore_patterns_raw = get_config_val(
        config, "IGNORE_PATTERNS", "ignore", "patterns", ""
    )
    cfg["IGNORE_PATTERNS"] = [
        p.strip()
        for p in ignore_patterns_raw.replace("\n", ",").split(",")
        if p.strip()
    ]

    # MEDIA_EXTENSIONS: all file types that trigger a Plex folder scan.
    # Subtitles are included so Plex re-scans when a new .srt appears.
    cfg["MEDIA_EXTENSIONS"] = {
        # Video
        ".mp4",
        ".mkv",
        ".avi",
        ".mov",
        ".wmv",
        ".flv",
        ".webm",
        ".m4v",
        ".m4p",
        ".m4b",
        ".m4r",
        ".3gp",
        ".mpg",
        ".mpeg",
        ".m2v",
        ".m2ts",
        ".ts",
        ".vob",
        ".iso",
        ".strm",
        # Audio
        ".mp3",
        ".flac",
        ".m4a",
        ".wav",
        ".ogg",
        ".opus",
        ".wma",
        # Subtitles (trigger folder scan only, not library check)
        ".srt",
        ".sub",
        ".ass",
        ".vtt",
    }

    # Default LIBRARY_EXTENSIONS: file types that the media server actually
    # indexes as individual library items. Only these are checked against
    # is_in_library() to determine if a file is missing.
    #
    # Subtitle sidecars (.srt/.sub/.ass/.vtt) are intentionally excluded —
    # Plex/Jellyfin never stores subtitle paths as library entries, so checking
    # them would cause every subtitle to appear "missing" and pile up as stuck.
    #
    # Override via [scan] library_extensions in config.ini or the
    # LIBRARY_EXTENSIONS env var (comma-separated list of extensions with dots):
    #   e.g. library_extensions = .mkv,.mp4,.mp3,.flac,.epub,.mobi
    _DEFAULT_LIBRARY_EXTENSIONS = (
        # Video
        ".mp4,.mkv,.avi,.mov,.wmv,.flv,.webm"
        ",.m4v,.m4p,.m4b,.m4r,.3gp,.mpg,.mpeg"
        ",.m2v,.m2ts,.ts,.vob,.iso,.strm"
        # Audio
        ",.mp3,.flac,.m4a,.wav,.ogg,.opus,.wma"
    )
    library_ext_raw = get_config_val(
        config,
        "LIBRARY_EXTENSIONS",
        "scan",
        "library_extensions",
        _DEFAULT_LIBRARY_EXTENSIONS,
    )
    cfg["LIBRARY_EXTENSIONS"] = {
        e.strip().lower() if e.strip().startswith(".") else "." + e.strip().lower()
        for e in library_ext_raw.replace("\n", ",").split(",")
        if e.strip()
    }

    return cfg


def get_webhook_token(password=None, configured_token=None):
    if configured_token:
        return configured_token
    if password:
        import hashlib

        return hashlib.sha256(f"omniscan-webhook-{password}".encode()).hexdigest()[:16]
    return None


def save_config(config, config_path="config.ini"):
    directory = os.path.dirname(os.path.abspath(config_path)) or "."
    parser = configparser.ConfigParser(interpolation=None)
    if os.path.exists(config_path):
        parser.read(config_path)
    for section, values in config.items():
        if not parser.has_section(section):
            parser.add_section(section)
        for key, value in values.items():
            parser.set(section, key, str(value))
    fd, temporary_path = tempfile.mkstemp(prefix=".config.", dir=directory, text=True)
    try:
        os.fchmod(fd, stat.S_IRUSR | stat.S_IWUSR)
        with os.fdopen(fd, "w") as handle:
            parser.write(handle)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_path, config_path)
        os.chmod(config_path, stat.S_IRUSR | stat.S_IWUSR)
    except Exception:
        try:
            os.unlink(temporary_path)
        except OSError:
            pass
        raise


def _int_str(val, default):
    try:
        return str(int(float(val)))
    except (ValueError, TypeError):
        return str(default)


def _float_str(val, default):
    try:
        return str(float(val))
    except (ValueError, TypeError):
        return str(default)


def _bool_str(val, default=False):
    if isinstance(val, str):
        return str(val.lower() in ("true", "1", "yes")).lower()
    return str(bool(val)).lower()


def format_config_sections(c):
    """Convert flat runtime configuration dictionary to section-based dictionary for config.ini."""
    sections = {
        "server": {
            "type": str(c.get("SERVER_TYPE", "plex")).lower(),
            "url": str(c.get("SERVER_URL", "")),
            "api_key": str(c.get("API_KEY", "")),
        },
        "plex": {
            "server": str(c.get("PLEX_URL", "")),
            "token": str(c.get("TOKEN", "")),
        },
        "behaviour": {
            "scan_workers": _int_str(c.get("SCAN_WORKERS"), 4),
            "scan_debounce": _int_str(c.get("SCAN_DEBOUNCE"), 10),
            "scan_delay": _float_str(c.get("SCAN_DELAY"), 0.0),
            "watch": _bool_str(c.get("WATCH_MODE"), False),
            "run_interval": _int_str(c.get("RUN_INTERVAL"), 24),
            "run_interval_unit": str(c.get("RUN_INTERVAL_UNIT", "hours")).lower(),
            "max_retries": _int_str(c.get("MAX_RETRIES"), 3),
            "run_on_startup": _bool_str(c.get("RUN_ON_STARTUP"), True),
            "start_time": str(c.get("START_TIME", "") or ""),
            "incremental_scan": _bool_str(c.get("INCREMENTAL_SCAN"), False),
            "scan_since_days": _int_str(c.get("SCAN_SINCE_DAYS"), 7),
            "symlink_check": _bool_str(c.get("SYMLINK_CHECK"), False),
            "empty_trash": _bool_str(c.get("EMPTY_TRASH"), False),
            "integrity_check": _bool_str(c.get("INTEGRITY_CHECK"), False),
            "ffprobe_check": _bool_str(c.get("FFPROBE_CHECK"), False),
            "deletion_threshold": _int_str(c.get("DELETION_THRESHOLD"), 50),
            "abort_on_mass_deletion": _bool_str(c.get("ABORT_ON_MASS_DELETION"), True),
            "notification_group_window": _int_str(c.get("NOTIFICATION_GROUP_WINDOW"), 15),
            "cleanup_days": _int_str(c.get("CLEANUP_DAYS"), 10),
            "plex_analyze": _bool_str(c.get("PLEX_ANALYZE"), False),
            "plex_refresh": _bool_str(c.get("PLEX_REFRESH"), False),
        },
        "notifications": {
            "enabled": str(c.get("NOTIFICATIONS_ENABLED", True)).lower(),
            "discord_webhook_url": str(c.get("DISCORD_WEBHOOK_URL", "")),
            "mention_users": ",".join(c.get("DISCORD_MENTION_USERS", []))
            if isinstance(c.get("DISCORD_MENTION_USERS"), (list, tuple))
            else str(c.get("DISCORD_MENTION_USERS", "")),
            "mention_roles": ",".join(c.get("DISCORD_MENTION_ROLES", []))
            if isinstance(c.get("DISCORD_MENTION_ROLES"), (list, tuple))
            else str(c.get("DISCORD_MENTION_ROLES", "")),
            "mention_everyone": str(c.get("DISCORD_MENTION_EVERYONE", False)).lower(),
            "mention_here": str(c.get("DISCORD_MENTION_HERE", False)).lower(),
            "mention_events": ",".join(c.get("DISCORD_MENTION_EVENTS", []))
            if isinstance(c.get("DISCORD_MENTION_EVENTS"), (list, tuple))
            else str(c.get("DISCORD_MENTION_EVENTS", "")),
        },
        "scan": {
            "directories": ",".join(c.get("SCAN_PATHS", []))
            if isinstance(c.get("SCAN_PATHS"), (list, tuple))
            else str(c.get("SCAN_PATHS", "")),
            "watch_directories": ",".join(c.get("WATCH_DIRECTORIES", []))
            if isinstance(c.get("WATCH_DIRECTORIES"), (list, tuple))
            else str(c.get("WATCH_DIRECTORIES", "")),
        },
        "ignore": {
            "patterns": ",".join(c.get("IGNORE_PATTERNS", []))
            if isinstance(c.get("IGNORE_PATTERNS"), (list, tuple))
            else str(c.get("IGNORE_PATTERNS", "")),
        },
        "logs": {
            "loglevel": str(c.get("LOG_LEVEL", "INFO")),
        },
        "rewrite": {
            "mappings": ",".join(
                [f"{src}:{dst}" for src, dst in c.get("PATH_REWRITES", [])]
            )
            if isinstance(c.get("PATH_REWRITES"), list)
            else str(c.get("PATH_REWRITES", "")),
        },
    }
    web_sec = {}
    if "WEB_USERNAME" in c:
        web_sec["username"] = str(c.get("WEB_USERNAME", "admin"))
    if "WEB_PASSWORD" in c and c["WEB_PASSWORD"]:
        web_sec["password"] = str(c.get("WEB_PASSWORD", ""))
    if "WEB_AUTH_DISABLED" in c:
        web_sec["auth_disabled"] = str(c.get("WEB_AUTH_DISABLED", False)).lower()
    if "WEBHOOK_TOKEN" in c and c["WEBHOOK_TOKEN"]:
        web_sec["webhook_token"] = str(c.get("WEBHOOK_TOKEN", ""))
    if web_sec:
        sections["web"] = web_sec

    return sections


def persist_settings(c, config_path="config.ini"):
    """Atomically validates and saves flat runtime configuration to config.ini."""
    sections = format_config_sections(c)
    save_config(sections, config_path=config_path)


def apply_and_save_settings(scanner, settings_dict, config_path="config.ini"):
    """Atomically validates, persists, and publishes candidate settings to runtime and scheduler."""
    candidate = dict(scanner.config if scanner and hasattr(scanner, "config") else {})

    for key, val in settings_dict.items():
        if val is not None:
            candidate[key] = val

    # Normalization & validation
    if "SERVER_TYPE" in candidate:
        candidate["SERVER_TYPE"] = str(candidate["SERVER_TYPE"]).lower()
    if "SERVER_URL" in candidate:
        candidate["SERVER_URL"] = normalize_emby_url(
            candidate["SERVER_URL"], candidate.get("SERVER_TYPE")
        )
    if "SCAN_SINCE_DAYS" in candidate:
        try:
            candidate["SCAN_SINCE_DAYS"] = max(0, int(float(candidate["SCAN_SINCE_DAYS"])))
        except (ValueError, TypeError):
            candidate["SCAN_SINCE_DAYS"] = 7
    if "NOTIFICATION_GROUP_WINDOW" in candidate:
        try:
            candidate["NOTIFICATION_GROUP_WINDOW"] = max(1, int(float(candidate["NOTIFICATION_GROUP_WINDOW"])))
        except (ValueError, TypeError):
            candidate["NOTIFICATION_GROUP_WINDOW"] = 15
    if "RUN_INTERVAL" in candidate:
        try:
            candidate["RUN_INTERVAL"] = max(1, int(float(candidate["RUN_INTERVAL"])))
        except (ValueError, TypeError):
            candidate["RUN_INTERVAL"] = 24
    if "RUN_INTERVAL_UNIT" in candidate:
        unit = str(candidate["RUN_INTERVAL_UNIT"]).lower()
        candidate["RUN_INTERVAL_UNIT"] = unit if unit in ("hours", "minutes") else "hours"
    if "START_TIME" in candidate:
        candidate["START_TIME"] = str(candidate["START_TIME"] or "").strip()
    if "MAX_RETRIES" in candidate:
        try:
            candidate["MAX_RETRIES"] = max(1, int(float(candidate["MAX_RETRIES"])))
        except (ValueError, TypeError):
            candidate["MAX_RETRIES"] = 3
    if "SCAN_WORKERS" in candidate:
        try:
            candidate["SCAN_WORKERS"] = max(1, min(64, int(float(candidate["SCAN_WORKERS"]))))
        except (ValueError, TypeError):
            candidate["SCAN_WORKERS"] = 4
    if "SCAN_DEBOUNCE" in candidate:
        try:
            candidate["SCAN_DEBOUNCE"] = max(0, int(float(candidate["SCAN_DEBOUNCE"])))
        except (ValueError, TypeError):
            candidate["SCAN_DEBOUNCE"] = 10
    if "SCAN_DELAY" in candidate:
        try:
            candidate["SCAN_DELAY"] = max(0.0, float(candidate["SCAN_DELAY"]))
        except (ValueError, TypeError):
            candidate["SCAN_DELAY"] = 0.0
    if "CLEANUP_DAYS" in candidate:
        try:
            candidate["CLEANUP_DAYS"] = max(1, int(float(candidate["CLEANUP_DAYS"])))
        except (ValueError, TypeError):
            candidate["CLEANUP_DAYS"] = 10
    if "DELETION_THRESHOLD" in candidate:
        try:
            candidate["DELETION_THRESHOLD"] = max(1, int(float(candidate["DELETION_THRESHOLD"])))
        except (ValueError, TypeError):
            candidate["DELETION_THRESHOLD"] = 50

    # 1. Atomic disk persistence FIRST
    persist_settings(candidate, config_path=config_path)

    # 2. Disk persistence succeeded: publish to runtime scanner
    runtime_warnings = []
    if scanner and hasattr(scanner, "config"):
        scanner.config.update(candidate)

        # 3. Update stuck file tracker
        if hasattr(scanner, "history"):
            try:
                scanner.history.max_retries = candidate.get("MAX_RETRIES", 3)
            except Exception as e:
                runtime_warnings.append(f"Tracker update warning: {e}")

        # 4. Reschedule safely (contain any scheduler exception)
        if hasattr(scanner, "reschedule"):
            try:
                res = scanner.reschedule(
                    interval=candidate.get("RUN_INTERVAL", 24),
                    interval_unit=candidate.get("RUN_INTERVAL_UNIT", "hours"),
                    start_time=candidate.get("START_TIME", ""),
                )
                if isinstance(res, tuple) and len(res) == 2 and not res[0]:
                    runtime_warnings.append(f"Scheduler update issue: {res[1]}")
            except Exception as sched_ex:
                logging.getLogger(__name__).error(f"Error during rescheduling: {sched_ex}")
                runtime_warnings.append(f"Scheduler update error: {sched_ex}")

        # 5. Server reconnect / listener restart (contained)
        st = (candidate.get("SERVER_TYPE") or "").lower()
        if st == "plex":
            try:
                server = scanner.connect_to_plex(retry=False)
                if not server or not getattr(scanner, "plex", None):
                    runtime_warnings.append("Could not connect to Plex server")
                else:
                    libs = scanner.get_library_ids()
                    if not libs or not getattr(scanner, "plex", None):
                        runtime_warnings.append("Connected to Plex but failed to discover libraries")
            except Exception as e:
                logging.getLogger(__name__).warning(f"Error reconnecting to Plex on settings save: {e}")
                runtime_warnings.append(f"Plex reconnect warning: {e}")
        elif st in ["jellyfin", "emby"]:
            try:
                if hasattr(scanner, "restart_jellyfin_alert_listener"):
                    scanner.restart_jellyfin_alert_listener()
            except Exception as e:
                logging.getLogger(__name__).warning(f"Error restarting Jellyfin listener: {e}")
                runtime_warnings.append(f"Jellyfin listener restart warning: {e}")

    applied_ok = len(runtime_warnings) == 0
    warning_msg = "; ".join(runtime_warnings) if runtime_warnings else None
    return candidate, applied_ok, warning_msg
