import os
import configparser
import logging

def get_config_val(config, env_key, config_section, config_key, fallback=None, cast_func=None):
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
            logging.warning(f"Invalid value for {env_key}/{config_key}: {val}. Using fallback: {fallback}")
            return fallback
    return val

def load_config(config_path='config.ini'):
    config = configparser.ConfigParser()
    config.read(config_path)
    
    cfg = {}

    # Load config with Env Var overrides
    cfg['SERVER_TYPE'] = get_config_val(config, 'SERVER_TYPE', 'server', 'type', 'plex').lower()
    cfg['PLEX_URL'] = get_config_val(config, 'PLEX_SERVER', 'plex', 'server')
    cfg['TOKEN'] = get_config_val(config, 'PLEX_TOKEN', 'plex', 'token')
    
    # Generic Server support (Emby/Jellyfin)
    cfg['SERVER_URL'] = get_config_val(config, 'SERVER_URL', 'server', 'url')
    cfg['API_KEY'] = get_config_val(config, 'API_KEY', 'server', 'api_key')
    
    cfg['LOG_LEVEL'] = get_config_val(config, 'LOG_LEVEL', 'logs', 'loglevel', 'INFO')
    cfg['SCAN_INTERVAL'] = get_config_val(config, 'SCAN_INTERVAL', 'behaviour', 'scan_interval', 15, int)
    cfg['RUN_INTERVAL'] = get_config_val(config, 'RUN_INTERVAL', 'behaviour', 'run_interval', 24, int)
    cfg['DISCORD_WEBHOOK_URL'] = get_config_val(config, 'DISCORD_WEBHOOK_URL', 'notifications', 'discord_webhook_url')
    cfg['DISCORD_AVATAR_URL'] = "https://raw.githubusercontent.com/drondeseries/omniscan/master/assets/logo.png"
    cfg['DISCORD_WEBHOOK_NAME'] = "Omniscan"
    cfg['SYMLINK_CHECK'] = get_config_val(config, 'SYMLINK_CHECK', 'behaviour', 'symlink_check', 'false', lambda x: str(x).lower() == 'true')
    cfg['NOTIFICATIONS_ENABLED'] = get_config_val(config, 'NOTIFICATIONS_ENABLED', 'notifications', 'enabled', 'true', lambda x: str(x).lower() == 'true')
    cfg['START_TIME'] = get_config_val(config, 'START_TIME', 'behaviour', 'start_time')
    cfg['RUN_ON_STARTUP'] = get_config_val(config, 'RUN_ON_STARTUP', 'behaviour', 'run_on_startup', 'true', lambda x: str(x).lower() == 'true')
    cfg['DRY_RUN'] = get_config_val(config, 'DRY_RUN', 'behaviour', 'dry_run', 'false', lambda x: str(x).lower() == 'true')
    cfg['SCAN_WORKERS'] = get_config_val(config, 'SCAN_WORKERS', 'behaviour', 'scan_workers', 4, int)
    cfg['SCAN_DEBOUNCE'] = get_config_val(config, 'SCAN_DEBOUNCE', 'behaviour', 'scan_debounce', 10, int)
    cfg['USE_POLLING'] = get_config_val(config, 'USE_POLLING', 'behaviour', 'use_polling', 'false', lambda x: str(x).lower() == 'true')
    cfg['WATCH_MODE'] = get_config_val(config, 'WATCH_MODE', 'behaviour', 'watch', 'false', lambda x: str(x).lower() == 'true')
    
    # New Features
    cfg['INCREMENTAL_SCAN'] = get_config_val(config, 'INCREMENTAL_SCAN', 'behaviour', 'incremental_scan', 'false', lambda x: str(x).lower() == 'true')
    cfg['SCAN_SINCE_DAYS'] = get_config_val(config, 'SCAN_SINCE_DAYS', 'behaviour', 'scan_since_days', 7, int)
    cfg['HEALTH_CHECK'] = get_config_val(config, 'HEALTH_CHECK', 'behaviour', 'health_check', 'false', lambda x: str(x).lower() == 'true')
    cfg['IGNORE_SAMPLES'] = get_config_val(config, 'IGNORE_SAMPLES', 'behaviour', 'ignore_samples', 'false', lambda x: str(x).lower() == 'true')
    cfg['MIN_DURATION'] = get_config_val(config, 'MIN_DURATION', 'behaviour', 'min_duration', 180, int)
    cfg['SCAN_TIMEOUT'] = get_config_val(config, 'SCAN_TIMEOUT', 'behaviour', 'scan_timeout', 60, int)
    cfg['SCAN_DELAY'] = get_config_val(config, 'SCAN_DELAY', 'behaviour', 'scan_delay', 0.0, float)
    cfg['DELETION_THRESHOLD'] = get_config_val(config, 'DELETION_THRESHOLD', 'behaviour', 'deletion_threshold', 50, int)
    cfg['ABORT_ON_MASS_DELETION'] = get_config_val(config, 'ABORT_ON_MASS_DELETION', 'behaviour', 'abort_on_mass_deletion', 'true', lambda x: str(x).lower() == 'true')

    # Web Security
    cfg['WEB_USERNAME'] = get_config_val(config, 'WEB_USERNAME', 'web', 'username', 'admin')
    cfg['WEB_PASSWORD'] = get_config_val(config, 'WEB_PASSWORD', 'web', 'password')

    # Parse Directories
    directories_raw = get_config_val(config, 'SCAN_DIRECTORIES', 'scan', 'directories', '')
    cfg['SCAN_PATHS'] = [path.strip() for path in directories_raw.replace('\n', ',').split(',') if path.strip()]
    if cfg['SCAN_PATHS']:
        cfg['SCAN_PATHS'].sort()

    # Parse Ignore Patterns
    ignore_patterns_raw = get_config_val(config, 'IGNORE_PATTERNS', 'ignore', 'patterns', '')
    cfg['IGNORE_PATTERNS'] = [p.strip() for p in ignore_patterns_raw.replace('\n', ',').split(',') if p.strip()]
    
    # Media extensions
    cfg['MEDIA_EXTENSIONS'] = {
        '.mp4', '.mkv', '.avi', '.mov', '.wmv', '.flv', '.webm',
        '.m4v', '.m4p', '.m4b', '.m4r', '.3gp', '.mpg', '.mpeg',
        '.m2v', '.m2ts', '.ts', '.vob', '.iso'
    }

    return cfg
