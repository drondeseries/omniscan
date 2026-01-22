from prometheus_client import Counter, Gauge, Histogram, REGISTRY

# Define Metrics
SCANNED_FILES_TOTAL = Counter('omniscan_scanned_files_total', 'Total number of files scanned')
MISSING_FILES_TOTAL = Counter('omniscan_missing_files_total', 'Total number of missing files detected')
TRIGGERED_SCANS_TOTAL = Counter('omniscan_triggered_scans_total', 'Total number of media server scans triggered')
SCAN_ERRORS_TOTAL = Counter('omniscan_scan_errors_total', 'Total number of scan errors')
WATCHED_DIRECTORIES = Gauge('omniscan_watched_directories', 'Number of directories currently being watched')
PENDING_SCANS = Gauge('omniscan_pending_scans', 'Number of scans currently pending (debouncing)')
HEALTH_CHECKS_TOTAL = Counter('omniscan_health_checks_total', 'Total number of file health checks performed')
HEALTH_CHECK_FAILURES = Counter('omniscan_health_check_failures', 'Total number of failed health checks')
SCAN_DURATION_SECONDS = Histogram('omniscan_scan_duration_seconds', 'Time spent scanning directories')

def init_metrics():
    """Initialize metrics (optional, mostly for registry setup if needed)"""
    pass
