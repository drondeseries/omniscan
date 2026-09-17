import os
import tempfile
import unittest
from unittest.mock import MagicMock, patch
from omniscan_pkg.scanner import PlexScanner
from omniscan_pkg.models import RunStats, StuckFileTracker
import logging

# Disable logging for tests
logging.disable(logging.CRITICAL)


class TestPlexScanner(unittest.TestCase):
    def setUp(self):
        self.config = {
            "PLEX_URL": "http://mock:32400",
            "TOKEN": "mock_token",
            "IGNORE_PATTERNS": ["*.tmp", "sample*"],
            "MEDIA_EXTENSIONS": {".mkv", ".mp4"},
            "LIBRARY_EXTENSIONS": {".mkv", ".mp4"},
            "SYMLINK_CHECK": True,
            "SCAN_PATHS": ["/data"],
            "NOTIFICATIONS_ENABLED": False,
            "SCAN_DELAY": 0.0,
            "INCREMENTAL_SCAN": False,
            "HEALTH_CHECK": False,
            "SCAN_WORKERS": 4,
            "SERVER_TYPE": "plex",
        }
        self.temp_dir = tempfile.TemporaryDirectory()
        self.config["HISTORY_DB"] = os.path.join(self.temp_dir.name, "history.db")
        self.scanner = PlexScanner(self.config)

    def tearDown(self):
        self.scanner.event_executor.shutdown(wait=True)
        self.scanner.scan_monitor_executor.shutdown(wait=True)
        self.scanner.temp_dir = getattr(self.scanner, "temp_dir", None)
        self.temp_dir.cleanup()

    def test_integrity_timeout_is_quarantined_until_file_changes(self):
        file_path = os.path.join(self.temp_dir.name, "broken.mkv")
        with open(file_path, "wb") as handle:
            handle.write(b"media")

        tracker = self.scanner.history
        tracker.quarantine_integrity_failure(file_path, "ffprobe timeout")
        self.assertTrue(tracker.is_quarantined(file_path))

        with open(file_path, "ab") as handle:
            handle.write(b"changed")
        self.assertFalse(tracker.is_quarantined(file_path))

    @patch("subprocess.run")
    def test_ffprobe_timeout_is_reported(self, mock_run):
        import subprocess

        mock_run.side_effect = subprocess.TimeoutExpired("ffprobe", 30)
        self.scanner.config["INTEGRITY_CHECK"] = True
        self.scanner.config["FFPROBE_CHECK"] = True
        file_path = os.path.join(self.temp_dir.name, "timeout.mkv")
        with open(file_path, "wb") as handle:
            handle.write(b"media")
        self.assertEqual(self.scanner.check_file_integrity(file_path), (False, "ffprobe timeout"))

    def test_is_ignored(self):
        self.assertTrue(self.scanner.is_ignored("/path/to/sample_file.mkv"))
        self.assertTrue(self.scanner.is_ignored("/path/to/file.tmp"))
        self.assertFalse(self.scanner.is_ignored("/path/to/movie.mkv"))

    @patch("omniscan_pkg.scanner.PlexServer")
    def test_connect_to_plex_success(self, MockPlex):
        mock_instance = MockPlex.return_value
        mock_instance.friendlyName = "MockServer"
        mock_instance.version = "1.0"

        server = self.scanner.connect_to_plex()
        self.assertEqual(server, mock_instance)
        self.assertIsNotNone(self.scanner.plex)

    @patch("omniscan_pkg.scanner.PlexServer")
    def test_connect_to_plex_retry(self, MockPlex):
        # Fail once then succeed
        MockPlex.side_effect = [
            Exception("Connection failed"),
            MagicMock(friendlyName="MockServer", version="1.0"),
        ]

        with patch("time.sleep") as mock_sleep:
            server = self.scanner.connect_to_plex()
            self.assertEqual(mock_sleep.call_count, 1)
            self.assertIsNotNone(server)

    def test_is_broken_symlink(self):
        with patch("os.path.islink", return_value=True), patch(
            "os.path.exists", return_value=False
        ), patch("os.path.realpath", return_value="/broken/path"):
            self.assertTrue(self.scanner.is_broken_symlink("/path/to/link"))

        with patch("os.path.islink", return_value=True), patch(
            "os.path.exists", return_value=True
        ):
            self.assertFalse(self.scanner.is_broken_symlink("/path/to/valid_link"))

    @patch("os.scandir")
    @patch("os.path.getsize")
    def test_scan_directory(self, mock_getsize, mock_scandir):
        mock_movie = MagicMock()
        mock_movie.name = "movie.mkv"
        mock_movie.path = "/data/movie.mkv"
        mock_movie.is_dir.return_value = False
        mock_movie.is_file.return_value = True

        mock_ignored = MagicMock()
        mock_ignored.name = "ignored.tmp"
        mock_ignored.path = "/data/ignored.tmp"
        mock_ignored.is_dir.return_value = False
        mock_ignored.is_file.return_value = True

        mock_txt = MagicMock()
        mock_txt.name = "text.txt"
        mock_txt.path = "/data/text.txt"
        mock_txt.is_dir.return_value = False
        mock_txt.is_file.return_value = True

        mock_scandir.return_value.__enter__.return_value = [
            mock_movie,
            mock_ignored,
            mock_txt,
        ]
        mock_getsize.return_value = 1000

        # Mock is_in_library to return False (missing)
        self.scanner.is_in_library = MagicMock(return_value=False)
        self.scanner.get_library_id_for_path = MagicMock(
            return_value=("1", "Movies", "movie")
        )

        stats = RunStats(self.config)
        tracker = StuckFileTracker()
        tracker._load_history = MagicMock(return_value={})
        tracker.increment_attempt = MagicMock(return_value=False)  # Not stuck
        tracker.lock = MagicMock()
        tracker.lock.__enter__ = MagicMock()
        tracker.lock.__exit__ = MagicMock()

        folders_to_scan = set()
        lock = MagicMock()
        lock.__enter__ = MagicMock()
        lock.__exit__ = MagicMock()

        self.scanner.scan_directory("/data", stats, tracker, folders_to_scan, lock)

        # Verify stats
        self.assertEqual(stats.total_scanned, 1)  # Only movie.mkv
        self.assertEqual(stats.total_missing, 1)
        self.assertEqual(len(folders_to_scan), 1)

    def test_calculate_missing_files_ignores_broken_symlink(self):
        self.scanner.library_sections_cache = [
            {"id": "1", "title": "Movies", "type": "movie", "locations": ["/data"]}
        ]
        self.scanner.library_files = {"1": set()}

        def mock_exists(path):
            if path == "/data":
                return True
            if path == "/data/movie.mkv":
                return True
            if path == "/data/broken.mkv":
                return False
            return False

        with patch("os.path.exists", side_effect=mock_exists), patch(
            "os.walk", return_value=[("/data", [], ["movie.mkv", "broken.mkv"])]
        ), patch("os.path.islink", side_effect=lambda p: p == "/data/broken.mkv"):

            missing_cnt = self.scanner.calculate_missing_files_for_library("1")

            self.assertEqual(missing_cnt, 1)
            self.assertIn("/data/movie.mkv", self.scanner.library_missing_files["1"])
            self.assertNotIn(
                "/data/broken.mkv", self.scanner.library_missing_files["1"]
            )

    def test_jellyfin_headers_format(self):
        self.scanner.config["API_KEY"] = "secret_test_token"
        headers = self.scanner._get_jellyfin_headers()
        self.assertEqual(headers["X-Emby-Token"], "secret_test_token")
        self.assertEqual(headers["X-MediaBrowser-Token"], "secret_test_token")
        self.assertIn('Token="secret_test_token"', headers["Authorization"])
        self.assertIn('Client="Omniscan"', headers["Authorization"])
        self.assertIn('DeviceId="omniscan"', headers["Authorization"])
        self.assertEqual(headers["Accept"], "application/json")
        self.assertEqual(headers["Content-Type"], "application/json")

    @patch("omniscan_pkg.scanner.websocket")
    def test_jellyfin_websocket_connection_headers(self, mock_ws):
        self.scanner.config["SERVER_TYPE"] = "jellyfin"
        self.scanner.config["SERVER_URL"] = "http://jellyfin.local:8096"
        self.scanner.config["API_KEY"] = "my_api_token"

        def side_effect(*args, **kwargs):
            self.scanner.jellyfin_ws_stop.set()
            raise Exception("Stop listener")

        mock_ws.create_connection.side_effect = side_effect

        with patch("time.sleep"):
            self.scanner._start_jellyfin_alert_listener()

        self.assertTrue(mock_ws.create_connection.called)
        call_args, call_kwargs = mock_ws.create_connection.call_args
        endpoint = call_args[0]
        self.assertTrue(endpoint.startswith("ws://jellyfin.local:8096/socket"))
        self.assertIn("api_key=my_api_token", endpoint)
        self.assertIn("ApiKey=my_api_token", endpoint)
        self.assertIn("token=my_api_token", endpoint)
        self.assertIn("deviceId=omniscan", endpoint)

        headers = call_kwargs.get("header")
        self.assertIsNotNone(headers)
        self.assertIn("X-Emby-Token: my_api_token", headers)
        self.assertIn("X-MediaBrowser-Token: my_api_token", headers)
        self.assertTrue(
            any(
                "Authorization: MediaBrowser" in h
                and 'Token="my_api_token"' in h
                and 'Client="Omniscan"' in h
                for h in headers
            )
        )

    @patch("omniscan_pkg.scanner.websocket")
    def test_emby_websocket_connection_endpoint(self, mock_ws):
        self.scanner.config["SERVER_TYPE"] = "emby"
        self.scanner.config["SERVER_URL"] = "https://emby.local:8096"
        self.scanner.config["API_KEY"] = "emby_token"

        def side_effect(*args, **kwargs):
            self.scanner.jellyfin_ws_stop.set()
            raise Exception("Stop listener")

        mock_ws.create_connection.side_effect = side_effect

        with patch("time.sleep"):
            self.scanner._start_jellyfin_alert_listener()

        call_args, call_kwargs = mock_ws.create_connection.call_args
        endpoint = call_args[0]
        self.assertTrue(endpoint.startswith("wss://emby.local:8096/embywebsocket"))
        self.assertIn("api_key=emby_token", endpoint)
        self.assertIn("ApiKey=emby_token", endpoint)
        self.assertIn("token=emby_token", endpoint)

    @patch("omniscan_pkg.scanner.WEBSOCKET_SUPPORTED", False)
    def test_jellyfin_websocket_unsupported(self):
        self.scanner.config["SERVER_TYPE"] = "jellyfin"
        self.scanner.config["SERVER_URL"] = "http://jellyfin.local:8096"
        self.scanner.config["API_KEY"] = "my_api_token"
        # Should return early cleanly without error
        self.scanner._start_jellyfin_alert_listener()

    def test_try_plugin_scan_success(self):
        self.scanner.config["SERVER_URL"] = "http://jellyfin.local:8096"
        self.scanner.config["API_KEY"] = "token123"
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "ItemId": "item-abc-123",
            "Status": "Created",
            "Message": "Created successfully",
        }
        with patch.object(
            self.scanner.http_session, "post", return_value=mock_response
        ) as mock_post, patch.object(
            self.scanner.scan_monitor_executor, "submit"
        ) as mock_submit:
            res = self.scanner._try_plugin_scan("/media/movie.mkv", None)
            self.assertTrue(res)
            mock_post.assert_called_once()
            args, kwargs = mock_post.call_args
            self.assertEqual(args[0], "http://jellyfin.local:8096/Library/ScanPath")
            self.assertEqual(kwargs["json"], {"Path": "/media/movie.mkv"})
            self.assertIn("Authorization", kwargs["headers"])
            mock_submit.assert_called_once_with(
                self.scanner.refresh_jellyfin_item, "item-abc-123"
            )


if __name__ == "__main__":
    unittest.main()
