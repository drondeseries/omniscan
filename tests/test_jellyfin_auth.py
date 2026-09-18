import unittest
from unittest.mock import MagicMock, patch
from omniscan_pkg.config import (
    get_jellyfin_headers,
    get_jellyfin_params,
    normalize_emby_url,
)
from omniscan_pkg.scanner import PlexScanner


class TestJellyfinAuthentication(unittest.TestCase):
    def test_get_jellyfin_headers_with_valid_token(self):
        token = "test_token_12345"
        headers = get_jellyfin_headers(token)

        self.assertEqual(headers["X-Emby-Token"], token)
        self.assertEqual(headers["X-MediaBrowser-Token"], token)
        self.assertEqual(headers["Accept"], "application/json")
        self.assertEqual(headers["Content-Type"], "application/json")

        # Must include MediaBrowser scheme with Client, Device, DeviceId, Version, Token
        auth = headers["Authorization"]
        self.assertTrue(auth.startswith("MediaBrowser "))
        self.assertIn('Client="Omniscan"', auth)
        self.assertIn('Device="Omniscan"', auth)
        self.assertIn('DeviceId="omniscan"', auth)
        self.assertIn('Version="1.0.0"', auth)
        self.assertIn(f'Token="{token}"', auth)

        # Must also include X-Emby-Authorization for reverse proxies that strip Authorization
        emby_auth = headers["X-Emby-Authorization"]
        self.assertTrue(emby_auth.startswith("MediaBrowser "))
        self.assertIn(f'Token="{token}"', emby_auth)

    def test_get_jellyfin_headers_strips_whitespace(self):
        token = "   test_token_with_whitespace \n\t "
        headers = get_jellyfin_headers(token)
        clean = "test_token_with_whitespace"
        self.assertEqual(headers["X-Emby-Token"], clean)
        self.assertEqual(headers["X-MediaBrowser-Token"], clean)
        self.assertIn(f'Token="{clean}"', headers["Authorization"])

    def test_get_jellyfin_headers_empty_or_none(self):
        for empty_val in ["", None, "   "]:
            headers = get_jellyfin_headers(empty_val)
            self.assertEqual(headers["X-Emby-Token"], "")
            self.assertEqual(headers["X-MediaBrowser-Token"], "")
            self.assertIn('Token=""', headers["Authorization"])

    def test_get_jellyfin_params_with_token(self):
        token = "test_token_12345"
        params = get_jellyfin_params(token)
        self.assertEqual(params["ApiKey"], token)
        self.assertEqual(params["api_key"], token)

    def test_get_jellyfin_params_strips_whitespace(self):
        token = "   my_token   "
        params = get_jellyfin_params(token)
        self.assertEqual(params["ApiKey"], "my_token")
        self.assertEqual(params["api_key"], "my_token")

    def test_get_jellyfin_params_empty_or_none(self):
        for empty_val in ["", None, "   "]:
            params = get_jellyfin_params(empty_val)
            self.assertEqual(params, {})

    def test_normalize_emby_url(self):
        self.assertEqual(
            normalize_emby_url("http://jellyfin.local:8096/"),
            "http://jellyfin.local:8096",
        )
        self.assertEqual(
            normalize_emby_url("http://jellyfin.local:8096///"),
            "http://jellyfin.local:8096",
        )
        self.assertEqual(
            normalize_emby_url("  http://jellyfin.local:8096/  "),
            "http://jellyfin.local:8096",
        )
        self.assertEqual(
            normalize_emby_url("http://jellyfin.local:8096/emby/"),
            "http://jellyfin.local:8096/emby",
        )
        self.assertEqual(normalize_emby_url(""), "")
        self.assertEqual(normalize_emby_url(None), "")


class TestScannerJellyfinEndpoints(unittest.TestCase):
    def setUp(self):
        config = {
            "SERVER_TYPE": "jellyfin",
            "SERVER_URL": "http://jellyfin.local:8096/",
            "API_KEY": "scanner_secret_token",
            "PLEX_URL": "",
            "TOKEN": "",
            "SCAN_DIRECTORIES": ["/media/movies"],
            "SCAN_WORKERS": 1,
            "LOG_LEVEL": "DEBUG",
            "LIBRARY_EXTENSIONS": {".mkv", ".mp4"},
            "IGNORE_PATTERNS": [],
            "PATH_REWRITES": [],
        }
        with patch.object(PlexScanner, "connect_to_plex"):
            self.scanner = PlexScanner(config)
        self.scanner.http_session = MagicMock()

    def test_get_jellyfin_libraries_request(self):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = [
            {"Name": "Movies", "ItemId": "1", "Locations": ["/media/movies"], "CollectionType": "movies"}
        ]
        self.scanner.http_session.get.return_value = mock_resp

        self.scanner._get_jellyfin_libraries()

        self.scanner.http_session.get.assert_called_once()
        args, kwargs = self.scanner.http_session.get.call_args
        self.assertEqual(args[0], "http://jellyfin.local:8096/Library/VirtualFolders")
        self.assertEqual(kwargs["headers"]["X-Emby-Token"], "scanner_secret_token")
        self.assertIn('Token="scanner_secret_token"', kwargs["headers"]["Authorization"])
        self.assertEqual(kwargs["params"]["ApiKey"], "scanner_secret_token")

    def test_try_plugin_scan_request(self):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"ItemId": "item123", "Status": "Created"}
        self.scanner.http_session.post.return_value = mock_resp

        # Mock refresh_jellyfin_item so we only test the ScanPath call directly
        with patch.object(self.scanner, "refresh_jellyfin_item"):
            result = self.scanner._try_plugin_scan("/media/movies/Test (2026)/Test (2026).mkv", None)
            self.assertTrue(result)

        self.scanner.http_session.post.assert_called_once()
        args, kwargs = self.scanner.http_session.post.call_args
        self.assertEqual(args[0], "http://jellyfin.local:8096/Library/ScanPath")
        self.assertEqual(kwargs["json"], {"Path": "/media/movies/Test (2026)/Test (2026).mkv"})
        self.assertEqual(kwargs["headers"]["X-Emby-Token"], "scanner_secret_token")
        self.assertIn('Token="scanner_secret_token"', kwargs["headers"]["Authorization"])
        self.assertEqual(kwargs["params"]["ApiKey"], "scanner_secret_token")

    def test_fallback_trigger_scan_request(self):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        self.scanner.http_session.post.return_value = mock_resp

        result = self.scanner._fallback_trigger_scan("/media/movies/Test (2026)", {"event_type": "added"})
        self.assertTrue(result)

        self.scanner.http_session.post.assert_called_once()
        args, kwargs = self.scanner.http_session.post.call_args
        self.assertEqual(args[0], "http://jellyfin.local:8096/Library/Media/Updated")
        self.assertEqual(kwargs["headers"]["X-Emby-Token"], "scanner_secret_token")
        self.assertIn('Token="scanner_secret_token"', kwargs["headers"]["Authorization"])
        self.assertEqual(kwargs["params"]["ApiKey"], "scanner_secret_token")

    def test_is_jellyfin_emby_scanning_request(self):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = [{"Key": "RefreshLibrary", "State": "Running"}]
        self.scanner.http_session.get.return_value = mock_resp

        is_scanning = self.scanner._is_jellyfin_emby_scanning()
        self.assertTrue(is_scanning)

        self.scanner.http_session.get.assert_called_once()
        args, kwargs = self.scanner.http_session.get.call_args
        self.assertEqual(args[0], "http://jellyfin.local:8096/ScheduledTasks")
        self.assertEqual(kwargs["headers"]["X-Emby-Token"], "scanner_secret_token")
        self.assertIn('Token="scanner_secret_token"', kwargs["headers"]["Authorization"])
        self.assertEqual(kwargs["params"]["ApiKey"], "scanner_secret_token")

    def test_refresh_jellyfin_item_request(self):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        self.scanner.http_session.post.return_value = mock_resp

        self.scanner.refresh_jellyfin_item("item456")

        self.scanner.http_session.post.assert_called_once()
        args, kwargs = self.scanner.http_session.post.call_args
        self.assertEqual(args[0], "http://jellyfin.local:8096/Items/item456/Refresh")
        self.assertEqual(kwargs["headers"]["X-Emby-Token"], "scanner_secret_token")
        self.assertIn('Token="scanner_secret_token"', kwargs["headers"]["Authorization"])
        self.assertEqual(kwargs["params"]["ApiKey"], "scanner_secret_token")


if __name__ == "__main__":
    unittest.main()
