import unittest
from unittest.mock import MagicMock, patch
from fastapi.testclient import TestClient
from omniscan_pkg.web import app, set_scanner
from omniscan_pkg.config import get_webhook_token


class TestWebHookAPI(unittest.TestCase):
    def setUp(self):
        self.client = TestClient(app)
        self.mock_scanner = MagicMock()
        self.mock_scanner.config = {
            "WEB_PASSWORD": "testpassword",
            "WEBHOOK_TOKEN": get_webhook_token("testpassword"),
            "PATH_REWRITES": [],
        }
        set_scanner(self.mock_scanner)
        self.token = self.mock_scanner.config["WEBHOOK_TOKEN"]

    def test_webhook_unauthorized(self):
        # Missing token
        response = self.client.post("/api/webhook", json={"path": "/media/movies/Test"})
        self.assertEqual(response.status_code, 401)
        self.assertEqual(response.json(), {"detail": "Unauthorized"})

        # Incorrect token
        response = self.client.post(
            "/api/webhook?apikey=wrong", json={"path": "/media/movies/Test"}
        )
        self.assertEqual(response.status_code, 401)
        self.assertEqual(response.json(), {"detail": "Unauthorized"})

    def test_webhook_test_event(self):
        # Sonarr/Radarr test event should succeed immediately without path validation
        payload = {
            "eventType": "Test",
            "series": {"title": "Test Series"},
            "episodes": [],
            "instanceName": "Sonarr",
            "applicationUrl": "http://localhost:8989",
        }
        response = self.client.post(f"/api/webhook?apikey={self.token}", json=payload)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.json(),
            {"status": "success", "message": "Test webhook received successfully"},
        )

        # Verify scanner was NOT called for scanning or submission
        self.mock_scanner.submit_file_event.assert_not_called()
        self.mock_scanner.trigger_scan.assert_not_called()

    @patch("os.path.exists", return_value=True)
    @patch("os.path.isfile", return_value=True)
    def test_webhook_scan_file(self, mock_isfile, mock_exists):
        payload = {"path": "/media/movies/TestMovie.mkv"}
        response = self.client.post(f"/api/webhook?apikey={self.token}", json=payload)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {"status": "success", "triggered": 1})
        self.mock_scanner.submit_file_event.assert_called_once_with(
            "created", "/media/movies/TestMovie.mkv", metadata=None
        )

    def test_engineio_session_disconnected_handling(self):
        import asyncio
        from unittest.mock import AsyncMock
        import engineio.async_server

        async def run_test():
            server = engineio.async_server.AsyncServer()
            server._async = {
                "translate_request": lambda *args, **kwargs: {
                    "QUERY_STRING": "sid=123",
                    "REQUEST_METHOD": "POST",
                }
            }
            server._log_error_once = MagicMock()
            server._bad_request = MagicMock(return_value={"bad": "request"})
            server._make_response = AsyncMock(return_value="response")

            with patch(
                "omniscan_pkg.web.original_handle_request", new_callable=AsyncMock
            ) as mock_orig:
                mock_orig.side_effect = KeyError("Session is disconnected")

                res = await server.handle_request("scope", "receive", "send")

                self.assertEqual(res, "response")
                server._log_error_once.assert_called_once_with(
                    "Invalid session 123", "bad-sid"
                )
                server._bad_request.assert_called_once_with("Invalid session 123")
                server._make_response.assert_called_once_with(
                    {"bad": "request"},
                    {"QUERY_STRING": "sid=123", "REQUEST_METHOD": "POST"},
                )

        asyncio.run(run_test())

    @patch("builtins.open")
    @patch("configparser.ConfigParser.write")
    def test_update_settings_restarts_jellyfin_alert_listener(self, mock_write, mock_open):
        from omniscan_pkg.web import get_current_user

        app.dependency_overrides[get_current_user] = lambda: "admin"
        try:
            payload = {
                "server_type": "Jellyfin",
                "server_url": "http://jellyfin.local:8096",
                "api_key": "test_token",
                "plex_server": "",
                "plex_token": "",
                "scan_directories": "/media",
                "scan_workers": 4,
                "scan_debounce": 10,
                "scan_delay": 0.0,
                "watch_mode": False,
                "run_interval": 24,
                "run_on_startup": True,
                "start_time": "",
                "incremental_scan": False,
                "scan_since_days": 7,
                "symlink_check": False,
                "empty_trash": False,
                "deletion_threshold": 50,
                "abort_on_mass_deletion": True,
                "notifications_enabled": False,
                "discord_webhook_url": "",
                "notification_group_window": 15,
                "ignore_patterns": "",
                "log_level": "INFO",
                "path_rewrites": "",
                "integrity_check": False,
                "ffprobe_check": False,
            }
            response = self.client.post("/api/settings", json=payload)
            self.assertEqual(response.status_code, 200)
            self.mock_scanner.restart_jellyfin_alert_listener.assert_called_once()
        finally:
            app.dependency_overrides.pop(get_current_user, None)


if __name__ == "__main__":
    unittest.main()
