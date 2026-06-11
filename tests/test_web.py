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
            'WEB_PASSWORD': 'testpassword',
            'PATH_REWRITES': [],
        }
        set_scanner(self.mock_scanner)
        self.token = get_webhook_token('testpassword')

    def test_webhook_unauthorized(self):
        # Missing token
        response = self.client.post("/api/webhook", json={"path": "/media/movies/Test"})
        self.assertEqual(response.status_code, 401)
        self.assertEqual(response.json(), {"detail": "Unauthorized"})

        # Incorrect token
        response = self.client.post("/api/webhook?apikey=wrong", json={"path": "/media/movies/Test"})
        self.assertEqual(response.status_code, 401)
        self.assertEqual(response.json(), {"detail": "Unauthorized"})

    def test_webhook_test_event(self):
        # Sonarr/Radarr test event should succeed immediately without path validation
        payload = {
            "eventType": "Test",
            "series": {"title": "Test Series"},
            "episodes": [],
            "instanceName": "Sonarr",
            "applicationUrl": "http://localhost:8989"
        }
        response = self.client.post(f"/api/webhook?apikey={self.token}", json=payload)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {"status": "success", "message": "Test webhook received successfully"})
        
        # Verify scanner was NOT called for scanning or submission
        self.mock_scanner.submit_file_event.assert_not_called()
        self.mock_scanner.trigger_scan.assert_not_called()

    @patch('os.path.exists', return_value=True)
    @patch('os.path.isfile', return_value=True)
    def test_webhook_scan_file(self, mock_isfile, mock_exists):
        payload = {
            "path": "/media/movies/TestMovie.mkv"
        }
        response = self.client.post(f"/api/webhook?apikey={self.token}", json=payload)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {"status": "success", "triggered": 1})
        self.mock_scanner.submit_file_event.assert_called_once_with('created', '/media/movies/TestMovie.mkv', metadata=None)

if __name__ == '__main__':
    unittest.main()
