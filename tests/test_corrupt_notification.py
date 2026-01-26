import unittest
from unittest.mock import MagicMock, patch
from omniscan_pkg.scanner import PlexScanner
from omniscan_pkg.models import RunStats
from discord import Color
import os

class TestCorruptNotification(unittest.TestCase):
    def setUp(self):
        self.config = {
            'PLEX_URL': 'http://mock:32400',
            'TOKEN': 'mock_token',
            'IGNORE_PATTERNS': [],
            'MEDIA_EXTENSIONS': {'.mkv', '.mp4'},
            'SYMLINK_CHECK': False,
            'SCAN_PATHS': ['/data'],
            'NOTIFICATIONS_ENABLED': True,
            'SCAN_DELAY': 0.0,
            'INCREMENTAL_SCAN': False,
            'HEALTH_CHECK': True,
            'SCAN_WORKERS': 4,
            'SERVER_TYPE': 'plex'
        }
        self.scanner = PlexScanner(self.config)

    @patch('omniscan_pkg.scanner.os.path.getsize', return_value=1000)
    @patch('omniscan_pkg.scanner.os.path.basename', return_value="corrupt_movie.mkv")
    def test_corrupt_file_notification_with_reason(self, mock_basename, mock_getsize):
        # Mock check_file_health to fail with a specific reason
        self.scanner.check_file_health = MagicMock(return_value=(False, {"status": "Corrupt", "error": "Bitstream Error"}))
        
        # Mock send_single_notification
        self.scanner.send_single_notification = MagicMock()
        
        # Mock other dependencies
        self.scanner.is_in_library = MagicMock(return_value=False)
        self.scanner.is_ignored = MagicMock(return_value=False)
        
        stats = RunStats(self.config)
        
        # Call scan_file
        self.scanner.scan_file('/path/to/corrupt_movie.mkv', stats=stats)
        
        # Verify send_single_notification was called with the error reason
        self.scanner.send_single_notification.assert_called_once()
        args, _ = self.scanner.send_single_notification.call_args
        title, message, color = args
        
        self.assertIn("Bitstream Error", message)
        self.assertIn("/path/to/corrupt_movie.mkv", message)
        self.assertEqual(title, "⚠️ Corrupt File Detected")

if __name__ == '__main__':
    unittest.main()
