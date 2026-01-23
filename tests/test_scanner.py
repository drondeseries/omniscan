import unittest
from unittest.mock import MagicMock, patch, mock_open
import os
from omniscan_pkg.scanner import PlexScanner
from omniscan_pkg.models import RunStats, StuckFileTracker
import logging

# Disable logging for tests
logging.disable(logging.CRITICAL)

class TestPlexScanner(unittest.TestCase):
    def setUp(self):
        self.config = {
            'PLEX_URL': 'http://mock:32400',
            'TOKEN': 'mock_token',
            'IGNORE_PATTERNS': ['*.tmp', 'sample*'],
            'MEDIA_EXTENSIONS': {'.mkv', '.mp4'},
            'SYMLINK_CHECK': True,
            'SCAN_PATHS': ['/data'],
            'NOTIFICATIONS_ENABLED': False,
            'SCAN_DELAY': 0.0,
            'INCREMENTAL_SCAN': False,
            'HEALTH_CHECK': False,
            'SCAN_WORKERS': 4,
            'SERVER_TYPE': 'plex'
        }
        self.scanner = PlexScanner(self.config)

    def test_is_ignored(self):
        self.assertTrue(self.scanner.is_ignored('/path/to/sample_file.mkv'))
        self.assertTrue(self.scanner.is_ignored('/path/to/file.tmp'))
        self.assertFalse(self.scanner.is_ignored('/path/to/movie.mkv'))

    @patch('omniscan_pkg.scanner.PlexServer')
    def test_connect_to_plex_success(self, MockPlex):
        mock_instance = MockPlex.return_value
        mock_instance.friendlyName = "MockServer"
        mock_instance.version = "1.0"
        
        server = self.scanner.connect_to_plex()
        self.assertEqual(server, mock_instance)
        self.assertIsNotNone(self.scanner.plex)

    @patch('omniscan_pkg.scanner.PlexServer')
    def test_connect_to_plex_retry(self, MockPlex):
        # Fail once then succeed
        MockPlex.side_effect = [Exception("Connection failed"), MagicMock(friendlyName="MockServer", version="1.0")]
        
        with patch('time.sleep') as mock_sleep:
            server = self.scanner.connect_to_plex()
            self.assertEqual(mock_sleep.call_count, 1)
            self.assertIsNotNone(server)

    def test_is_broken_symlink(self):
        with patch('os.path.islink', return_value=True), \
             patch('os.path.exists', return_value=False), \
             patch('os.path.realpath', return_value='/broken/path'):
            self.assertTrue(self.scanner.is_broken_symlink('/path/to/link'))

        with patch('os.path.islink', return_value=True), \
             patch('os.path.exists', return_value=True):
            self.assertFalse(self.scanner.is_broken_symlink('/path/to/valid_link'))

    @patch('os.walk')
    @patch('os.path.getsize')
    def test_scan_directory(self, mock_getsize, mock_walk):
        mock_walk.return_value = [
            ('/data', [], ['movie.mkv', 'ignored.tmp', 'text.txt'])
        ]
        mock_getsize.return_value = 1000
        
        # Mock is_in_plex to return False (missing)
        self.scanner.is_in_plex = MagicMock(return_value=False)
        self.scanner.get_library_id_for_path = MagicMock(return_value=('1', 'Movies', 'movie'))
        
        stats = RunStats(self.config)
        tracker = StuckFileTracker()
        tracker._load_history = MagicMock(return_value={})
        tracker.increment_attempt = MagicMock(return_value=False) # Not stuck
        tracker.lock = MagicMock()
        tracker.lock.__enter__ = MagicMock()
        tracker.lock.__exit__ = MagicMock()
        
        folders_to_scan = set()
        lock = MagicMock()
        lock.__enter__ = MagicMock()
        lock.__exit__ = MagicMock()
        
        self.scanner.scan_directory('/data', stats, tracker, folders_to_scan, lock)
        
        # Verify stats
        self.assertEqual(stats.total_scanned, 1) # Only movie.mkv
        self.assertEqual(stats.total_missing, 1)
        self.assertEqual(len(folders_to_scan), 1)

if __name__ == '__main__':
    unittest.main()
