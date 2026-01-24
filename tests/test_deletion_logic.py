import unittest
from unittest.mock import MagicMock, patch
import os
import time
from omniscan_pkg.scanner import PlexScanner

class TestDeletionLogic(unittest.TestCase):
    def setUp(self):
        self.config = {
            'MEDIA_EXTENSIONS': {'.mkv', '.mp4'},
            'SERVER_TYPE': 'plex',
            'PLEX_URL': 'http://mock',
            'TOKEN': 'mock',
            'SCAN_WORKERS': 1,
            'SCAN_DEBOUNCE': 10,
            'NOTIFICATIONS_ENABLED': False,
            'IGNORE_PATTERNS': []
        }
        self.scanner = PlexScanner(self.config)
        # Mock methods that might be called
        self.scanner.get_library_id_for_path = MagicMock(return_value=('1', 'Movies', 'movie'))
        self.scanner.trigger_scan = MagicMock()
        self.scanner.pending_scans_lock = MagicMock()
        self.scanner.pending_scans_lock.__enter__ = MagicMock()
        self.scanner.pending_scans_lock.__exit__ = MagicMock()

    @patch('os.path.exists')
    @patch('time.sleep')
    def test_handle_deletion_real(self, mock_sleep, mock_exists):
        # Case: File gone, stays gone
        mock_exists.return_value = False
        
        self.scanner.handle_deletion('/path/movie.mkv')
        
        mock_sleep.assert_called_once_with(2)
        # Should verify twice (start, and after sleep)
        self.assertEqual(mock_exists.call_count, 2)
        # Should proceed to trigger scan
        self.scanner.trigger_scan.assert_called()

    @patch('os.path.exists')
    @patch('time.sleep')
    def test_handle_deletion_false_positive_exists_initially(self, mock_sleep, mock_exists):
        # Case: File exists immediately
        mock_exists.return_value = True
        
        self.scanner.handle_deletion('/path/movie.mkv')
        
        # Should not sleep
        mock_sleep.assert_not_called()
        # Should not trigger scan
        self.scanner.trigger_scan.assert_not_called()

    @patch('os.path.exists')
    @patch('time.sleep')
    def test_handle_deletion_transient_glitch(self, mock_sleep, mock_exists):
        # Case: File gone initially, but reappears after sleep
        mock_exists.side_effect = [False, True]
        
        self.scanner.handle_deletion('/path/movie.mkv')
        
        mock_sleep.assert_called_once_with(2)
        # Should not trigger scan
        self.scanner.trigger_scan.assert_not_called()

    @patch('os.path.exists')
    def test_handle_deletion_wrong_extension(self, mock_exists):
        self.scanner.handle_deletion('/path/sub.srt')
        
        mock_exists.assert_not_called()
        self.scanner.trigger_scan.assert_not_called()

if __name__ == '__main__':
    unittest.main()
