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
            'IGNORE_PATTERNS': [],
            'SCAN_PATHS': ['/mnt/usenet-rclone/tv', '/mnt/usenet-rclone/movies']
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
        # Case: File gone, Root exists, stays gone
        # mock_exists calls:
        # 1. exists(file_path) -> False (Initial check)
        # 2. exists(scan_root) -> True (Mount check)
        # 3. exists(file_path) -> False (After sleep check)
        
        # We need to distinguish between file check and root check.
        def side_effect(path):
            if path.endswith('.mkv'): return False
            if path.startswith('/mnt/'): return True # Root exists
            return False
            
        mock_exists.side_effect = side_effect
        
        self.scanner.handle_deletion('/mnt/usenet-rclone/tv/movie.mkv')
        
        mock_sleep.assert_called_once_with(2)
        # Should proceed to trigger scan
        self.scanner.trigger_scan.assert_called()

    @patch('os.path.exists')
    @patch('time.sleep')
    def test_handle_deletion_mount_failure(self, mock_sleep, mock_exists):
        # Case: File gone, but Root ALSO gone (Mount failure)
        def side_effect(path):
            return False # Everything is gone
            
        mock_exists.side_effect = side_effect
        
        self.scanner.handle_deletion('/mnt/usenet-rclone/tv/movie.mkv')
        
        # Should abort before sleep
        mock_sleep.assert_not_called()
        self.scanner.trigger_scan.assert_not_called()

    @patch('os.path.exists')
    @patch('time.sleep')
    def test_handle_deletion_transient_glitch(self, mock_sleep, mock_exists):
        # Case: File gone initially, Root exists, but file reappears after sleep
        def side_effect(path):
            if path == '/mnt/usenet-rclone/tv/movie.mkv':
                # Returns False first (initial), then True (reappear)
                # But wait, logic is:
                # 1. if exists(file): return
                # 2. if not exists(root): return
                # 3. sleep(2)
                # 4. if exists(file): return
                pass
            return True 
        
        # We need a mutable iterator or something to handle the sequence
        # Call 1 (file): False
        # Call 2 (root): True
        # Call 3 (file): True
        mock_exists.side_effect = [False, True, True]
        
        self.scanner.handle_deletion('/mnt/usenet-rclone/tv/movie.mkv')
        
        mock_sleep.assert_called_once_with(2)
        # Should not trigger scan
        self.scanner.trigger_scan.assert_not_called()

    def test_trigger_scan_metadata_merging(self):
        # Restore real trigger_scan for this test
        self.scanner.trigger_scan = lambda library_id, folder_path, force=False, metadata=None: \
            PlexScanner.trigger_scan(self.scanner, library_id, folder_path, force, metadata)

        # 1. Start with a deletion event
        self.scanner.trigger_scan('1', '/mnt/usenet-rclone/tv/ShowName/Season 01', metadata={'event_type': 'deleted'})
        
        # Verify it is in pending scans
        key = ('1', '/mnt/usenet-rclone/tv/ShowName/Season 01', None)
        self.assertIn(key, self.scanner.pending_scans)
        _, metadata = self.scanner.pending_scans[key]
        self.assertEqual(metadata.get('event_type'), 'deleted')

        # 2. Add an addition event to the same folder path (de-bounces/updates)
        self.scanner.trigger_scan('1', '/mnt/usenet-rclone/tv/ShowName/Season 01', metadata={'event_type': 'added'})

        # Verify that 'deleted' event type was preserved in the merged metadata
        _, metadata = self.scanner.pending_scans[key]
        self.assertEqual(metadata.get('event_type'), 'deleted')

    def test_trigger_scan_collapsing(self):
        # Restore real trigger_scan
        self.scanner.trigger_scan = lambda library_id, folder_path, force=False, metadata=None: \
            PlexScanner.trigger_scan(self.scanner, library_id, folder_path, force, metadata)

        # 1. Queue a deletion scan for a sub-folder
        self.scanner.trigger_scan('1', '/mnt/usenet-rclone/tv/ShowName/Season 01/Episode 01', metadata={'event_type': 'deleted'})

        # 2. Queue a broader scan for the parent folder (Case 2 collapsing)
        self.scanner.trigger_scan('1', '/mnt/usenet-rclone/tv/ShowName/Season 01', metadata={'event_type': 'added'})

        # Verify the sub-folder scan was removed
        sub_key = ('1', '/mnt/usenet-rclone/tv/ShowName/Season 01/Episode 01', None)
        self.assertNotIn(sub_key, self.scanner.pending_scans)

        # Verify the parent folder scan is active and has inherited the 'deleted' metadata
        parent_key = ('1', '/mnt/usenet-rclone/tv/ShowName/Season 01', None)
        self.assertIn(parent_key, self.scanner.pending_scans)
        _, metadata = self.scanner.pending_scans[parent_key]
        self.assertEqual(metadata.get('event_type'), 'deleted')

if __name__ == '__main__':
    unittest.main()