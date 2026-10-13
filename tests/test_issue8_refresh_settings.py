import functools
import os
import tempfile
import unittest
from unittest.mock import MagicMock, patch
import schedule
from omniscan_pkg.config import (
    load_config,
    persist_settings,
    format_config_sections,
    apply_and_save_settings,
)
from omniscan_pkg.scanner import PlexScanner
from fastapi.testclient import TestClient
from omniscan_pkg.web import app, set_scanner, get_current_user


class TestIssue8Regression(unittest.TestCase):
    def setUp(self):
        schedule.clear()
        self.orig_cwd = os.getcwd()
        self.temp_dir = tempfile.TemporaryDirectory()
        os.chdir(self.temp_dir.name)

        self.config_path = os.path.join(self.temp_dir.name, "config.ini")
        self.history_db = os.path.join(self.temp_dir.name, "history.db")
        with open(self.config_path, "w") as f:
            f.write(
                "[server]\ntype = jellyfin\nurl = http://jellyfin.local:8096\napi_key = test_key\n"
                "[behaviour]\nrun_interval = 24\nrun_interval_unit = hours\nplex_refresh = false\n"
                "[scan]\ndirectories = /media/movies\n"
            )
        self.config = load_config(self.config_path)
        self.config["HISTORY_DB"] = self.history_db
        self.scanner = PlexScanner(self.config)

    def tearDown(self):
        self.scanner.shutdown()
        schedule.clear()
        os.chdir(self.orig_cwd)
        self.temp_dir.cleanup()

    def test_persist_settings_atomic_and_preserves_custom(self):
        candidate = dict(self.scanner.config)
        candidate["PLEX_REFRESH"] = True
        candidate["RUN_INTERVAL"] = 12
        candidate["RUN_INTERVAL_UNIT"] = "hours"
        candidate["START_TIME"] = "04:30"

        persist_settings(candidate, config_path=self.config_path)

        reloaded = load_config(self.config_path)
        self.assertTrue(reloaded["PLEX_REFRESH"])
        self.assertEqual(reloaded["RUN_INTERVAL"], 12)
        self.assertEqual(reloaded["RUN_INTERVAL_UNIT"], "hours")
        self.assertEqual(reloaded["START_TIME"], "04:30")

    def test_persist_settings_write_failure_leaves_config_intact(self):
        candidate = dict(self.scanner.config)
        candidate["PLEX_REFRESH"] = True

        # Injected error during atomic replacement
        with patch("os.replace", side_effect=OSError("Disk full")):
            with self.assertRaises(OSError):
                persist_settings(candidate, config_path=self.config_path)

        # Original config is untouched
        reloaded = load_config(self.config_path)
        self.assertFalse(reloaded["PLEX_REFRESH"])

    def test_reschedule_preserves_unrelated_jobs(self):
        unrelated_job = schedule.every(10).seconds.do(lambda: None).tag("other_service")
        self.assertIn(unrelated_job, schedule.jobs)

        ok, err = self.scanner.reschedule(interval=6, interval_unit="hours", start_time="02:00")
        self.assertTrue(ok)
        self.assertIsNone(err)

        # Unrelated job was NOT cleared
        self.assertIn(unrelated_job, schedule.jobs)

        # Scanner jobs are tagged with 'scanner'
        scanner_jobs = [j for j in schedule.jobs if "scanner" in getattr(j, "tags", set())]
        self.assertGreater(len(scanner_jobs), 0)

    def test_reschedule_unwraps_partial_job_func(self):
        # A legacy job wrapped with functools.partial
        wrapped_fn = functools.partial(self.scanner.run_scan)
        legacy_job = schedule.every(24).hours.do(wrapped_fn)
        self.assertIn(legacy_job, schedule.jobs)

        ok, err = self.scanner.reschedule(interval=8, interval_unit="hours")
        self.assertTrue(ok)
        self.assertNotIn(legacy_job, schedule.jobs)

    def test_reschedule_staging_failure_preserves_active_jobs(self):
        # Establish an initial schedule
        self.scanner.reschedule(interval=4, interval_unit="hours")
        initial_jobs = list(schedule.jobs)
        self.assertEqual(len(initial_jobs), 1)

        # Simulate exception during staging creation
        with patch.object(schedule.Scheduler, "every", side_effect=RuntimeError("Staging error")):
            ok, err = self.scanner.reschedule(interval=10, interval_unit="hours")
            self.assertFalse(ok)
            self.assertIn("Staging error", err)

        # Active schedule is 100% intact
        self.assertEqual(len(schedule.jobs), 1)
        self.assertEqual(schedule.jobs[0].interval, 4)

    def test_reschedule_invalid_start_time_falls_back_safely(self):
        ok, err = self.scanner.reschedule(interval=6, interval_unit="hours", start_time="invalid:time")
        self.assertTrue(ok)
        scanner_jobs = [j for j in schedule.jobs if "scanner" in getattr(j, "tags", set())]
        self.assertEqual(len(scanner_jobs), 1)

    def test_reschedule_minutes_unit(self):
        ok, err = self.scanner.reschedule(interval=30, interval_unit="minutes")
        self.assertTrue(ok)
        scanner_jobs = [j for j in schedule.jobs if "scanner" in getattr(j, "tags", set())]
        self.assertEqual(len(scanner_jobs), 1)
        self.assertEqual(scanner_jobs[0].unit, "minutes")

    def test_jellyfin_item_id_rejects_ignored_path_filter(self):
        target_file = "/media/movies/Army of Shadows (1969)/movie.mkv"
        wrong_file = "/media/movies/Insidious (2010)/insidious.mkv"

        # Mock /Items returning wrong movie because Jellyfin ignored ?Path=
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "Items": [{"Id": "wrong-id-insidious", "Path": wrong_file}]
        }

        # Mock fallback search returning the actual item
        mock_search_response = MagicMock()
        mock_search_response.status_code = 200
        mock_search_response.json.return_value = {
            "Items": [
                {"Id": "correct-id-army-of-shadows", "Path": target_file}
            ]
        }

        def mock_get(url, **kwargs):
            if "searchTerm=" in url:
                return mock_search_response
            return mock_response

        with patch.object(self.scanner.http_session, "get", side_effect=mock_get):
            found_id = self.scanner.get_jellyfin_item_id(target_file)
            # Must NOT return "wrong-id-insidious"
            self.assertEqual(found_id, "correct-id-army-of-shadows")

    def test_jellyfin_item_id_ambiguous_direct_stops_immediately_without_fallback(self):
        target_file = "/media/movies/Movie A/movie.mkv"

        # Direct /Items query returns two distinct items for the same file path (ambiguous)
        mock_direct_response = MagicMock()
        mock_direct_response.status_code = 200
        mock_direct_response.json.return_value = {
            "Items": [
                {"Id": "ambiguous-id-1", "Path": target_file, "Type": "Movie"},
                {"Id": "ambiguous-id-2", "Path": target_file, "Type": "Movie"},
            ]
        }

        fallback_called = False

        def mock_get(url, **kwargs):
            nonlocal fallback_called
            if "searchTerm=" in url:
                fallback_called = True
                return MagicMock(status_code=200, json=lambda: {"Items": [{"Id": "ambiguous-id-1", "Path": target_file, "Type": "Movie"}]})
            return mock_direct_response

        with patch.object(self.scanner.http_session, "get", side_effect=mock_get):
            found_id = self.scanner.get_jellyfin_item_id(target_file)
            # Must reject immediately and return None
            self.assertIsNone(found_id)
            # Crucial: must NOT call fallback search to arbitrarily pick one candidate
            self.assertFalse(fallback_called)

    def test_jellyfin_item_id_rejects_non_movie_containers(self):
        target_file = "/media/movies/movie.mkv"
        folder_path = "/media/movies"

        # Types that should never match as a movie container
        for forbidden_type in ["Folder", "CollectionFolder", "BoxSet", "Video", "Series", "Season", "Playlist"]:
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.json.return_value = {
                "Items": [
                    {"Id": f"{forbidden_type}-id", "Path": folder_path, "Type": forbidden_type}
                ]
            }

            with patch.object(self.scanner.http_session, "get", return_value=mock_response):
                found_id = self.scanner.get_jellyfin_item_id(target_file)
                self.assertIsNone(found_id, f"Failed: container with type {forbidden_type} was matched as movie!")

    def test_jellyfin_item_id_matches_strictly_movie_type_container(self):
        target_file = "/media/movies/Army of Shadows (1969)/movie.mkv"
        movie_folder = "/media/movies/Army of Shadows (1969)"

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "Items": [{"Id": "folder-id-army-of-shadows", "Path": movie_folder, "Type": "Movie"}]
        }

        with patch.object(self.scanner.http_session, "get", return_value=mock_response):
            found_id = self.scanner.get_jellyfin_item_id(target_file)
            self.assertEqual(found_id, "folder-id-army-of-shadows")

    def test_post_scan_retries_and_refreshes_when_enabled(self):
        self.scanner.config["PLEX_REFRESH"] = True
        file_path = "/media/movies/movie.mkv"

        call_count = 0

        def mock_get_id(path):
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                return None  # First attempt DB hasn't settled
            return "settled-item-id"

        with patch.object(self.scanner, "get_jellyfin_item_id", side_effect=mock_get_id), \
             patch.object(self.scanner, "refresh_jellyfin_item") as mock_refresh, \
             patch.object(self.scanner, "wait_for_jellyfin_identification", return_value=True) as mock_wait, \
             patch("time.sleep"):
            self.scanner._post_scan_process_file(file_path, max_attempts=3)
            self.assertEqual(call_count, 2)
            mock_wait.assert_called_once_with("settled-item-id")
            mock_refresh.assert_called_once_with("settled-item-id", replace_all=True)

    def test_post_scan_skips_refresh_when_identification_times_out(self):
        """Fallback path: wait False must suppress the replace-all refresh."""
        self.scanner.config["PLEX_REFRESH"] = True
        file_path = "/media/movies/movie.mkv"

        with patch.object(self.scanner, "get_jellyfin_item_id", return_value="item-123"), \
             patch.object(self.scanner, "refresh_jellyfin_item") as mock_refresh, \
             patch.object(self.scanner, "wait_for_jellyfin_identification", return_value=False) as mock_wait, \
             patch("time.sleep"):
            self.scanner._post_scan_process_file(file_path, max_attempts=1)
            mock_wait.assert_called_once_with("item-123")
            mock_refresh.assert_not_called()

    def test_post_scan_skips_refresh_when_disabled(self):
        self.scanner.config["PLEX_REFRESH"] = False
        file_path = "/media/movies/movie.mkv"

        with patch.object(self.scanner, "get_jellyfin_item_id", return_value="item-123"), \
             patch.object(self.scanner, "refresh_jellyfin_item") as mock_refresh, \
             patch.object(self.scanner, "wait_for_jellyfin_identification") as mock_wait, \
             patch("time.sleep"):
            self.scanner._post_scan_process_file(file_path, max_attempts=1)
            mock_refresh.assert_not_called()
            mock_wait.assert_not_called()

    def test_refresh_uses_replace_all_metadata(self):
        """Post-import refresh must force overwrite (issue #8 follow-up).

        FullRefresh alone preserves existing values such as the
        filename-derived title; only ReplaceAllMetadata=true behaves like
        Jellyfin's manual "Replace all metadata" action.
        """
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        with patch.object(self.scanner.http_session, "post", return_value=mock_resp) as mock_post:
            self.scanner.refresh_jellyfin_item("item-abc", replace_all=True)
            _, kwargs = mock_post.call_args
            self.assertEqual(kwargs["params"]["ReplaceAllMetadata"], "true")
            # Default stays lightweight for other callers
            self.scanner.refresh_jellyfin_item("item-abc")
            _, kwargs = mock_post.call_args
            self.assertEqual(kwargs["params"]["ReplaceAllMetadata"], "false")

    def test_wait_for_identification_returns_when_provider_ids_present(self):
        with patch.object(
            self.scanner, "get_jellyfin_item_provider_ids", return_value={"Tmdb": "1302904"}
        ), patch("time.sleep") as mock_sleep:
            self.assertTrue(self.scanner.wait_for_jellyfin_identification("item-abc"))
            mock_sleep.assert_not_called()

    def test_wait_for_identification_times_out_without_provider_ids(self):
        with patch.object(
            self.scanner, "get_jellyfin_item_provider_ids", return_value={}
        ), patch("time.sleep"):
            self.assertFalse(self.scanner.wait_for_jellyfin_identification("item-abc", max_attempts=2))

    def test_wait_for_identification_enforces_deadline_before_attempt(self):
        """No HTTP lookup may start once the deadline has passed."""
        with patch.object(
            self.scanner, "get_jellyfin_item_provider_ids"
        ) as mock_ids, patch("time.sleep"), patch(
            "omniscan_pkg.scanner.time.monotonic", return_value=1000.0
        ):
            # monotonic == deadline start + 15s budget already consumed
            self.scanner.wait_for_jellyfin_identification(
                "item-abc", max_attempts=5, max_wait_seconds=0
            )
            mock_ids.assert_not_called()

    def test_wait_for_identification_rejects_result_after_deadline(self):
        """Provider IDs arriving after the deadline must not trigger refresh."""
        calls = {"n": 0}

        def slow_lookup(item_id, timeout=None):
            calls["n"] += 1
            return {"Tmdb": "1302904"}

        # First monotonic: deadline computation; second: pre-attempt check
        # (within budget); third: post-result check (past deadline).
        times = iter([1000.0, 1005.0, 1020.0])

        with patch.object(
            self.scanner, "get_jellyfin_item_provider_ids", side_effect=slow_lookup
        ), patch("time.sleep"), patch(
            "omniscan_pkg.scanner.time.monotonic", side_effect=lambda: next(times)
        ):
            self.assertFalse(
                self.scanner.wait_for_jellyfin_identification(
                    "item-abc", max_attempts=5, max_wait_seconds=15
                )
            )
            self.assertEqual(calls["n"], 1)

    def test_wait_for_identification_passes_remaining_budget_as_timeout(self):
        """Each lookup's HTTP timeout is capped to the remaining deadline."""
        seen = {}

        def capture_timeout(item_id, timeout=None):
            seen["timeout"] = timeout
            return {}

        with patch.object(
            self.scanner, "get_jellyfin_item_provider_ids", side_effect=capture_timeout
        ), patch("time.sleep"), patch(
            "omniscan_pkg.scanner.time.monotonic", return_value=1000.0
        ):
            self.scanner.wait_for_jellyfin_identification(
                "item-abc", max_attempts=1, max_wait_seconds=7
            )
            self.assertEqual(seen["timeout"], 7)

    def test_wait_for_identification_no_trailing_sleep_and_deadline_capped(self):
        """No sleep after the final attempt; total wait capped by deadline."""
        import omniscan_pkg.scanner as scanner_mod

        with patch.object(
            self.scanner, "get_jellyfin_item_provider_ids", return_value={}
        ), patch.object(scanner_mod.time, "sleep") as mock_sleep, patch.object(
            scanner_mod.time, "monotonic", side_effect=[0, 1, 2, 100, 101]
        ):
            self.assertFalse(
                self.scanner.wait_for_jellyfin_identification(
                    "item-abc", max_attempts=5, max_wait_seconds=15
                )
            )
            # Deadline (monotonic=100 > 15) stops the loop early: fewer sleeps
            # than max_attempts-1 and no trailing sleep after the last attempt.
            self.assertLessEqual(mock_sleep.call_count, 2)

    def test_get_provider_ids_returns_empty_on_error(self):
        with patch.object(
            self.scanner.http_session, "get", side_effect=Exception("down")
        ):
            self.assertEqual(self.scanner.get_jellyfin_item_provider_ids("item-abc"), {})

    def test_get_provider_ids_returns_empty_when_helper_exceeds_timeout(self):
        """A trickling server must not hold the caller past the budget."""
        import threading

        real_thread = threading.Thread

        class HangingThread:
            def __init__(self, *args, **kwargs):
                self._kwargs = kwargs

            def start(self):
                pass

            def join(self, timeout=None):
                pass

            def is_alive(self):
                return True

        with patch("omniscan_pkg.scanner.threading.Thread", HangingThread):
            start = __import__("time").monotonic()
            self.assertEqual(
                self.scanner.get_jellyfin_item_provider_ids("item-abc", timeout=0.2),
                {},
            )
            elapsed = __import__("time").monotonic() - start
            # Caller returns within budget despite the hung I/O thread…
            self.assertLess(elapsed, 5)
            # …and the hang is tracked (bounded) for visibility.
            self.assertEqual(len(self.scanner._orphaned_io_threads), 1)

    def test_get_provider_ids_recovers_after_trickle(self):
        """A stuck lookup must not wedge subsequent lookups."""
        import threading

        calls = {"n": 0}
        real_thread_cls = threading.Thread

        class OneHungThenReal:
            def __init__(self, *args, **kwargs):
                self._target = kwargs.get("target")
                self._args = kwargs.get("args", ())
                self._alive = True
                self._t = None

            def start(self):
                calls["n"] += 1
                if calls["n"] == 1:
                    return  # first lookup hangs forever
                self._t = real_thread_cls(
                    target=self._target, args=self._args, daemon=True
                )
                self._t.start()

            def join(self, timeout=None):
                if self._t is not None:
                    self._t.join(timeout=timeout)

            def is_alive(self):
                if self._t is not None:
                    return self._t.is_alive()
                return True

        with patch(
            "omniscan_pkg.scanner.threading.Thread", OneHungThenReal
        ), patch.object(
            self.scanner,
            "_fetch_provider_ids_thread",
            side_effect=lambda *a: a[-1].update(
                {"result": {"Tmdb": "1302904"}}
            ),
        ):
            # First lookup hangs -> {} …
            self.assertEqual(
                self.scanner.get_jellyfin_item_provider_ids("item-abc", timeout=0.2),
                {},
            )
            # …second lookup succeeds: no shared worker was wedged.
            self.assertEqual(
                self.scanner.get_jellyfin_item_provider_ids("item-abc", timeout=5),
                {"Tmdb": "1302904"},
            )

    def test_try_plugin_scan_item_id_submits_no_python_refresh(self):
        """Single refresh owner: plugin ItemId => no Python refresh submitted."""
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "ItemId": "item-abc-123",
            "Status": "Created",
            "Message": "Created successfully",
        }

        self.scanner.config["PLEX_REFRESH"] = True
        with patch.object(self.scanner.http_session, "post", return_value=mock_resp), \
              patch.object(self.scanner.scan_monitor_executor, "submit") as mock_submit, \
              patch.object(self.scanner, "refresh_jellyfin_item") as mock_refresh, \
              patch.object(self.scanner, "wait_for_jellyfin_identification") as mock_wait:
            ret = self.scanner._try_plugin_scan("/media/movies/new.mkv", None)
            self.assertTrue(ret)
            # Plugin owns the refresh — Python must not enqueue or call one.
            mock_submit.assert_not_called()
            mock_wait.assert_not_called()
            mock_refresh.assert_not_called()

    def test_try_plugin_scan_status_only_enqueues_verification(self):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "Status": "Created",
            "Message": "Created without item id",
        }

        with patch.object(self.scanner.http_session, "post", return_value=mock_resp), \
             patch.object(self.scanner.scan_monitor_executor, "submit") as mock_submit:
            ret = self.scanner._try_plugin_scan("/media/movies/new.mkv", None)
            self.assertTrue(ret)
            # Verification must be enqueued with delay
            mock_submit.assert_called_once_with(
                self.scanner._post_scan_process_file_delayed, "/media/movies/new.mkv", delay=5
            )

    def test_try_plugin_scan_error_response_does_not_clear_history(self):
        file_path = "/media/movies/failed_leaf.mkv"
        self.scanner.history.increment_attempt(file_path)
        self.assertIn(file_path, self.scanner.history.stuck_paths)

        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "Status": "Error",
            "Message": "Item could not be created",
            "ItemId": "",
        }

        with patch.object(self.scanner.http_session, "post", return_value=mock_resp):
            ret = self.scanner._try_plugin_scan(file_path, None)
            self.assertFalse(ret)
            # History entry is NOT cleared on error
            self.assertIn(file_path, self.scanner.history.stuck_paths)

    def test_api_settings_legacy_payload_preserves_omitted_settings(self):
        client = TestClient(app)
        set_scanner(self.scanner)
        app.dependency_overrides[get_current_user] = lambda: "admin"

        # Pre-set custom values
        self.scanner.config["PLEX_REFRESH"] = True
        self.scanner.config["PLEX_ANALYZE"] = True
        self.scanner.config["RUN_INTERVAL_UNIT"] = "minutes"
        self.scanner.config["MAX_RETRIES"] = 7
        self.scanner.config["CLEANUP_DAYS"] = 21

        try:
            # Legacy payload from older client that omits the new fields
            legacy_payload = {
                "server_type": "jellyfin",
                "server_url": "http://jellyfin.local:8096",
                "api_key": "test_key",
                "plex_server": "",
                "plex_token": "",
                "scan_directories": "/media/movies",
                "scan_workers": 4,
                "scan_debounce": 10,
                "scan_delay": 0.0,
                "watch_mode": False,
                "run_interval": 15,
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

            response = client.post("/api/settings", json=legacy_payload)
            self.assertEqual(response.status_code, 200)

            # Omitted fields must NOT be reset to defaults
            self.assertTrue(self.scanner.config["PLEX_REFRESH"])
            self.assertTrue(self.scanner.config["PLEX_ANALYZE"])
            self.assertEqual(self.scanner.config["RUN_INTERVAL_UNIT"], "minutes")
            self.assertEqual(self.scanner.config["MAX_RETRIES"], 7)
            self.assertEqual(self.scanner.config["CLEANUP_DAYS"], 21)
            self.assertEqual(self.scanner.config["RUN_INTERVAL"], 15)
        finally:
            app.dependency_overrides.pop(get_current_user, None)

    def test_api_settings_round_trip_persistence(self):
        client = TestClient(app)
        set_scanner(self.scanner)
        app.dependency_overrides[get_current_user] = lambda: "admin"

        try:
            payload = {
                "server_type": "jellyfin",
                "server_url": "http://jellyfin.local:8096",
                "api_key": "test_key",
                "plex_server": "",
                "plex_token": "",
                "scan_directories": "/media/movies",
                "scan_workers": 4,
                "scan_debounce": 10,
                "scan_delay": 0.0,
                "watch_mode": False,
                "run_interval": 12,
                "run_interval_unit": "hours",
                "run_on_startup": True,
                "start_time": "05:00",
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
                "plex_refresh": True,
                "plex_analyze": True,
            }

            # Actual disk write into isolated temp_dir
            response = client.post("/api/settings", json=payload)
            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.json()["status"], "success")

            # Verify persisted file and reload from isolated config.ini
            reloaded = load_config("config.ini")
            self.assertTrue(reloaded["PLEX_REFRESH"])
            self.assertTrue(reloaded["PLEX_ANALYZE"])
            self.assertEqual(reloaded["RUN_INTERVAL"], 12)
            self.assertEqual(reloaded["START_TIME"], "05:00")
            self.assertTrue(self.scanner.config["PLEX_REFRESH"])
        finally:
            app.dependency_overrides.pop(get_current_user, None)

    def test_apply_and_save_settings_unified_handler(self):
        # Exercises the shared apply_and_save_settings used by ui.py and web.py
        settings_values = {
            "START_TIME": "04:15",
            "PLEX_REFRESH": True,
            "RUN_INTERVAL": 8,
            "RUN_INTERVAL_UNIT": "hours",
        }

        candidate, ok, err = apply_and_save_settings(
            self.scanner, settings_values, config_path=self.config_path
        )

        self.assertTrue(ok)
        self.assertIsNone(err)
        self.assertEqual(self.scanner.config["START_TIME"], "04:15")
        self.assertTrue(self.scanner.config["PLEX_REFRESH"])
        self.assertEqual(self.scanner.config["RUN_INTERVAL"], 8)

        reloaded = load_config(self.config_path)
        self.assertEqual(reloaded["START_TIME"], "04:15")
        self.assertTrue(reloaded["PLEX_REFRESH"])

    def test_apply_and_save_settings_scheduler_warning_reported(self):
        # When scheduler staging fails, disk persistence still succeeds, and ok=False is reported
        with patch.object(schedule.Scheduler, "every", side_effect=RuntimeError("Scheduler stage failure")):
            candidate, ok, err = apply_and_save_settings(
                self.scanner, {"RUN_INTERVAL": 10}, config_path=self.config_path
            )
            self.assertFalse(ok)
            self.assertIn("Scheduler stage failure", err)
            # Disk persistence succeeded
            reloaded = load_config(self.config_path)
            self.assertEqual(reloaded["RUN_INTERVAL"], 10)

    def test_reschedule_mixed_tagged_and_legacy_jobs_cleaned_in_single_pass(self):
        # Establish a tagged job AND an untagged legacy job bound to self.scanner.run_scan
        tagged_job = schedule.every(1).hours.do(self.scanner.run_scan).tag("scanner")
        legacy_job = schedule.every(2).hours.do(self.scanner.run_scan)
        self.assertEqual(len(schedule.jobs), 2)

        # Reschedule should clean BOTH in a single pass without leaving duplicates
        ok, err = self.scanner.reschedule(interval=6, interval_unit="hours")
        self.assertTrue(ok)
        self.assertNotIn(tagged_job, schedule.jobs)
        self.assertNotIn(legacy_job, schedule.jobs)
        scanner_jobs = [j for j in schedule.jobs if "scanner" in getattr(j, "tags", set())]
        self.assertEqual(len(scanner_jobs), 1)

    def test_apply_and_save_settings_reschedule_exception_contained(self):
        # Even if scanner.reschedule raises an unexpected exception, apply_and_save_settings
        # contains it, preserves disk persistence, and returns ok=False
        with patch.object(self.scanner, "reschedule", side_effect=RuntimeError("Unexpected reschedule explosion")):
            candidate, ok, err = apply_and_save_settings(
                self.scanner, {"RUN_INTERVAL": 14}, config_path=self.config_path
            )
            self.assertFalse(ok)
            self.assertIn("Unexpected reschedule explosion", err)
            reloaded = load_config(self.config_path)
            self.assertEqual(reloaded["RUN_INTERVAL"], 14)

    def test_apply_and_save_settings_plex_reconnect_warning_reported(self):
        # When Plex reconnect fails after save, apply_and_save_settings contains it and returns ok=False
        self.scanner.config["SERVER_TYPE"] = "plex"
        with patch.object(self.scanner, "connect_to_plex", side_effect=ConnectionError("Plex offline")):
            candidate, ok, err = apply_and_save_settings(
                self.scanner, {"SERVER_TYPE": "plex"}, config_path=self.config_path
            )
            self.assertFalse(ok)
            self.assertIn("Plex reconnect warning: Plex offline", err)
            reloaded = load_config(self.config_path)
            self.assertEqual(reloaded["SERVER_TYPE"], "plex")

    def test_apply_and_save_settings_plex_failed_library_discovery_reported(self):
        # When Plex connects but library discovery fails / clears self.plex, ok=False is reported
        self.scanner.config["SERVER_TYPE"] = "plex"
        mock_server = MagicMock()
        self.scanner.plex = mock_server

        def mock_connect(*args, **kwargs):
            self.scanner.plex = mock_server
            return mock_server

        def mock_get_libs():
            self.scanner.plex = None
            return {}

        with patch.object(self.scanner, "connect_to_plex", side_effect=mock_connect), \
             patch.object(self.scanner, "get_library_ids", side_effect=mock_get_libs):
            candidate, ok, err = apply_and_save_settings(
                self.scanner, {"SERVER_TYPE": "plex"}, config_path=self.config_path
            )
            self.assertFalse(ok)
            self.assertIn("failed to discover libraries", err)

    def test_apply_and_save_settings_normalizes_float_integers(self):
        # NiceGUI number inputs supply floats (e.g. 14.0).
        # Must serialize as integer strings so load_config's int cast succeeds and doesn't reset to defaults.
        settings_values = {
            "SCAN_SINCE_DAYS": 14.0,
            "NOTIFICATION_GROUP_WINDOW": 30.0,
            "SCAN_WORKERS": 8.0,
            "MAX_RETRIES": 5.0,
            "RUN_INTERVAL": 12.0,
            "CLEANUP_DAYS": 20.0,
            "DELETION_THRESHOLD": 40.0,
        }

        candidate, ok, err = apply_and_save_settings(
            self.scanner, settings_values, config_path=self.config_path
        )
        self.assertTrue(ok)
        self.assertEqual(candidate["SCAN_SINCE_DAYS"], 14)
        self.assertEqual(candidate["NOTIFICATION_GROUP_WINDOW"], 30)

        # Verify disk serialization and reload
        reloaded = load_config(self.config_path)
        self.assertEqual(reloaded["SCAN_SINCE_DAYS"], 14)
        self.assertEqual(reloaded["NOTIFICATION_GROUP_WINDOW"], 30)
        self.assertEqual(reloaded["SCAN_WORKERS"], 8)
        self.assertEqual(reloaded["MAX_RETRIES"], 5)
        self.assertEqual(reloaded["RUN_INTERVAL"], 12)
        self.assertEqual(reloaded["CLEANUP_DAYS"], 20)
        self.assertEqual(reloaded["DELETION_THRESHOLD"], 40)


if __name__ == "__main__":
    unittest.main()
