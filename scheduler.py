"""YATB - Yet Another Backup Tool - Scheduler module.

Background scheduler for periodic backup execution.
"""
from __future__ import annotations

import threading
from datetime import datetime

import db


class Scheduler:
    def __init__(self, enqueue_callback, ssh_callback) -> None:
        self._enqueue = enqueue_callback
        self._ssh_callback = ssh_callback
        self._stop_event = threading.Event()
        self._thread = threading.Thread(target=self._loop, daemon=True)

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()

    def _loop(self) -> None:
        while not self._stop_event.is_set():
            enabled = db.get_setting("scheduler_enabled", "1") == "1"
            if enabled:
                self._run_cycle()
            self._stop_event.wait(30)

    def _run_cycle(self) -> None:
        profiles = db.list_profiles()
        now_local = datetime.now()
        now_utc = datetime.utcnow()
        today = now_local.strftime("%Y-%m-%d")
        current_time = now_local.strftime("%H:%M")

        for profile in profiles:
            if not profile["schedule_enabled"]:
                continue
            schedule_time = profile["schedule_time"]
            if not schedule_time:
                continue
            if current_time < schedule_time:
                continue
            
            # Check if we should run based on last run status
            last_run = db.get_last_run_for_profile(int(profile["id"]))
            should_run = False
            
            if last_run is None:
                # Never run before, run now if time is right
                should_run = True
            elif last_run["status"] == "success":
                # Last run was successful, only run if last successful run was before today's schedule time
                last_started_at = last_run["started_at"]
                if last_started_at:
                    try:
                        last_started_utc = datetime.fromisoformat(last_started_at.replace("Z", "+00:00"))
                        # Convert UTC to local time for comparison
                        last_started_local = datetime.fromtimestamp(last_started_utc.timestamp())
                        
                        # Check if last run was today (local timezone)
                        if last_started_local.date() == now_local.date():
                            last_time = last_started_local.strftime("%H:%M")
                            # Don't run if we already ran today at or after the schedule time
                            if last_time >= schedule_time:
                                should_run = False
                            else:
                                # Last run was today but before schedule time, so this is the first run at schedule time
                                should_run = True
                        else:
                            # Last run was a different day, allow run
                            should_run = True
                    except (ValueError, AttributeError, TypeError):
                        # Can't parse date, allow run
                        should_run = True
                else:
                    should_run = True
            else:
                # Last run failed or still running, retry every hour
                last_finished_at = last_run["finished_at"]
                if last_finished_at:
                    try:
                        last_finished = datetime.fromisoformat(last_finished_at.replace("Z", "+00:00"))
                        hours_since_last = (now_utc - last_finished.replace(tzinfo=None)).total_seconds() / 3600
                        if hours_since_last >= 1.0:
                            should_run = True
                    except (ValueError, AttributeError, TypeError):
                        # If we can't parse the date, don't run to avoid continuous retries
                        should_run = False
                else:
                    # No finished_at time (still running), don't run
                    should_run = False
            
            if should_run:
                enqueued = self._enqueue(int(profile["id"]), "scheduler")
                # Note: last_scheduled_date will be updated on successful completion

        self._run_ssh_schedule(current_time, today)

    def _run_ssh_schedule(self, current_time: str, today: str) -> None:
        enabled = db.get_setting("ssh_schedule_enabled", "0") == "1"
        schedule_time = db.get_setting("ssh_schedule_time")
        if not enabled or not schedule_time:
            return
        if current_time < schedule_time:
            return
        
        # Check if we should run based on last run status
        now_local = datetime.now()
        now_utc = datetime.utcnow()
        last_run = db.get_last_system_run_for_task("ssh")
        should_run = False
        
        if last_run is None:
            # Never run before, run now if time is right
            should_run = True
        elif last_run["status"] == "success":
            # Last run was successful, only run if last successful run was before today's schedule time
            last_started_at = last_run["started_at"]
            if last_started_at:
                try:
                    last_started_utc = datetime.fromisoformat(last_started_at.replace("Z", "+00:00"))
                    # Convert UTC to local time for comparison
                    last_started_local = datetime.fromtimestamp(last_started_utc.timestamp())
                    
                    # Check if last run was today (local timezone)
                    if last_started_local.date() == now_local.date():
                        last_time = last_started_local.strftime("%H:%M")
                        # Don't run if we already ran today at or after the schedule time
                        if last_time >= schedule_time:
                            should_run = False
                        else:
                            # Last run was today but before schedule time, so this is the first run at schedule time
                            should_run = True
                    else:
                        # Last run was a different day, allow run
                        should_run = True
                except (ValueError, AttributeError, TypeError):
                    # Can't parse date, allow run
                    should_run = True
            else:
                should_run = True
        else:
            # Last run failed or still running, retry every hour
            last_finished_at = last_run["finished_at"]
            if last_finished_at:
                try:
                    last_finished = datetime.fromisoformat(last_finished_at.replace("Z", "+00:00"))
                    hours_since_last = (now_utc - last_finished.replace(tzinfo=None)).total_seconds() / 3600
                    if hours_since_last >= 1.0:
                        should_run = True
                except (ValueError, AttributeError, TypeError):
                    # If we can't parse the date, don't run to avoid continuous retries
                    should_run = False
            else:
                # No finished_at time (still running), don't run
                should_run = False
        
        if should_run:
            started = self._ssh_callback()
            # Note: ssh_last_scheduled_date will be updated on successful completion
