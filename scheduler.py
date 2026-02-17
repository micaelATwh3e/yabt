"""YATB - Yet Another Backup Tool - Scheduler module.

Background scheduler for periodic backup execution.
"""
from __future__ import annotations

import threading
import time
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
        now = datetime.now()
        today = now.strftime("%Y-%m-%d")
        current_time = now.strftime("%H:%M")

        for profile in profiles:
            if not profile["schedule_enabled"]:
                continue
            schedule_time = profile["schedule_time"]
            if not schedule_time:
                continue
            if current_time < schedule_time:
                continue
            last_date = profile["last_scheduled_date"]
            if last_date == today:
                continue
            enqueued = self._enqueue(int(profile["id"]), "scheduler")
            if enqueued:
                db.set_profile_last_scheduled(int(profile["id"]), today)

        self._run_ssh_schedule(current_time, today)

    def _run_ssh_schedule(self, current_time: str, today: str) -> None:
        enabled = db.get_setting("ssh_schedule_enabled", "0") == "1"
        schedule_time = db.get_setting("ssh_schedule_time")
        if not enabled or not schedule_time:
            return
        if current_time < schedule_time:
            return
        last_date = db.get_setting("ssh_last_scheduled_date")
        if last_date == today:
            return
        started = self._ssh_callback()
        if started:
            db.set_setting("ssh_last_scheduled_date", today)
