"""YATB - Yet Another Backup Tool - Scheduler module.

Background scheduler for periodic backup execution.
"""
from __future__ import annotations

import calendar
import threading
from datetime import datetime, timedelta

import db

VALID_FREQUENCIES = {"day", "week", "month", "year"}


def _normalize_frequency(value: str | None) -> str:
    if not value:
        return "day"
    frequency = str(value).strip().lower()
    return frequency if frequency in VALID_FREQUENCIES else "day"


def _parse_utc_to_local(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return datetime.fromtimestamp(parsed.timestamp())


def _add_months(value: datetime, months: int) -> datetime:
    month_index = value.month - 1 + months
    year = value.year + month_index // 12
    month = month_index % 12 + 1
    day = min(value.day, calendar.monthrange(year, month)[1])
    return value.replace(year=year, month=month, day=day)


def _add_years(value: datetime, years: int) -> datetime:
    target_year = value.year + years
    day = min(value.day, calendar.monthrange(target_year, value.month)[1])
    return value.replace(year=target_year, day=day)


def _next_scheduled_after(last_started_local: datetime, schedule_time: str, frequency: str) -> datetime:
    hour_text, minute_text = schedule_time.split(":", 1)
    base = last_started_local.replace(hour=int(hour_text), minute=int(minute_text), second=0, microsecond=0)

    if frequency == "week":
        return base + timedelta(days=7)
    if frequency == "month":
        return _add_months(base, 1)
    if frequency == "year":
        return _add_years(base, 1)
    return base + timedelta(days=1)


def _is_due_from_last_success(
    now_local: datetime,
    last_started_local: datetime,
    schedule_time: str,
    frequency: str,
) -> bool:
    today_schedule_passed = now_local.strftime("%H:%M") >= schedule_time

    # If a success happened earlier today before schedule time, still allow first scheduled run.
    if last_started_local.date() == now_local.date() and last_started_local.strftime("%H:%M") < schedule_time:
        return today_schedule_passed

    next_due = _next_scheduled_after(last_started_local, schedule_time, frequency)
    return now_local >= next_due


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
        current_time = now_local.strftime("%H:%M")

        for profile in profiles:
            if not profile["schedule_enabled"]:
                continue
            schedule_time = profile["schedule_time"]
            schedule_frequency = _normalize_frequency(profile["schedule_frequency"])
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
                # Last run was successful, run based on configured frequency.
                last_started_at = last_run["started_at"]
                if last_started_at:
                    try:
                        last_started_local = _parse_utc_to_local(last_started_at)
                        should_run = _is_due_from_last_success(
                            now_local,
                            last_started_local,
                            schedule_time,
                            schedule_frequency,
                        )
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

        self._run_ssh_schedule(current_time)

    def _run_ssh_schedule(self, current_time: str) -> None:
        enabled = db.get_setting("ssh_schedule_enabled", "0") == "1"
        schedule_time = db.get_setting("ssh_schedule_time")
        schedule_frequency = _normalize_frequency(db.get_setting("ssh_schedule_frequency", "day"))
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
            # Last run was successful, run based on configured frequency.
            last_started_at = last_run["started_at"]
            if last_started_at:
                try:
                    last_started_local = _parse_utc_to_local(last_started_at)
                    should_run = _is_due_from_last_success(
                        now_local,
                        last_started_local,
                        schedule_time,
                        schedule_frequency,
                    )
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
