from __future__ import annotations

import json
import secrets
import sqlite3
import threading
from pathlib import Path

from flask import Flask, flash, jsonify, redirect, render_template, request, session, url_for

import auth
import backup
import db
import scheduler


def create_app() -> Flask:
    """Create and configure the YATB Flask application."""
    app = Flask(__name__)

    secret_path = Path(__file__).parent / ".secret_key"
    if secret_path.exists():
        app.config["SECRET_KEY"] = secret_path.read_bytes()
    else:
        secret = secrets.token_bytes(32)
        secret_path.write_bytes(secret)
        secret_path.chmod(0o600)
        app.config["SECRET_KEY"] = secret

    db.init_db()
    _ensure_default_admin()
    _ensure_defaults()
    _ensure_default_configs()

    queue = backup.BackupQueue()
    app.config["backup_queue"] = queue

    system_lock = threading.Lock()
    system_state = {"ssh": False, "samba": False}
    app.config["system_state"] = system_state
    app.config["system_lock"] = system_lock

    sched = scheduler.Scheduler(queue.enqueue, lambda: _start_ssh_task(system_state, system_lock, "scheduler"))
    sched.start()
    app.config["scheduler"] = sched

    @app.context_processor
    def inject_user():
        return {
            "current_user": {
                "id": session.get("user_id"),
                "username": session.get("username"),
                "role": session.get("role"),
            }
        }

    @app.route("/login", methods=["GET", "POST"])
    def login():
        if request.method == "POST":
            username = request.form.get("username", "").strip()
            password = request.form.get("password", "")
            user = db.find_user_by_username(username)
            if user and auth.verify_password(password, user["password_hash"]):
                session.clear()
                session["user_id"] = user["id"]
                session["username"] = user["username"]
                session["role"] = user["role"]
                db.set_last_login(int(user["id"]))
                return redirect(url_for("index"))
            flash("Invalid username or password", "error")
        return render_template("login.html")

    @app.route("/logout")
    def logout():
        session.clear()
        return redirect(url_for("login"))

    @app.route("/")
    @auth.login_required
    def index():
        profiles = _profiles_with_last_run()
        queue_status = queue.status()
        scheduler_enabled = db.get_setting("scheduler_enabled", "1") == "1"
        system_status = _system_status(system_state)
        return render_template(
            "index.html",
            profiles=profiles,
            queue_status=queue_status,
            scheduler_enabled=scheduler_enabled,
            system_status=system_status,
        )

    @app.route("/config/ssh", methods=["GET", "POST"])
    @auth.login_required
    def ssh_config():
        config = db.get_setting_json("ssh_config", {})
        schedule_time = db.get_setting("ssh_schedule_time", "")
        schedule_enabled = db.get_setting("ssh_schedule_enabled", "0") == "1"
        if request.method == "POST":
            raw = request.form.get("config_json", "{}")
            schedule_time = request.form.get("schedule_time", "").strip()
            schedule_enabled = request.form.get("schedule_enabled") == "on"
            try:
                data = json.loads(raw)
                if not isinstance(data, dict):
                    raise ValueError("Config must be a JSON object")
                db.set_setting_json("ssh_config", data)
                db.set_setting("ssh_schedule_time", schedule_time)
                db.set_setting("ssh_schedule_enabled", "1" if schedule_enabled else "0")
                flash("SSH configuration saved", "success")
                return redirect(url_for("ssh_config"))
            except (json.JSONDecodeError, ValueError) as exc:
                flash(f"Invalid JSON: {exc}", "error")
        return render_template(
            "ssh_config.html",
            config_json=json.dumps(config, indent=2),
            schedule_time=schedule_time,
            schedule_enabled=schedule_enabled,
        )

    @app.route("/config/samba", methods=["GET", "POST"])
    @auth.login_required
    def samba_config():
        config = db.get_setting_json("samba_config", {})
        if request.method == "POST":
            raw = request.form.get("config_json", "{}")
            try:
                data = json.loads(raw)
                if not isinstance(data, dict):
                    raise ValueError("Config must be a JSON object")
                db.set_setting_json("samba_config", data)
                flash("Samba configuration saved", "success")
                return redirect(url_for("samba_config"))
            except (json.JSONDecodeError, ValueError) as exc:
                flash(f"Invalid JSON: {exc}", "error")
        return render_template("samba_config.html", config_json=json.dumps(config, indent=2))

    @app.route("/profiles")
    @auth.login_required
    def profiles():
        profiles = db.list_profiles()
        return render_template("profiles.html", profiles=profiles)

    @app.route("/profiles/new", methods=["GET", "POST"])
    @auth.login_required
    def profile_new():
        if request.method == "POST":
            form = _profile_form_to_data(request.form)
            try:
                db.create_profile(**form)
                flash("Profile created", "success")
                return redirect(url_for("profiles"))
            except sqlite3.IntegrityError:
                flash("Profile name must be unique", "error")
        return render_template("profile_edit.html", profile=None)

    @app.route("/profiles/<int:profile_id>/edit", methods=["GET", "POST"])
    @auth.login_required
    def profile_edit(profile_id: int):
        profile = db.get_profile(profile_id)
        if profile is None:
            flash("Profile not found", "error")
            return redirect(url_for("profiles"))
        profile_form = _profile_to_form(profile)
        if request.method == "POST":
            form = _profile_form_to_data(request.form)
            try:
                db.update_profile(profile_id, **form)
                flash("Profile updated", "success")
                return redirect(url_for("profiles"))
            except sqlite3.IntegrityError:
                flash("Profile name must be unique", "error")
        return render_template("profile_edit.html", profile=profile_form)

    @app.route("/profiles/<int:profile_id>/delete", methods=["POST"])
    @auth.login_required
    def profile_delete(profile_id: int):
        db.delete_profile(profile_id)
        flash("Profile deleted", "success")
        return redirect(url_for("profiles"))

    @app.route("/api/profiles/<int:profile_id>/run", methods=["POST"])
    @auth.login_required
    def api_profile_run(profile_id: int):
        enqueued = queue.enqueue(profile_id, "manual")
        if not enqueued:
            return jsonify({"success": False, "message": "Already running or queued"})
        return jsonify({"success": True})

    @app.route("/runs")
    @auth.login_required
    def runs():
        runs = db.list_runs(100)
        return render_template("runs.html", runs=runs)

    @app.route("/system-runs")
    @auth.login_required
    def system_runs():
        runs = db.list_system_runs(100)
        return render_template("system_runs.html", runs=runs)

    @app.route("/system-runs/<int:run_id>")
    @auth.login_required
    def system_run_detail(run_id: int):
        run = db.get_system_run(run_id)
        if run is None:
            flash("Run not found", "error")
            return redirect(url_for("system_runs"))
        return render_template("system_run_detail.html", run=run)

    @app.route("/runs/<int:run_id>")
    @auth.login_required
    def run_detail(run_id: int):
        run = db.get_run(run_id)
        if run is None:
            flash("Run not found", "error")
            return redirect(url_for("runs"))
        return render_template("run_detail.html", run=run)

    @app.route("/users")
    @auth.login_required
    @auth.require_role("admin")
    def users():
        users = db.list_users()
        return render_template("users.html", users=users)

    @app.route("/users/new", methods=["GET", "POST"])
    @auth.login_required
    @auth.require_role("admin")
    def user_new():
        if request.method == "POST":
            username = request.form.get("username", "").strip()
            role = request.form.get("role", "operator")
            password = request.form.get("password", "")
            if not username or not password:
                flash("Username and password are required", "error")
                return redirect(url_for("user_new"))
            try:
                db.create_user(username, auth.hash_password(password), role)
                flash("User created", "success")
                return redirect(url_for("users"))
            except sqlite3.IntegrityError:
                flash("Username must be unique", "error")
        return render_template("user_edit.html", user=None)

    @app.route("/users/<int:user_id>/edit", methods=["GET", "POST"])
    @auth.login_required
    @auth.require_role("admin")
    def user_edit(user_id: int):
        user = db.find_user_by_id(user_id)
        if user is None:
            flash("User not found", "error")
            return redirect(url_for("users"))
        if request.method == "POST":
            username = request.form.get("username", "").strip()
            role = request.form.get("role", "operator")
            password = request.form.get("password", "")
            password_hash = auth.hash_password(password) if password else None
            try:
                db.update_user(user_id, username, role, password_hash)
                flash("User updated", "success")
                return redirect(url_for("users"))
            except sqlite3.IntegrityError:
                flash("Username must be unique", "error")
        return render_template("user_edit.html", user=user)

    @app.route("/users/<int:user_id>/delete", methods=["POST"])
    @auth.login_required
    @auth.require_role("admin")
    def user_delete(user_id: int):
        if session.get("user_id") == user_id:
            flash("You cannot delete your own account", "error")
            return redirect(url_for("users"))
        db.delete_user(user_id)
        flash("User deleted", "success")
        return redirect(url_for("users"))

    @app.route("/api/status")
    @auth.login_required
    def api_status():
        scheduler_enabled = db.get_setting("scheduler_enabled", "1") == "1"
        return jsonify({
            "queue": queue.status(),
            "scheduler_enabled": scheduler_enabled,
            "system": _system_status(system_state),
        })

    @app.route("/api/scheduler/toggle", methods=["POST"])
    @auth.login_required
    def api_scheduler_toggle():
        current = db.get_setting("scheduler_enabled", "1") == "1"
        db.set_setting("scheduler_enabled", "0" if current else "1")
        return jsonify({"success": True, "enabled": not current})

    @app.route("/api/system/ssh/run", methods=["POST"])
    @auth.login_required
    def api_run_ssh():
        return _run_system_task("ssh", system_state, system_lock)

    @app.route("/api/system/samba/run", methods=["POST"])
    @auth.login_required
    def api_run_samba():
        return _run_system_task("samba", system_state, system_lock)

    return app


def _profiles_with_last_run():
    profiles = []
    for profile in db.list_profiles():
        last_run = db.get_last_run_for_profile(int(profile["id"]))
        profiles.append({
            "profile": profile,
            "last_run": last_run,
        })
    return profiles


def _ensure_default_admin() -> None:
    if db.list_users():
        return
    db.create_user("admin", auth.hash_password("admin"), "admin")


def _ensure_defaults() -> None:
    if db.get_setting("scheduler_enabled") is None:
        db.set_setting("scheduler_enabled", "1")
    if db.get_setting("ssh_schedule_enabled") is None:
        db.set_setting("ssh_schedule_enabled", "0")
    if db.get_setting("ssh_schedule_time") is None:
        db.set_setting("ssh_schedule_time", "")


def _ensure_default_configs() -> None:
    if db.get_setting("ssh_config") is None:
        db.set_setting_json(
            "ssh_config",
            {
                "local_backup_dir": "/tmp/ssh_backups",
                "exclude_patterns": ["*.tmp", "*.log"],
                "servers": [
                    {
                        "name": "example-server",
                        "enabled": False,
                        "host": "192.168.1.100",
                        "port": 22,
                        "username": "backup",
                        "password": "",
                        "ssh_key_path": "~/.ssh/id_rsa",
                        "pre_commands": [
                            {
                                "description": "Dump database",
                                "command": "pg_dump -U postgres mydb > /tmp/mydb.sql",
                                "use_sudo": False,
                                "timeout": 300
                            }
                        ],
                        "pre_commands_use_sudo": False,
                        "remote_paths": ["/etc", "/home"],
                        "use_sudo": False,
                        "sudo_password": "",
                        "use_compression": True,
                        "exclude_patterns": []
                    }
                ]
            },
        )
    if db.get_setting("samba_config") is None:
        db.set_setting_json(
            "samba_config",
            {
                "samba_enabled": False,
                "samba_workgroup": "WORKGROUP",
                "samba_description": "Atlas Backup",
                "samba_server_name": "BACKUP-SERVER",
                    "sudo_password": "",
                "force_user": "",
                "force_group": "",
                "create_mask": "0755",
                "directory_mask": "0755",
                "shares": [
                    {
                        "name": "backups",
                        "path": "/srv/backups",
                        "comment": "Backup archive",
                        "read_only": False,
                        "guest_ok": False,
                        "valid_users": []
                    }
                ],
                "samba_users": [
                    {"username": "backup", "password": "", "enabled": False}
                ]
            },
        )


def _system_status(state: dict) -> dict:
    return {"ssh": state.get("ssh", False), "samba": state.get("samba", False)}


def _start_ssh_task(state: dict, lock: threading.Lock, triggered_by: str = "manual") -> bool:
    return _run_system_task("ssh", state, lock, async_mode=True, use_json=False, triggered_by=triggered_by)


def _run_system_task(
    task_type: str,
    state: dict,
    lock: threading.Lock,
    async_mode: bool = True,
    use_json: bool = True,
    triggered_by: str = "manual",
):
    with lock:
        if state.get(task_type):
            if use_json:
                return jsonify({"success": False, "message": "Task already running"}), 400
            return False
        state[task_type] = True

    def runner():
        run_id = db.create_system_run(task_type)
        log_lines = []

        def log(message: str) -> None:
            stamp = db.now_iso()
            log_lines.append(f"{stamp} {message}")

        status = "failed"
        message = "Unknown error"
        try:
            if task_type == "ssh":
                config = db.get_setting_json("ssh_config", {})
                status, message = backup.SSHBackupRunner(config, log).run()
            elif task_type == "samba":
                config = db.get_setting_json("samba_config", {})
                status, message = backup.SambaManager(config, log).run()
            else:
                status, message = "failed", "Unknown task type"
        except Exception as exc:
            status = "failed"
            message = f"Unexpected error: {exc}"
            log(message)
        finally:
            db.finish_system_run(run_id, status, message, "\n".join(log_lines))
            
            # Update last_scheduled_date on successful completion when triggered by scheduler
            if status == "success" and triggered_by == "scheduler":
                if task_type == "ssh":
                    today = db.now_iso()[:10]  # Extract date part (YYYY-MM-DD)
                    db.set_setting("ssh_last_scheduled_date", today)
            
            with lock:
                state[task_type] = False

    if async_mode:
        thread = threading.Thread(target=runner, daemon=True)
        thread.start()
        if use_json:
            return jsonify({"success": True})
        return True
    runner()
    return True


def _profile_form_to_data(form):
    name = form.get("name", "").strip()
    source_path = form.get("source_path", "").strip()
    dest_path = form.get("dest_path", "").strip()
    exclude_raw = form.get("exclude_patterns", "")
    exclude_patterns = [line.strip() for line in exclude_raw.splitlines() if line.strip()]
    schedule_time = form.get("schedule_time", "").strip() or None
    schedule_enabled = form.get("schedule_enabled") == "on"
    try:
        retention_count = int(form.get("retention_count", "7") or 0)
    except ValueError:
        retention_count = 7
    verify_mode = form.get("verify_mode", "size")
    return {
        "name": name,
        "source_path": source_path,
        "dest_path": dest_path,
        "exclude_patterns": exclude_patterns,
        "schedule_time": schedule_time,
        "schedule_enabled": schedule_enabled,
        "retention_count": retention_count,
        "verify_mode": verify_mode,
    }


def _profile_to_form(profile):
    raw = profile["exclude_patterns"] or "[]"
    try:
        data = json.loads(raw)
        patterns = [str(item) for item in data] if isinstance(data, list) else []
    except json.JSONDecodeError:
        patterns = [line.strip() for line in raw.splitlines() if line.strip()]
    exclude_text = "\n".join(patterns)
    data = dict(profile)
    data["exclude_text"] = exclude_text
    return data


app = create_app()


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)
