"""YATB - Yet Another Backup Tool - Backup runners and managers.

Implements local file backups, SSH remote backups, and Samba configuration.
"""
from __future__ import annotations

import fnmatch
import hashlib
import json
import os
import shutil
import stat
import subprocess
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Iterable, Optional

import db


class BackupQueue:
    def __init__(self) -> None:
        self._queue: list[dict] = []
        self._running_profiles: set[int] = set()
        self._lock = threading.Lock()
        self._condition = threading.Condition(self._lock)
        self._worker = threading.Thread(target=self._worker_loop, daemon=True)
        self._worker.start()

    def enqueue(self, profile_id: int, triggered_by: str) -> bool:
        with self._condition:
            if profile_id in self._running_profiles:
                return False
            if any(item["profile_id"] == profile_id for item in self._queue):
                return False
            self._queue.append({"profile_id": profile_id, "triggered_by": triggered_by})
            self._condition.notify()
            return True

    def status(self) -> dict:
        with self._lock:
            return {
                "running": sorted(self._running_profiles),
                "queued": [item["profile_id"] for item in self._queue],
            }

    def _worker_loop(self) -> None:
        while True:
            with self._condition:
                while not self._queue:
                    self._condition.wait()
                item = self._queue.pop(0)
                profile_id = int(item["profile_id"])
                self._running_profiles.add(profile_id)
            try:
                run_backup(profile_id, item["triggered_by"])
            finally:
                with self._condition:
                    self._running_profiles.discard(profile_id)


def run_backup(profile_id: int, triggered_by: str) -> None:
    profile = db.get_profile(profile_id)
    if profile is None:
        return

    run_id = db.create_run(profile_id, triggered_by)
    log_lines: list[str] = []

    def log(message: str) -> None:
        timestamp = datetime.utcnow().isoformat(timespec="seconds") + "Z"
        log_lines.append(f"{timestamp} {message}")

    try:
        status, message = run_profile_backup(profile, log)
    except Exception as exc:
        status = "failed"
        message = f"Unexpected error: {exc}"
        log(message)
    db.finish_run(run_id, status, message, "\n".join(log_lines))


def run_profile_backup(profile, log) -> tuple[str, str]:
    source = Path(profile["source_path"]).expanduser()
    dest_base = Path(profile["dest_path"]).expanduser() / profile["name"]

    if not source.exists():
        log(f"Source does not exist: {source}")
        return "failed", "Source path does not exist"

    dest_base.mkdir(parents=True, exist_ok=True)
    try:
        if dest_base.resolve().is_relative_to(source.resolve()):
            log("Destination is inside the source path; refusing to run")
            return "failed", "Destination cannot be inside source"
    except AttributeError:
        source_resolved = source.resolve()
        dest_resolved = dest_base.resolve()
        if str(dest_resolved).startswith(str(source_resolved) + os.sep):
            log("Destination is inside the source path; refusing to run")
            return "failed", "Destination cannot be inside source"
    timestamp = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
    dest_root = dest_base / timestamp

    patterns = _parse_patterns(profile["exclude_patterns"])
    verify_mode = profile["verify_mode"]
    retention_count = int(profile["retention_count"] or 0)

    copied = 0
    skipped = 0
    errors = 0

    log(f"Starting backup: {source} -> {dest_root}")

    for root, dirs, files in os.walk(source):
        rel_dir = os.path.relpath(root, source)
        rel_dir = "" if rel_dir == "." else rel_dir
        if _should_exclude(rel_dir, patterns):
            dirs[:] = []
            skipped += 1
            continue

        dest_dir = dest_root / rel_dir
        dest_dir.mkdir(parents=True, exist_ok=True)

        for filename in files:
            rel_file = os.path.join(rel_dir, filename) if rel_dir else filename
            if _should_exclude(rel_file, patterns):
                skipped += 1
                continue
            src_path = Path(root) / filename
            dest_path = dest_dir / filename
            try:
                shutil.copy2(src_path, dest_path)
                copied += 1
            except Exception as exc:
                errors += 1
                log(f"Copy failed for {src_path}: {exc}")

    mismatches = 0
    if verify_mode in {"size", "hash"}:
        log(f"Verifying files using {verify_mode}")
        mismatches = _verify_backup(source, dest_root, patterns, verify_mode, log)

    if retention_count > 0:
        _apply_retention(dest_base, retention_count, log)

    if errors > 0 or mismatches > 0:
        return "failed", f"Completed with {errors} errors and {mismatches} mismatches"

    return "success", f"Completed: {copied} files copied, {skipped} skipped"


def _parse_patterns(raw: str) -> list[str]:
    try:
        data = json.loads(raw)
        if isinstance(data, list):
            return [str(item) for item in data if str(item).strip()]
    except json.JSONDecodeError:
        return [item.strip() for item in raw.splitlines() if item.strip()]
    return []


def _should_exclude(path: str, patterns: Iterable[str]) -> bool:
    if not path:
        return False
    for pattern in patterns:
        if fnmatch.fnmatch(path, pattern) or fnmatch.fnmatch(Path(path).name, pattern):
            return True
    return False


def _verify_backup(
    source: Path,
    dest_root: Path,
    patterns: Iterable[str],
    mode: str,
    log,
) -> int:
    mismatches = 0
    for root, _, files in os.walk(source):
        rel_dir = os.path.relpath(root, source)
        rel_dir = "" if rel_dir == "." else rel_dir
        if _should_exclude(rel_dir, patterns):
            continue
        for filename in files:
            rel_file = os.path.join(rel_dir, filename) if rel_dir else filename
            if _should_exclude(rel_file, patterns):
                continue
            src_path = Path(root) / filename
            dest_path = dest_root / rel_file
            if not dest_path.exists():
                mismatches += 1
                log(f"Missing destination file: {dest_path}")
                continue
            if mode == "size":
                if src_path.stat().st_size != dest_path.stat().st_size:
                    mismatches += 1
                    log(f"Size mismatch: {rel_file}")
            else:
                if _hash_file(src_path) != _hash_file(dest_path):
                    mismatches += 1
                    log(f"Hash mismatch: {rel_file}")
    return mismatches


def _hash_file(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _apply_retention(dest_base: Path, retention_count: int, log) -> None:
    runs = sorted([p for p in dest_base.iterdir() if p.is_dir()])
    if len(runs) <= retention_count:
        return
    to_remove = runs[: len(runs) - retention_count]
    for run_dir in to_remove:
        try:
            shutil.rmtree(run_dir)
            log(f"Removed old backup: {run_dir.name}")
        except Exception as exc:
            log(f"Failed to remove {run_dir.name}: {exc}")


class SSHBackupRunner:
    def __init__(self, config: dict, log) -> None:
        self.config = config
        self.log = log

    def run(self) -> tuple[str, str]:
        try:
            import paramiko
        except Exception:
            self.log("paramiko is not installed; SSH backup is unavailable")
            return "failed", "paramiko not installed"

        servers = self.config.get("servers", [])
        if not servers:
            self.log("No SSH servers configured")
            return "failed", "No SSH servers configured"

        local_base = Path(self.config.get("local_backup_dir", ""))
        if not str(local_base):
            self.log("Missing local_backup_dir")
            return "failed", "Missing local_backup_dir"
        local_base.mkdir(parents=True, exist_ok=True)

        default_excludes = self.config.get("exclude_patterns", [])
        timestamp = datetime.utcnow().strftime("%Y%m%d-%H%M%S")

        for server in servers:
            if not server.get("enabled", True):
                continue
            status, message = self._backup_server(paramiko, server, local_base, default_excludes, timestamp)
            if status != "success":
                return status, message

        return "success", "SSH backups completed"

    def _backup_server(self, paramiko, server: dict, local_base: Path, base_excludes: list, timestamp: str):
        name = server.get("name") or server.get("host")
        self.log(f"Starting SSH backup for {name}")
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        connect_kwargs = {
            "hostname": server.get("host"),
            "port": int(server.get("port", 22)),
            "username": server.get("username"),
            "timeout": 30,
        }
        if server.get("ssh_key_path"):
            connect_kwargs["key_filename"] = os.path.expanduser(server.get("ssh_key_path"))
        if server.get("password"):
            connect_kwargs["password"] = server.get("password")

        try:
            ssh.connect(**connect_kwargs)
        except Exception as exc:
            self.log(f"SSH connect failed for {name}: {exc}")
            return "failed", f"SSH connect failed for {name}"

        sftp = ssh.open_sftp()
        backup_dir = local_base / name / timestamp
        backup_dir.mkdir(parents=True, exist_ok=True)

        exclude_patterns = list(base_excludes) + list(server.get("exclude_patterns", []))
        use_sudo = bool(server.get("use_sudo", False))
        sudo_password = server.get("sudo_password", "")
        use_compression = bool(server.get("use_compression", True))
        pre_commands = []
        if isinstance(server.get("pre_commands"), list):
            pre_commands.extend(server.get("pre_commands"))
        if isinstance(server.get("pre_backup_commands"), list):
            pre_commands.extend(server.get("pre_backup_commands"))
        pre_commands_use_sudo = bool(server.get("pre_commands_use_sudo", use_sudo))

        try:
            if pre_commands:
                ok = self._run_pre_commands(ssh, pre_commands, pre_commands_use_sudo, sudo_password)
                if not ok:
                    return "failed", f"Pre-commands failed for {name}"
            for remote_path in server.get("remote_paths", []):
                if use_compression:
                    status = self._download_remote_archive(
                        ssh,
                        sftp,
                        remote_path,
                        backup_dir,
                        exclude_patterns,
                        use_sudo,
                        sudo_password,
                        name,
                        timestamp,
                    )
                    if not status:
                        return "failed", f"Failed to backup {remote_path}"
                else:
                    status = self._download_remote_path(
                        ssh,
                        sftp,
                        remote_path,
                        backup_dir,
                        exclude_patterns,
                        use_sudo,
                        sudo_password,
                    )
                    if not status:
                        return "failed", f"Failed to backup {remote_path}"
        finally:
            sftp.close()
            ssh.close()

        return "success", f"SSH backup finished for {name}"

    def _run_pre_commands(self, ssh, commands: list, use_sudo: bool, sudo_password: str) -> bool:
        for item in commands:
            command, run_sudo, timeout = self._normalize_pre_command(item, use_sudo)
            if not command:
                continue
            cmd = command
            if run_sudo:
                cmd = f"sudo -S sh -c \"{command}\""
            self.log(f"Running pre-command: {command}")
            stdin, stdout, stderr = ssh.exec_command(cmd, timeout=timeout)
            if run_sudo and sudo_password:
                stdin.write(sudo_password + "\n")
                stdin.flush()
            exit_status = stdout.channel.recv_exit_status()
            if exit_status != 0:
                error = stderr.read().decode().strip()
                self.log(f"Pre-command failed: {command} ({error})")
                return False
        return True

    def _normalize_pre_command(self, item, default_sudo: bool) -> tuple[str, bool, int]:
        if isinstance(item, dict):
            command = str(item.get("command", "")).strip()
            run_sudo = bool(item.get("use_sudo", default_sudo))
            timeout = int(item.get("timeout", 3600))
            return command, run_sudo, timeout
        command = str(item).strip()
        return command, default_sudo, 3600

    def _download_remote_archive(
        self,
        ssh,
        sftp,
        remote_path: str,
        backup_dir: Path,
        exclude_patterns: list,
        use_sudo: bool,
        sudo_password: str,
        server_name: str,
        timestamp: str,
    ) -> bool:
        path_base = os.path.basename(remote_path.rstrip("/")) or "root"
        archive_name = f"{path_base}_{timestamp}.tar.gz"
        remote_archive = f"/tmp/backup_{server_name}_{timestamp}_{path_base}.tar.gz"
        local_archive = backup_dir / archive_name

        if not self._create_remote_archive(ssh, remote_path, remote_archive, exclude_patterns, use_sudo, sudo_password):
            return False

        try:
            try:
                sftp.get(remote_archive, str(local_archive))
            except Exception:
                if use_sudo:
                    self.log(f"Downloading archive with sudo: {remote_archive}")
                    data = self._sudo_cat(ssh, remote_archive, sudo_password)
                    if data is None:
                        return False
                    with open(local_archive, "wb") as handle:
                        handle.write(data)
                else:
                    return False
        finally:
            self._remote_rm(ssh, remote_archive, use_sudo, sudo_password)

        return True

    def _download_remote_path(
        self,
        ssh,
        sftp,
        remote_path: str,
        backup_dir: Path,
        exclude_patterns: list,
        use_sudo: bool,
        sudo_password: str,
    ) -> bool:
        try:
            attrs = sftp.stat(remote_path)
            is_dir = stat.S_ISDIR(attrs.st_mode)
        except IOError:
            if not use_sudo:
                self.log(f"Remote path not accessible: {remote_path}")
                return False
            exists = self._sudo_exists(ssh, remote_path, sudo_password)
            if not exists:
                self.log(f"Remote path not accessible: {remote_path}")
                return False
            is_dir = True

        base_name = os.path.basename(remote_path.rstrip("/"))
        local_path = backup_dir / (base_name or "root")

        if is_dir:
            self._download_directory(sftp, remote_path, local_path, exclude_patterns)
            return True

        return self._download_file(sftp, remote_path, local_path)

    def _download_directory(self, sftp, remote_path: str, local_path: Path, exclude_patterns: list) -> None:
        local_path.mkdir(parents=True, exist_ok=True)
        try:
            items = sftp.listdir_attr(remote_path)
        except IOError as exc:
            self.log(f"Cannot access {remote_path}: {exc}")
            return

        for item in items:
            remote_item = os.path.join(remote_path, item.filename).replace("\\", "/")
            local_item = local_path / item.filename
            if _should_exclude(remote_item, exclude_patterns):
                continue
            if stat.S_ISDIR(item.st_mode):
                self._download_directory(sftp, remote_item, local_item, exclude_patterns)
            else:
                self._download_file(sftp, remote_item, local_item)

    def _download_file(self, sftp, remote_path: str, local_path: Path) -> bool:
        try:
            local_path.parent.mkdir(parents=True, exist_ok=True)
            sftp.get(remote_path, str(local_path))
            return True
        except Exception as exc:
            self.log(f"Failed to download {remote_path}: {exc}")
            return False

    def _create_remote_archive(
        self,
        ssh,
        remote_path: str,
        archive_path: str,
        exclude_patterns: list,
        use_sudo: bool,
        sudo_password: str,
    ) -> bool:
        parent_dir = os.path.dirname(remote_path.rstrip("/"))
        base = os.path.basename(remote_path.rstrip("/"))
        exclude_args = " ".join([f"--exclude='{pattern}'" for pattern in exclude_patterns])
        tar_cmd = f"cd '{parent_dir}' && tar -czf '{archive_path}' {exclude_args} '{base}' 2>/dev/null"
        if use_sudo:
            tar_cmd = f"sudo -S sh -c \"{tar_cmd}\""
        stdin, stdout, stderr = ssh.exec_command(tar_cmd, timeout=3600)
        if use_sudo and sudo_password:
            stdin.write(sudo_password + "\n")
            stdin.flush()
        exit_status = stdout.channel.recv_exit_status()
        if exit_status != 0:
            self.log(f"Remote archive failed: {stderr.read().decode()}")
            return False
        return True

    def _remote_rm(self, ssh, path: str, use_sudo: bool, sudo_password: str) -> None:
        rm_cmd = f"rm -f '{path}'"
        if use_sudo:
            rm_cmd = f"sudo -S {rm_cmd}"
        stdin, _, _ = ssh.exec_command(rm_cmd)
        if use_sudo and sudo_password:
            stdin.write(sudo_password + "\n")
            stdin.flush()

    def _sudo_exists(self, ssh, path: str, sudo_password: str) -> bool:
        cmd = f"sudo -S test -e '{path}' && echo 'yes'"
        stdin, stdout, _ = ssh.exec_command(cmd)
        if sudo_password:
            stdin.write(sudo_password + "\n")
            stdin.flush()
        return "yes" in stdout.read().decode()

    def _sudo_cat(self, ssh, path: str, sudo_password: str) -> Optional[bytes]:
        cmd = f"sudo -S cat '{path}'"
        stdin, stdout, _ = ssh.exec_command(cmd)
        if sudo_password:
            stdin.write(sudo_password + "\n")
            stdin.flush()
        data = stdout.read()
        return data if data else None


class SambaManager:
    def __init__(self, config: dict, log) -> None:
        self.config = config
        self.log = log
        self.sudo_password = config.get("sudo_password", "")

    def run(self) -> tuple[str, str]:
        if not self.config.get("samba_enabled", True):
            return "failed", "Samba is disabled"

        if not self._check_installed():
            return "failed", "Samba is not installed"

        if not self._write_config():
            return "failed", "Failed to write smb.conf"

        if not self._test_config():
            return "failed", "smb.conf validation failed"

        self._manage_users()

        if not self._restart_service():
            return "failed", "Failed to restart Samba"

        return "success", "Samba configured and started"

    def _check_installed(self) -> bool:
        if shutil.which("smbd"):
            return True
        self.log("smbd not found in PATH")
        return False

    def _write_config(self) -> bool:
        conf = self._generate_config()
        tmp_path = "/tmp/smb.conf.new"
        try:
            with open(tmp_path, "w", encoding="utf-8") as handle:
                handle.write(conf)
        except Exception as exc:
            self.log(f"Failed to write temp config: {exc}")
            return False

        result = subprocess.run(
            ["sudo", "-S", "mv", tmp_path, "/etc/samba/smb.conf"],
            input=f"{self.sudo_password}\n" if self.sudo_password else None,
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            self.log(f"Failed to update smb.conf: {result.stderr.strip()}")
            return False
        return True

    def _generate_config(self) -> str:
        force_user = self.config.get("force_user") or ""
        force_group = self.config.get("force_group") or ""
        create_mask = self.config.get("create_mask", "0755")
        directory_mask = self.config.get("directory_mask", "0755")

        lines = [
            "[global]",
            f"   workgroup = {self.config.get('samba_workgroup', 'WORKGROUP')}",
            f"   server string = {self.config.get('samba_description', 'Backup Manager')}",
            f"   netbios name = {self.config.get('samba_server_name', 'BACKUP-SERVER')}",
            "   server role = standalone server",
            "   security = user",
            "   map to guest = never",
            "   load printers = no",
            "",
        ]

        for share in self.config.get("shares", []):
            if not share.get("enabled", True):
                continue
            lines.append(f"[{share.get('name', 'backup')}]")
            lines.append(f"   path = {share.get('path', '/srv/backups')}")
            comment = share.get("comment", "")
            if comment:
                lines.append(f"   comment = {comment}")
            lines.append(f"   read only = {'yes' if share.get('read_only', True) else 'no'}")
            lines.append("   browseable = yes")
            lines.append(f"   guest ok = {'yes' if share.get('guest_ok', False) else 'no'}")
            valid_users = share.get("valid_users", [])
            if valid_users:
                lines.append(f"   valid users = {' '.join(valid_users)}")
            if force_user:
                lines.append(f"   force user = {force_user}")
            if force_group:
                lines.append(f"   force group = {force_group}")
            lines.append(f"   create mask = {create_mask}")
            lines.append(f"   directory mask = {directory_mask}")
            lines.append("   follow symlinks = yes")
            lines.append("   wide links = yes")
            lines.append("")

        return "\n".join(lines)

    def _test_config(self) -> bool:
        if not shutil.which("testparm"):
            return True
        result = subprocess.run(["testparm", "-s"], capture_output=True, text=True)
        if result.returncode != 0:
            self.log(f"testparm error: {result.stderr.strip()}")
            return False
        return True

    def _manage_users(self) -> None:
        for user in self.config.get("samba_users", []):
            if not user.get("enabled", True):
                continue
            username = user.get("username")
            password = user.get("password")
            if not username or not password:
                continue
            if not self._system_user_exists(username):
                self.log(f"System user missing: {username}")
                continue
            cmd = ["sudo", "-S", "smbpasswd", "-a", "-s", username]
            process = subprocess.Popen(
                cmd,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            sudo_prefix = f"{self.sudo_password}\n" if self.sudo_password else ""
            stdout, stderr = process.communicate(input=f"{sudo_prefix}{password}\n{password}\n", timeout=10)
            if process.returncode != 0:
                self.log(f"smbpasswd failed for {username}: {stderr.strip() or stdout.strip()}")

    def _system_user_exists(self, username: str) -> bool:
        result = subprocess.run(["id", "-u", username], capture_output=True)
        return result.returncode == 0

    def _restart_service(self) -> bool:
        service = self._find_service()
        if service:
            result = subprocess.run(
                ["sudo", "-S", "systemctl", "restart", service],
                input=f"{self.sudo_password}\n" if self.sudo_password else None,
                capture_output=True,
                text=True,
            )
            if result.returncode != 0:
                stderr = result.stderr.strip()
                stdout = result.stdout.strip()
                self.log(f"systemctl restart {service} failed: {stderr or stdout}")
            else:
                return True

        result = subprocess.run(
            ["sudo", "-S", "pkill", "-f", "^smbd"],
            input=f"{self.sudo_password}\n" if self.sudo_password else None,
            capture_output=True,
        )
        _ = result
        subprocess.run(
            ["sudo", "-S", "pkill", "-f", "^nmbd"],
            input=f"{self.sudo_password}\n" if self.sudo_password else None,
            capture_output=True,
        )
        subprocess.run(
            ["sudo", "-S", "smbd", "-D"],
            input=f"{self.sudo_password}\n" if self.sudo_password else None,
            capture_output=True,
        )
        subprocess.run(
            ["sudo", "-S", "nmbd", "-D"],
            input=f"{self.sudo_password}\n" if self.sudo_password else None,
            capture_output=True,
        )
        return True

    def _find_service(self) -> Optional[str]:
        for name in ("smb", "smbd", "nmbd"):
            result = subprocess.run(
                ["systemctl", "list-unit-files", f"{name}.service"],
                capture_output=True,
                text=True,
            )
            if result.returncode == 0 and name in result.stdout:
                return f"{name}.service"
        return None
