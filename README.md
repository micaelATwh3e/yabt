# YATB - Yet Another Backup Tool

A single-page Flask + SQLite backup manager for reliable local and remote backups with scheduling, retention, and verification.

**Features:**
- 🔐 Multi-user login (admin and operator roles)
- 📦 Local backups with scheduling, retention, and hash/size verification
- 🌐 SSH/SFTP remote backups via paramiko
- 🗂️ Samba SMB share management for backup access
- ⏰ Daily scheduler for automated runs
- 📊 Real-time backup queue and run history
- 💾 All config stored in SQLite (no config files)

## Quick Start

```bash
./start.sh
```

Open http://localhost:5000 and log in with:
- Username: `admin`
- Password: `admin`

⚠️ Change password immediately after first login.

## Installation

For detailed setup and configuration, see [INSTALL.md](INSTALL.md).

Requirements:
- Python 3.10+
- Flask 3.0+
- paramiko 3.4+ (for SSH backups)
- Optional: Samba tools (for SMB sharing)

## Architecture

- **App**: Single Flask application in `app.py`
- **Database**: SQLite in `data/backup_manager.sqlite` (auto-initialized)
- **Backup logic**: Queue-based runner with thread pool in `backup.py`
- **Scheduler**: Separate thread checking time every 30 seconds
- **UI**: Jinja2 templates + vanilla JS

## Configuration

All settings are managed via the web interface and stored in SQLite:

- **Profiles**: Local backup source→destination with scheduling, retention, verification
- **SSH**: Remote servers with pre-commands, compression, and daily schedule
- **Samba**: SMB share export for backup access
- **Users**: Admin and operator accounts

## SSH Pre-commands

Run commands before backup (e.g., database dumps):

```json
"pre_commands": [
  { "command": "mysqldump -u root db > /tmp/db.sql", "use_sudo": false, "timeout": 300 }
]
```

## Development

Debug mode:
```bash
FLASK_DEBUG=1 python app.py
```

Tests (add as needed):
```bash
pytest
```

## License

MIT
