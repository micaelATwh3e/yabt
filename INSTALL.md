# YATB - Yet Another Backup Tool - Setup & Installation

## Quick Start

```bash
cd /path/to/yatb
./start.sh
```

Open http://localhost:5000 and log in with default credentials:
- Username: `admin`
- Password: `admin`

**⚠️ Change password immediately after first login.**

## Database Initialization

The database is initialized automatically on first run. A new SQLite database and default settings are created in `data/backup_manager.sqlite`.

## Configuration

All configuration is stored in SQLite:

### Backup Profiles
- Local file backup with scheduling, retention, and verification
- Accessible via Dashboard → Profiles

### SSH Backups
- Remote SSH/SFTP backups via paramiko
- Pre-commands support (e.g., `mysqldump` before backup)
- Configure via SSH page or edit JSON directly
- Can be scheduled daily

### Samba Sharing
- Export backup directories via SMB/CIFS
- Requires `sudo` to update `/etc/samba/smb.conf`
- Configure via Samba page

### Multi-user & Roles
- Admin: manage users and schedules
- Operator: run and manage backups
- Accessible via Users page (admin-only)

## Docker (Optional)

To run in Docker:

```dockerfile
FROM python:3.13-slim

WORKDIR /app
COPY . .

RUN chmod +x start.sh
RUN apt-get update && apt-get install -y sudo samba && rm -rf /var/lib/apt/lists/*

EXPOSE 5000

CMD ["./start.sh"]
```

## Requirements

- Python 3.10+
- Flask 3.0+
- paramiko 3.4+ (for SSH backups)
- smbpasswd/smbd (optional, for Samba sharing)
- At least 200 MB disk space for the app and logs

## Backup Paths

- Local backups: stored under destination paths (configurable per profile)
- SSH backups: stored in `ssh_config.local_backup_dir` (default: `/tmp/ssh_backups`)
- Logs: `/tmp/*.log` or as configured

## Troubleshooting

### SSH connection fails
- Ensure SSH key is readable and correct path is set
- Check `System Runs` → SSH run detail for detailed error logs
- Verify `pre_commands` syntax if enabled

### Samba won't start
- Check that `sudo` password is set in Samba config
- Review system `smb.conf` syntax: `testparm -s`
- Check `System Runs` logs for detailed errors

### Profiles don't schedule
- Ensure **Scheduler: On** toggle is active on dashboard
- Verify schedule time is set in the profile
- Check system timezone matches intended schedule time

## Performance

- Backup queue runs one profile at a time
- SSH backups with compression can be slow; consider using per-server timeout settings
- For large backups, increase SSH command timeouts in pre_commands

## Security Notes

⚠️ **Before deploying to production:**
- Change admin password
- Use HTTPS with a reverse proxy (nginx/traefik)
- Restrict network access to the app port
- Store SSH keys securely and use key-based auth
- Use strong Samba user passwords
- Enable firewall rules to allow only trusted IPs
