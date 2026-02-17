"""YATB - Yet Another Backup Tool - Authentication module.

Password hashing and login decorators.
"""
from __future__ import annotations

from functools import wraps
from typing import Callable

from flask import session, redirect, request, url_for, jsonify
from werkzeug.security import generate_password_hash, check_password_hash


def hash_password(password: str) -> str:
    return generate_password_hash(password, method="pbkdf2:sha256", salt_length=16)


def verify_password(password: str, password_hash: str) -> bool:
    return check_password_hash(password_hash, password)


def login_required(view: Callable):
    @wraps(view)
    def wrapped(*args, **kwargs):
        if not session.get("user_id"):
            if request.path.startswith("/api/"):
                return jsonify({"success": False, "message": "Login required"}), 401
            return redirect(url_for("login"))
        return view(*args, **kwargs)

    return wrapped


def require_role(role: str):
    def decorator(view: Callable):
        @wraps(view)
        def wrapped(*args, **kwargs):
            if session.get("role") != role:
                if request.path.startswith("/api/"):
                    return jsonify({"success": False, "message": "Forbidden"}), 403
                return redirect(url_for("index"))
            return view(*args, **kwargs)

        return wrapped

    return decorator
