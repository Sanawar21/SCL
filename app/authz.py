from functools import wraps

from flask import abort, session


def login_required(role=None):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            if "user" not in session:
                return abort(401)
            if role and session["user"].get("role") != role:
                return abort(403)
            return func(*args, **kwargs)

        return wrapper

    return decorator
