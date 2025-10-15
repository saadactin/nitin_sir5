import bcrypt
import os
from flask import session, redirect, url_for, flash
import logging
from db_utils import get_pg_connection, init_pg_schema

# Ensure schema is ready
init_pg_schema()


def create_user(username, password, role):
    """Create a new user with hashed password"""
    try:
        conn = get_pg_connection()
        cur = conn.cursor()

        hashed_pw = bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()

        cur.execute("""
            INSERT INTO metrics_sync_tables.users (username, password, role)
            VALUES (%s, %s, %s)
            ON CONFLICT (username) DO NOTHING
            RETURNING id
        """, (username, hashed_pw, role))

        result = cur.fetchone()
        conn.commit()
        cur.close()
        conn.close()
        
        # Simply return True/False without printing
        return result is not None
            
    except Exception as e:
        # Remove the error print or keep it for debugging (your choice)
        # print(f"Error creating user: {str(e)}")
        return False

def init_admin_user(create_if_missing=False, default_password=None):
    """Create default admin if not exists.

    Use create_if_missing=True to enable creation. This prevents automatic
    creation during import which can lead to insecure defaults.
    """
    if not create_if_missing:
        return

    conn = get_pg_connection()
    cur = conn.cursor()
    cur.execute("SELECT id FROM metrics_sync_tables.users WHERE username = 'admin';")
    if not cur.fetchone():
        pw = default_password or os.environ.get('DEFAULT_ADMIN_PASSWORD', 'admin123')
        create_user("admin", pw, "admin")
        # SECURITY: Never log the actual password
        logging.info(f"Default admin created (username: admin) - password set from environment or default")
    cur.close()
    conn.close()

def authenticate_user(username, password):
    """Check username + password, return role if valid"""
    conn = get_pg_connection()
    cur = conn.cursor()
    cur.execute("SELECT password, role FROM metrics_sync_tables.users WHERE username = %s", (username,))
    row = cur.fetchone()
    cur.close()
    conn.close()

    if row:
        stored_hash, role = row
        if bcrypt.checkpw(password.encode(), stored_hash.encode()):
            return role
    return None

def login_user(username, role):
    """Save login state in session"""
    # Set both keys for compatibility with different parts of the app
    session["user"] = username
    session["username"] = username
    session["role"] = role

def logout_user():
    """Clear session"""
    session.pop("user", None)
    session.pop("role", None)

def require_role(allowed_roles):
    """Decorator for route protection"""
    def wrapper(fn):
        def wrapped(*args, **kwargs):
            if "role" not in session or session["role"] not in allowed_roles:
                flash("Please log in to access this page", "warning")
                return redirect(url_for("login"))
            return fn(*args, **kwargs)
        wrapped.__name__ = fn.__name__
        return wrapped
    return wrapper

# ------------------ AUTO CREATE DEFAULT ADMIN ------------------
# Do not auto-create admin on import. To create default admin at startup,
# call init_admin_user(create_if_missing=True) from the application entrypoint
# or set CREATE_DEFAULT_ADMIN=1 and call it conditionally during startup.
