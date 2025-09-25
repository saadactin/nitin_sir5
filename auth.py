import bcrypt
from flask import session, redirect, url_for, flash
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
        
        if result:
            print(f"User {username} created successfully")  # Simple log without emojis
            return True
        else:
            print(f"User {username} already exists")  # Simple log without emojis
            return False
            
    except Exception as e:
        print(f"Error creating user: {str(e)}")  # Simple error logging
        return False
def init_admin_user():
    """Create default admin if not exists"""
    conn = get_pg_connection()
    cur = conn.cursor()
    cur.execute("SELECT id FROM metrics_sync_tables.users WHERE username = 'admin';")
    if not cur.fetchone():
        create_user("admin", "admin123", "admin")
        print("✅ Default admin created (admin / admin123)")
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
    session["user"] = username
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
                flash("❌ Access denied!", "danger")
                return redirect(url_for("index"))
            return fn(*args, **kwargs)
        wrapped.__name__ = fn.__name__
        return wrapped
    return wrapper

# ------------------ AUTO CREATE DEFAULT ADMIN ------------------
init_admin_user()
