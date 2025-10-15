import os
import sys

# ensure project root on path
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# load dotenv if available to pick up .env values
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

from auth import init_admin_user


def main():
    try:
        pw = os.environ.get('DEFAULT_ADMIN_PASSWORD', 'admin123')
        print('Creating default admin (if missing) with password from DEFAULT_ADMIN_PASSWORD')
        init_admin_user(create_if_missing=True, default_password=pw)
        print('Done. Check DB for admin row.')
    except Exception as e:
        print('ERROR:', repr(e))

if __name__ == '__main__':
    main()
