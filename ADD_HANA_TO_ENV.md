# Adding HANA Configuration to .env File

## Quick Guide

To use the HANA to ClickHouse migration test script, you need to **ADD** the following variables to your existing `.env` file.

## ✅ What to Add

Simply **ADD** these 4 lines to your existing `.env` file (don't remove or change any existing variables):

```env
# SAP HANA Configuration
HANA_HOST=192.168.16.62
HANA_PORT=30015
HANA_USERNAME=your_hana_username
HANA_PASSWORD=your_hana_password
```

## 📝 Example

If your `.env` file currently looks like this:

```env
# PostgreSQL Configuration (EXISTING - DON'T CHANGE)
PG_HOST=localhost
PG_PORT=5432
PG_DATABASE=test1
PG_USERNAME=postgres
PG_PASSWORD=your_pg_password

# Flask Configuration (EXISTING - DON'T CHANGE)
SECRET_KEY=your-secret-key
FLASK_DEBUG=0
APP_HOST=127.0.0.1
APP_PORT=5000

# ClickHouse Configuration (EXISTING - DON'T CHANGE)
CLICKHOUSE_HOST=localhost
CLICKHOUSE_PORT=9000
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=
```

Just **ADD** the HANA section at the end:

```env
# PostgreSQL Configuration (EXISTING - DON'T CHANGE)
PG_HOST=localhost
PG_PORT=5432
PG_DATABASE=test1
PG_USERNAME=postgres
PG_PASSWORD=your_pg_password

# Flask Configuration (EXISTING - DON'T CHANGE)
SECRET_KEY=your-secret-key
FLASK_DEBUG=0
APP_HOST=127.0.0.1
APP_PORT=5000

# ClickHouse Configuration (EXISTING - DON'T CHANGE)
CLICKHOUSE_HOST=localhost
CLICKHOUSE_PORT=9000
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=

# ==========================================
# SAP HANA Configuration (NEW - ADD THIS)
# ==========================================
HANA_HOST=192.168.16.62
HANA_PORT=30015
HANA_USERNAME=your_hana_username
HANA_PASSWORD=your_hana_password
```

## 🔄 Changing HANA Credentials

When you have a new HANA database with different IP, port, username, or password, simply update these 4 variables in your `.env` file:

```env
HANA_HOST=new_ip_address      # Change IP here
HANA_PORT=new_port            # Change port here (default: 30015)
HANA_USERNAME=new_username     # Change username here
HANA_PASSWORD=new_password     # Change password here
```

**That's it!** No code changes needed. The test script will automatically use the new values.

## ✅ Verification

After adding the variables, verify they're loaded correctly:

```bash
# On Windows PowerShell:
python -c "import os; from dotenv import load_dotenv; load_dotenv(); print('HANA_HOST:', os.getenv('HANA_HOST')); print('HANA_PORT:', os.getenv('HANA_PORT'))"

# On Linux/Mac:
python -c "import os; from dotenv import load_dotenv; load_dotenv(); print('HANA_HOST:', os.getenv('HANA_HOST')); print('HANA_PORT:', os.getenv('HANA_PORT'))"
```

## 📌 Important Notes

1. **Only add these variables** - Don't remove or modify existing `.env` variables
2. **Use actual credentials** - Replace `your_hana_username` and `your_hana_password` with real values
3. **IP address format** - Use IP like `192.168.16.62` or hostname like `hana-server.company.com`
4. **Port default** - If port is not specified, default `30015` will be used
5. **Security** - Never commit `.env` file to version control

## 🚀 Testing

Once added, run the test script:

```bash
python test_hana_to_clickhouse_migration.py
```

The script will automatically read HANA credentials from your `.env` file.

