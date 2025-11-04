# HANA Environment Variables Setup

## ✅ Project Now Uses ONLY .env Variables for HANA

The entire project has been updated to use **ONLY** the 4 HANA environment variables from your `.env` file. There are **NO hardcoded values** for HANA connections.

## Required Environment Variables

Add these 4 variables to your `.env` file when you have a HANA connection:

```bash
HANA_HOST=192.168.16.62
HANA_PORT=30015
HANA_USERNAME=your_hana_username
HANA_PASSWORD=your_hana_password
```

## Files Updated

### 1. `db_utils.py` ✅
- Added `load_hana_config()` function
- **REQUIRES** all 4 environment variables
- **NO hardcoded fallbacks** (removed `localhost`, `30015`, etc.)
- Raises `ValueError` with clear message if any variable is missing
- Automatically loads `.env` file using `dotenv`

### 2. `hana_sync.py` ✅
- Updated `connect_hana()` to use `load_hana_config()` from `.env`
- Removed hardcoded default port `30015`
- Falls back to `.env` if config is incomplete

### 3. `app.py` ✅
- All HANA connection routes now use `load_hana_config()`
- Removed hardcoded `os.getenv()` with defaults
- Removed hardcoded port `30015`

### 4. `scheduler_utils.py` ✅
- Updated scheduled HANA syncs to use `load_hana_config()`
- Removed hardcoded values

## How It Works

1. **All HANA connections** call `db_utils.load_hana_config()`
2. **Function loads `.env`** using `python-dotenv`
3. **Validates** that all 4 required variables are present
4. **Returns** config dict: `{'host': ..., 'port': ..., 'username': ..., 'password': ...}`
5. **NO fallbacks** - missing variables raise clear errors

## Error Messages

If you forget to set a variable, you'll get clear errors:

```
ValueError: HANA_HOST environment variable is required.
Please set it in your .env file: HANA_HOST=your_host
```

## Backward Compatibility

- If HANA env vars are **not set**, the code will try to use source-specific credentials from the database
- This allows existing sources to continue working
- But **new connections** will require `.env` variables

## Testing

Test your configuration:

```bash
python test_hana_env_config.py
```

## Verification Checklist

✅ `db_utils.load_hana_config()` requires all 4 env vars  
✅ No hardcoded `localhost`, `30015` anywhere  
✅ All HANA connections use `load_hana_config()`  
✅ Clear error messages if variables are missing  
✅ `.env` file is the single source of truth  

## Summary

🎯 **Single Source of Truth**: `.env` file only  
🚫 **No Hardcoded Values**: All removed  
✅ **Clear Errors**: Missing vars raise descriptive exceptions  
🔒 **Safe**: Can't accidentally use wrong connection settings  

## When You Get HANA Connection

1. Add the 4 HANA variables to your `.env` file
2. Restart your Flask app
3. All HANA connections will automatically use these values
4. No code changes needed!

---

**Set these 4 variables in `.env` when you have a HANA connection - everything else is automatic!**

