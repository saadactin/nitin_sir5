"""
Environment Variable Validator
Validates required environment variables on application startup
"""

import os
import logging

logger = logging.getLogger(__name__)

# Required environment variables for production
REQUIRED_ENV_VARS = {
    'SECRET_KEY': 'Secret key for Flask session encryption',
    'PG_PASSWORD': 'PostgreSQL database password',
    'CLICKHOUSE_PASSWORD': 'ClickHouse database password',
}

# Optional but recommended environment variables
RECOMMENDED_ENV_VARS = {
    'SMTP_SERVER': 'SMTP server for email notifications',
    'SMTP_USER': 'SMTP username for email notifications',
    'SMTP_PASSWORD': 'SMTP password for email notifications',
    'ADMIN_EMAILS': 'Comma-separated list of admin email addresses',
    'HANA_PASSWORD': 'SAP HANA database password (if using HANA)',
    'HANA_HOST': 'SAP HANA host address (if using HANA)',
    'HANA_PORT': 'SAP HANA port (if using HANA)',
    'HANA_USERNAME': 'SAP HANA username (if using HANA)',
}

# Optional environment variables with defaults
OPTIONAL_ENV_VARS = {
    'FLASK_ENV': 'Flask environment (development/production)',
    'APP_HOST': 'Application host address',
    'APP_PORT': 'Application port',
    'PG_HOST': 'PostgreSQL host (default: localhost)',
    'PG_PORT': 'PostgreSQL port (default: 5432)',
    'PG_DATABASE': 'PostgreSQL database name',
    'PG_USERNAME': 'PostgreSQL username',
    'CLICKHOUSE_HOST': 'ClickHouse host (default: localhost)',
    'CLICKHOUSE_PORT': 'ClickHouse port (default: 9000)',
    'CLICKHOUSE_USER': 'ClickHouse username',
    'SESSION_COOKIE_SECURE': 'Use secure cookies (0/1)',
    'SESSION_COOKIE_SAMESITE': 'SameSite cookie setting',
    'PERMANENT_SESSION_LIFETIME': 'Session lifetime in seconds',
}


def validate_environment(production_mode: bool = False) -> tuple[bool, list[str]]:
    """
    Validate environment variables.
    
    Args:
        production_mode: If True, require all mandatory variables. If False, only warn.
        
    Returns:
        tuple: (is_valid, list_of_missing_vars)
    """
    missing_vars = []
    warnings = []
    
    # Check required variables
    for var_name, description in REQUIRED_ENV_VARS.items():
        value = os.environ.get(var_name)
        if not value:
            missing_vars.append(var_name)
            if production_mode:
                logger.error(f"[REQUIRED] Environment variable missing: {var_name} - {description}")
            else:
                logger.warning(f"[WARNING] Environment variable not set: {var_name} - {description}")
        else:
            logger.debug(f"[OK] {var_name} is set")
    
    # Check recommended variables (warn only)
    for var_name, description in RECOMMENDED_ENV_VARS.items():
        value = os.environ.get(var_name)
        if not value:
            warnings.append(f"Recommended variable not set: {var_name} - {description}")
            logger.warning(f"[RECOMMENDED] Environment variable not set: {var_name} - {description}")
    
    # Summary
    if missing_vars:
        if production_mode:
            logger.error("=" * 80)
            logger.error("CRITICAL: Missing required environment variables!")
            logger.error("=" * 80)
            for var in missing_vars:
                logger.error(f"  - {var}: {REQUIRED_ENV_VARS[var]}")
            logger.error("=" * 80)
            logger.error("Application cannot start in production mode without these variables.")
            logger.error("Set them in your .env file or environment variables.")
            logger.error("=" * 80)
            return False, missing_vars
        else:
            logger.warning("=" * 80)
            logger.warning("WARNING: Some required environment variables are missing.")
            logger.warning("=" * 80)
            for var in missing_vars:
                logger.warning(f"  - {var}: {REQUIRED_ENV_VARS[var]}")
            logger.warning("=" * 80)
            logger.warning("Application will start but may not function correctly.")
            logger.warning("Set them in your .env file for production deployment.")
            logger.warning("=" * 80)
    
    if not missing_vars and not warnings:
        logger.info("✓ All required environment variables are set")
    
    return len(missing_vars) == 0, missing_vars


def get_environment_summary() -> dict:
    """Get summary of environment variable configuration"""
    summary = {
        'required': {},
        'recommended': {},
        'optional': {},
        'missing_required': [],
        'missing_recommended': [],
    }
    
    for var_name in REQUIRED_ENV_VARS:
        value = os.environ.get(var_name)
        if value:
            summary['required'][var_name] = '***' if 'PASSWORD' in var_name or 'SECRET' in var_name else value
        else:
            summary['missing_required'].append(var_name)
    
    for var_name in RECOMMENDED_ENV_VARS:
        value = os.environ.get(var_name)
        if value:
            summary['recommended'][var_name] = '***' if 'PASSWORD' in var_name else value
        else:
            summary['missing_recommended'].append(var_name)
    
    for var_name in OPTIONAL_ENV_VARS:
        value = os.environ.get(var_name)
        if value:
            summary['optional'][var_name] = '***' if 'PASSWORD' in var_name else value
    
    return summary


if __name__ == '__main__':
    # Test the validator
    logging.basicConfig(level=logging.INFO)
    is_valid, missing = validate_environment(production_mode=False)
    print(f"\nValidation result: {'PASS' if is_valid else 'FAIL'}")
    if missing:
        print(f"Missing variables: {', '.join(missing)}")
    
    print("\nEnvironment Summary:")
    summary = get_environment_summary()
    print(f"Required (set): {len(summary['required'])}/{len(REQUIRED_ENV_VARS)}")
    print(f"Recommended (set): {len(summary['recommended'])}/{len(RECOMMENDED_ENV_VARS)}")
    print(f"Optional (set): {len(summary['optional'])}/{len(OPTIONAL_ENV_VARS)}")

