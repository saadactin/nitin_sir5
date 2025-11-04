"""
Add HANA Docker settings to .env file
Uses YourPassword123 as specified
"""
import os

def setup_hana_env():
    """Add or update HANA settings in .env file"""
    env_file = '.env'
    
    # HANA Docker settings
    hana_settings = {
        'HANA_HOST': 'localhost',
        'HANA_PORT': '39017',
        'HANA_USERNAME': 'SYSTEM',
        'HANA_PASSWORD': 'YourPassword123'
    }
    
    # Read existing .env if it exists
    existing_lines = []
    existing_keys = set()
    
    if os.path.exists(env_file):
        with open(env_file, 'r', encoding='utf-8') as f:
            existing_lines = f.readlines()
            # Get existing keys
            for line in existing_lines:
                if '=' in line and not line.strip().startswith('#'):
                    key = line.split('=')[0].strip()
                    existing_keys.add(key)
    
    # Update or add HANA settings
    updated_lines = []
    for line in existing_lines:
        # Check if this line is a HANA setting we need to update
        should_keep = True
        for key in hana_settings.keys():
            if line.strip().startswith(f'{key}='):
                # Replace existing line
                updated_lines.append(f'{key}={hana_settings[key]}\n')
                should_keep = False
                break
        
        if should_keep:
            updated_lines.append(line)
    
    # Add any missing HANA settings
    for key, value in hana_settings.items():
        if key not in existing_keys:
            updated_lines.append(f'{key}={value}\n')
    
    # Write back to .env
    with open(env_file, 'w', encoding='utf-8') as f:
        f.writelines(updated_lines)
    
    print("=" * 70)
    print("[OK] HANA Settings Added to .env")
    print("=" * 70)
    print("\nAdded/Updated settings:")
    for key, value in hana_settings.items():
        display_value = value if 'PASSWORD' not in key else '***'
        print(f"   {key}={display_value}")
    print("\nFile: .env")
    print("\n[OK] Ready to use!")
    print("\nNext steps:")
    print("1. Start HANA: docker-compose up -d")
    print("2. Wait for initialization (5-10 min)")
    print("3. Run: python setup_hana_sample_db.py")

if __name__ == '__main__':
    setup_hana_env()

