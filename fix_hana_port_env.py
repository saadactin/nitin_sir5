"""Update HANA_PORT in .env to 39013"""
import os
import re

env_file = '.env'

if not os.path.exists(env_file):
    print(f"ERROR: {env_file} file not found!")
    exit(1)

# Read current .env
with open(env_file, 'r', encoding='utf-8') as f:
    lines = f.readlines()

# Update HANA_PORT
updated_lines = []
for line in lines:
    if line.strip().startswith('HANA_PORT='):
        updated_lines.append('HANA_PORT=39013\n')
        print(f"Updated: HANA_PORT=39013")
    else:
        updated_lines.append(line)

# Write back
with open(env_file, 'w', encoding='utf-8') as f:
    f.writelines(updated_lines)

print("\n[OK] .env updated to use port 39013")
print("Now you can leave form fields empty and use .env values!")

