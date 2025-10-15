import sys
sys.path.append(r"c:\Users\SaadSayyed\Desktop\portless\nitin_sir5\nitin_sir5")
try:
    import db_utils
    print('db_utils imported successfully')
except Exception as e:
    print('Import error:', type(e).__name__, e)
