import sys
sys.path.append('e:/fyp/SPTS/backend')
from auth import get_password_hash
from db_users import create_user
try:
    pwd = get_password_hash("test")
    print("Hash ok")
    create_user("testuser", pwd)
    print("DB ok")
except Exception as e:
    import traceback
    traceback.print_exc()

