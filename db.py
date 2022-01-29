from pymongo import MongoClient
import db_settings

client = MongoClient(db_settings.MONGO_LINK)
db = client[db_settings.MONGO_DB]

def get_or_create_user(db, effective_user, chat_id):
    user = db.users.find_one({"user_id": effective_user.id})
    if not user:
        user = {
            "user_id": effective_user.id,
            "first_name": effective_user.first_name,
            "last_name": effective_user.last_name,
            "username": effective_user.username,
            "chat_id": chat_id,
            "collect_companies": False,
            "companies_list": [],
            "tic_list": [],
            "budget": '',
            "data": None,
            'cleaned_weights': {},
            "ef": {},
        }
        db.users.insert_one(user)
    return user
