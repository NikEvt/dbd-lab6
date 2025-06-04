import json
from pymongo import MongoClient
from datetime import datetime
from dotenv import load_dotenv
import os

load_dotenv()

client = MongoClient(os.getenv('MONGO_URL'))
db = client["telecom_db"]

def import_json(collection_name, filename):
    with open(filename, "r", encoding="utf-8") as file:
        data = json.load(file)

        for doc in data:
            if "registrationdate" in doc:
                doc["registrationdate"] = datetime.strptime(doc["registrationdate"], "%Y-%m-%d")

        db[collection_name].insert_many(data)
        print(f"Импортировано в '{collection_name}': {len(data)} документов")

import_json("users", "users.json")
import_json("internettariffs", "internettariffs.json")
import_json("mobiletariffs", "mobiletariffs.json")
import_json("tvtariffs", "tvtariffs.json")