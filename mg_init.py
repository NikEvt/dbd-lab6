from pymongo import MongoClient, ASCENDING, DESCENDING
from datetime import datetime
from bson import ObjectId
from dotenv import load_dotenv
import os

load_dotenv()

client = MongoClient(os.getenv('MONGO_URL'))
db = client["telecom_db"]


users = db["users"]
internettariffs = db["internettariffs"]
mobiletariffs = db["mobiletariffs"]
tvtariffs = db["tvtariffs"]
services = db["services"]
usersubscriptions = db["usersubscriptions"]
userservices = db["userservices"]
operations = db["operations"]

users.drop()
internettariffs.drop()
mobiletariffs.drop()
tvtariffs.drop()
services.drop()
usersubscriptions.drop()
userservices.drop()
operations.drop()


user_id = ObjectId()
users.insert_one({
    "_id": user_id,
    "fullname": "Иванов Иван",
    "email": "ivanov@example.com",
    "phonenumber": "+79990001111",
    "address": "Москва, Ленина 1",
    "registrationdate": datetime(2023, 1, 10)
})


inet_id = ObjectId()
internettariffs.insert_one({
    "_id": inet_id,
    "tariffname": "Интернет 100",
    "monthlycost": 500.00,
    "speed": 100,
    "datacap": 500
})

mob_id = ObjectId()
mobiletariffs.insert_one({
    "_id": mob_id,
    "tariffname": "Мобильный Старт",
    "monthlycost": 300.00,
    "datalimit": 10,
    "callminutes": 500,
    "smslimit": 100
})

tv_id = ObjectId()
tvtariffs.insert_one({
    "_id": tv_id,
    "tariffname": "ТВ Базовый",
    "monthlycost": 400.00,
    "channelcount": 100
})

# Услуга
service_id = ObjectId()
services.insert_one({
    "_id": service_id,
    "servicename": "Netflix",
    "servicetype": "Streaming",
    "monthlycost": 800.00
})

subscription_id = ObjectId()
usersubscriptions.insert_one({
    "_id": subscription_id,
    "userid": user_id,
    "mobiletariffid": mob_id,
    "internettariffid": inet_id,
    "tvtariffid": tv_id,
    "startdate": datetime(2023, 6, 1),
    "enddate": datetime(2024, 6, 1)
})

userservices.insert_one({
    "userid": user_id,
    "serviceid": service_id,
    "activationdate": datetime(2023, 7, 1)
})


operations.insert_one({
    "userid": user_id,
    "subscriptionid": subscription_id,
    "serviceid": service_id,
    "operationtype": "Оплата",
    "amount": 1700.00,
    "operationdate": datetime(2024, 1, 1)
})

operations.create_index([("userid", ASCENDING), ("operationdate", DESCENDING)])

db.command({
    "create": "user_operations",
    "viewOn": "operations",
    "pipeline": [
        {
            "$lookup": {
                "from": "users",
                "localField": "userid",
                "foreignField": "_id",
                "as": "user"
            }
        },
        { "$unwind": "$user" },
        {
            "$project": {
                "operationtype": 1,
                "amount": 1,
                "operationdate": 1,
                "user.fullname": 1,
                "user.email": 1
            }
        }
    ]
})

print("База данных успешно загружена в MongoDB Atlas")