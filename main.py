from pymongo import MongoClient
from bson import ObjectId
from datetime import datetime

# connection to mongodb
client = MongoClient('mongodb://localhost:27017/')
db = client['shop']

# task 1: modelling

# 1. add a few items
items = [
    {"category": "Phone", "model": "iPhone 6", "producer": "Apple", "price": 600},
    {"category": "TV", "model": "Samsung QLED", "producer": "Samsung", "price": 1200},
    {"category": "Smart Watch", "model": "Apple Watch", "producer": "Apple", "price": 400},
    {"category": "Phone", "model": "Xiaomi T11", "producer": "Xiaomi", "price": 400},
    {"category": "Laptop", "model": "MacBook Air", "producer": "Apple", "price": 1000},
    {"category": "Headphones", "model": "Marshall Major IV", "producer": "Marshall", "price": 250},
    {"category": "Smart Watch", "model": "Samsung Galaxy Fit 3", "producer": "Samsung", "price": 70}
]
# db.items.insert_many(items)

for item in items:
    db.items.update_one(
        {"category": item["category"], "model": item["model"]},
        {"$set": item},
        upsert=True
    )

# 2. get all items
print("all items are:")
for item in db.items.find():
    print(item)

# 3. get number of items of some exact category
phone_count = db.items.count_documents({"category": "Phone"})
print(f"a number of items in Phone category: {phone_count}")

# 4. get a number of categories
category_count = len(db.items.distinct("category"))
print(f"a number of all categories: {category_count}")

# 5. a list of all producers
producers = db.items.distinct("producer")
print(f"a list of all producers: {producers}")

# 6. find some items by different criteria

# a) category and price in between a and b
phones_in_range = db.items.find({"category": "Phone", "price": {"$gte": 500, "$lte": 700}})
print("items of category Phone with price in between 500 and 700:")
for item in phones_in_range:
    print(item)

# b) model one or another
specific_models = db.items.find({"$or": [{"model": "iPhone 6"}, {"model": "Samsung QLED"}]})
print("items of models iPhone 6 or Samsung QLED:")
for item in specific_models:
    print(item)

# c) producers
specific_producers = db.items.find({"producer": {"$in": ["Apple", "Samsung"]}})
print("items of producers Apple or Samsung:")
for item in specific_producers:
    print(item)

# 7. updating some items, altering the existing values, adding new characteristics
db.items.update_many({"category": "Phone"}, {"$set": {"warranty": "2 years"}, "$inc": {"price": 50}})

# 8. get items that have some exact characteristic
warranty_items = db.items.find({"warranty": {"$exists": True}})
print("items that have warranty:")
for item in warranty_items:
    print(item)

# 9. increasing price
db.items.update_many({"warranty": {"$exists": True}}, {"$inc": {"price": 20}})

# task 2: orders

# 1. create a few orders
orders = [
    {
        "order_number": 201513,
        "date": datetime(2023, 1, 15),
        "total_sum": 1200,
        "customer": {
            "name": "Andrii",
            "surname": "Rodinov",
            "phones": [9876543, 1234567],
            "address": "PTI, Peremohy 37, Kyiv, UA"
        },
        "payment": {
            "card_owner": "Andrii Rodionov",
            "cardId": 12345678
        },
        "items_id": [ObjectId("552bc0f7bbcdf26a32e99954"), ObjectId("552bc285bbcdf26a32e99957")]
    },
    {
        "order_number": 201514,
        "date": datetime(2023, 1, 16),
        "total_sum": 400,
        "customer": {
            "name": "Oleg",
            "surname": "Ivanov",
            "phones": [9876543, 1234567],
            "address": "PTI, Peremohy 37, Kyiv, UA"
        },
        "payment": {
            "card_owner": "Oleg Ivanov",
            "cardId": 12345679
        },
        "items_id": [ObjectId("552bc285bbcdf26a32e99957"), ObjectId("552bc0f7bbcdf26a32e99958")]
    }
]
db.orders.insert_many(orders)

# 2. get all orders
print("all orders:")
for order in db.orders.find():
    print(order)

# 3. orders with total of more than a
expensive_orders = db.orders.find({"total_sum": {"$gt": 500}})
print("orders with total of more than 500:")
for order in expensive_orders:
    print(order)

# 4. get orders made by some exact customer
customer_orders = db.orders.find({"customer.name": "Andrii", "customer.surname": "Rodinov"})
print("orders made by Andrii Rodinov:")
for order in customer_orders:
    print(order)

# 5. get all orders with some exact item
orders_with_item = db.orders.find({"items_id": ObjectId("552bc285bbcdf26a32e99957")})
print("Orders with item (ID 552bc285bbcdf26a32e99957):")
for order in orders_with_item:
    print(order)

# 6. add one more item to all orders with some exact item and increase the total of the order
db.orders.update_many(
    {"items_id": ObjectId("552bc285bbcdf26a32e99957")},
    {
        "$push": {"items_id": ObjectId("552bc285bbcdf26a32e99959")},
        "$inc": {"total_sum": 100}
    }
)

# 7. get a number of items in some exact order
order_items_count = db.orders.aggregate([
    {"$match": {"order_number": 201513}},
    {"$project": {"number_of_items": {"$size": "$items_id"}}}
])
print("a number of items in order 201513:")
for count in order_items_count:
    print(count)

# 8. get information about the customer and numbers of credit cards for orders with total of more than a
expensive_order_customers = db.orders.find(
    {"total_sum": {"$gt": 500}},
    {"customer": 1, "payment.cardId": 1, "_id": 0}
)
print("customers and card numbers for orders with total of more than 500:")
for customer in expensive_order_customers:
    print(customer)

# 9. remove item from orders that were made in some exact period of time
db.orders.update_many(
    {"date": {"$gte": datetime(2023, 1, 1), "$lte": datetime(2023, 1, 31)}},
    {"$pull": {"items_id": ObjectId("552bc285bbcdf26a32e99957")}}
)

# 10. rename the customer (upd the surname)
db.orders.update_many(
    {"customer.surname": "Rodinov"},
    {"$set": {"customer.surname": "Rodionov"}}
)

# 11. find orders made by some exact customer and get information about customer and items
customer_orders_with_items = db.orders.aggregate([
    {"$match": {"customer.name": "Andrii", "customer.surname": "Rodionov"}},
    {"$lookup": {
        "from": "items",
        "localField": "items_id",
        "foreignField": "_id",
        "as": "items"
    }},
    {"$project": {
        "customer": 1,
        "items.model": 1,
        "items.price": 1,
        "_id": 0
    }}
])
print("Customer information and items for orders of Andrii Rodionov:")
for order in customer_orders_with_items:
    print(order)

# creating Capped Collection for reviews
if "reviews" in db.list_collection_names():
    db.reviews.drop()
db.create_collection("reviews", capped=True, size=5000, max=5)


# 1. old reviews are being removed
reviews = [
    {"review": "Review 1", "rating": 5},
    {"review": "Review 2", "rating": 4},
    {"review": "Review 3", "rating": 3},
    {"review": "Review 4", "rating": 2},
    {"review": "Review 5", "rating": 1},
    {"review": "Review 6", "rating": 5}
]
db.reviews.insert_many(reviews)

print("reviews:")
for review in db.reviews.find():
    print(review)

# close connection
client.close()
