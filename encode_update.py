import pymongo
from sshtunnel import SSHTunnelForwarder
from bson.objectid import ObjectId
import re
from tqdm import tqdm

regex = re.compile(r"[^a-zA-Z0-9\s\p{P}]")

server = SSHTunnelForwarder(("asr2.iem.technion.ac.il", 22), ssh_username="nimo", ssh_password="Laos2017",
                            remote_bind_address=('127.0.0.1', 27017))

server.start()
connection = pymongo.MongoClient('127.0.0.1', server.local_bind_port)

db = connection['asr16']
collection = db['documents']

filter = {"group": {"$exists": True}}

result = collection.find(filter, {"_id": 1, "current_document": 1, "posted_document": 1})

for doc in tqdm(collection.find(filter)):
    new_doc = {unicode(k).encode("utf-8"): regex.sub("", unicode(v).encode("utf-8")) for k, v in doc.iteritems()}
    filter_id = {"_id": ObjectId(new_doc["_id"])}
    update = {"$set": {"current_document": new_doc["current_document"], "posted_document": new_doc["posted_document"]}}
    result = collection.update_one(filter_id, update)
    check = dict(collection.find_one(filter_id, {"_id": 1, "current_document": 1, "posted_document": 1}))

connection.close()
server.stop()
