import pymongo
from sshtunnel import SSHTunnelForwarder
from bson.objectid import ObjectId
import re
from tqdm import tqdm
import pandas as pd

# TODO: change epoch to the epoch you want to update
epoch = 8
data = pd.read_csv("./bot_followups/bot_followup_{}.csv".format(epoch))
data.query_id = data.query_id.astype(str)

regex = re.compile(r"[^a-zA-Z0-9\s\p{P}]")

server = SSHTunnelForwarder(("asr2.iem.technion.ac.il", 22), ssh_username="nimo", ssh_password="Laos2017",
                            remote_bind_address=('127.0.0.1', 27017))

server.start()
connection = pymongo.MongoClient('127.0.0.1', server.local_bind_port)

db = connection['asr16']
collection = db['documents']

for idx, row in data.iterrows():
    print(idx+1)
    filter = {"query_id": row.query_id, "group": row.group, "username": row.username}
    current_doc = collection.find_one(filter)
    current_document = unicode(regex.sub("", row.text)).encode("utf-8")

    new_values = {"$set": {"current_document": current_document}}
    collection.update_one(filter, new_values)

connection.close()
server.stop()
