import pymongo
from sshtunnel import SSHTunnelForwarder
import pandas as pd
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
# result = collection.find(filter, {"_id": 1, "current_document": 1, "posted_document": 1})

new_docs = list()
for doc in tqdm(collection.find(filter)):
    new_doc = {unicode(k).encode("utf-8"): regex.sub("", unicode(v).encode("utf-8")) for k, v in doc.iteritems()}
    new_doc["round_no"] = str(int(new_doc["docno"].split(new_doc["query_id"])[0].replace("ROUND", "")))
    new_docs.append(new_doc)

df = pd.DataFrame(new_docs)
df.to_csv("db_snapshot.csv", encoding="utf-8", index=False)
connection.close()
server.stop()
