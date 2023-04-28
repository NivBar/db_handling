from sshtunnel import SSHTunnelForwarder
import pymongo
import json
import io
from pprint import pprint


bots = ["NMABOT", "NMTBOT", "NMSBOT", "MABOT", "MTBOT", "MSBOT"]

server = SSHTunnelForwarder(("asr2.iem.technion.ac.il", 22), ssh_username="nimo", ssh_password="Laos2017",
                            remote_bind_address=('127.0.0.1', 27017))

server.start()
connection = pymongo.MongoClient('127.0.0.1', server.local_bind_port)

db = connection['asr16']
collection = db['topics23']

bot_docs = dict()
for bot in bots:
    bot_docs[bot] = list()
    docs = collection.find({'bots': "{}".format(bot)})
    for doc in docs:
        id = doc["_id"].encode('utf8').split("_")[0]
        if id not in bot_docs[bot]:
            bot_docs[bot].append(id)
    bot_docs[bot] = sorted(bot_docs[bot])

with  open("bot_docs.json", "w") as outfile:
    json.dump(bot_docs, outfile)

connection.close()
server.stop()

x=1