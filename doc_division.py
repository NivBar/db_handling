import json
import itertools
from pymongo import MongoClient

client = MongoClient('asr2.iem.technion.ac.il', 27017)
db = client.asr16
col = db["topics23"]

def divide_bots(bots, topics):
    """
    Assuming 6 bots and 30 topics, having 2 bots compete in every topic, having 15 pairs of bots each pair competes over
    2 topics each
    :param bots: bot names
    :param topics: topic ids
    :return: dictionary of division of bots to topics
    """
    pairs = list(itertools.combinations(bots, 2)) * 2
    divisions = {topic: pair for topic, pair in zip(topics, pairs)}
    return divisions


bots = ["NMABOT", "NMTBOT", "NMSBOT", "MABOT", "MTBOT", "MSBOT"]
docs = json.load(open("topic_queries_doc.json", "r", encoding="utf8"))
new_docs = dict()
divisions = divide_bots(bots, docs.keys())

for k, v in docs.items():
    queries = ', '.join(v["queries"])

    new_docs["{}_A".format(k)] = v.copy()
    new_docs["{}_A".format(k)]["id_"] = "{}_A".format(k)
    new_docs["{}_A".format(k)]["LLM"] = True
    new_docs["{}_A".format(k)]["queries"] = queries
    new_docs["{}_A".format(k)]["bots"] = divisions[k]
    A_ind = col.insert_one(new_docs["{}_A".format(k)])
    print("{}_A".format(k), A_ind)

    new_docs["{}_B".format(k)] = v.copy()
    new_docs["{}_B".format(k)]["id_"] = "{}_B".format(k)
    new_docs["{}_B".format(k)]["LLM"] = False
    new_docs["{}_B".format(k)]["queries"] = queries
    new_docs["{}_B".format(k)]["bots"] = divisions[k]
    B_ind = col.insert_one(new_docs["{}_B".format(k)])
    print("{}_B".format(k), B_ind)

