from pymongo import MongoClient
import os
import sys
import subprocess
from datetime import datetime
import pandas as pd


def get_database():
    client = MongoClient('asr2.iem.technion.ac.il', 27017)
    db = client.asr16
    return db


def change_status(new_status=None):
    """
    Change status to running or not running.
    'indeterminate' is True if the competition is currently running.
    Otherwise, it is False (and then it's the ranking stage).
    :return:
    """
    db = get_database()
    status = db.status.find_one({})
    indeterminate = status['indeterminate']
    if new_status is not None and new_status == indeterminate:
        raise Exception('The status is already ' + str(new_status))
    status['indeterminate'] = not indeterminate
    db.status.save(status)


def get_status():
    db = get_database()
    status = db.status.find_one({})
    return status['indeterminate']


def get_current_documents():
    db = get_database()
    current_year = datetime(2023, 1, 1)
    documents = db.documents.find({'edittion_time': {'$gte': current_year}}).sort('query_id', 1)
    return documents


def assign_docno(documents, round_number):
    """
    Assign a unique docno to each document.
    :param documents:
    :param round_number:
    :return:
    """
    prefix = 'ROUND-' + str(round_number).zfill(2) + '-'
    docs_dict = {}
    for document in documents:
        topic_id = document['query_id'].encode().zfill(3)
        author = document['username'].encode()
        docno = prefix + topic_id + '-' + author
        doc_text = document['current_document'].encode(encoding='cp1252', errors='ignore') \
            .decode('utf-8', 'ignore').encode().rstrip()
        docs_dict[docno] = doc_text
    return docs_dict


topic_to_queries = {}


def get_queries(topic):
    topic = int(topic)
    return topic_to_queries[topic]


def write_trectext(documents, dir_path, file_name='documents.trectext'):
    file_path = dir_path + '/' + file_name
    with open(file_path, 'w') as f:
        for docno, text in documents.items():
            f.write('<DOC>\n')
            f.write('<DOCNO>' + docno + '</DOCNO>\n')
            f.write('<TEXT>\n' + text + '\n</TEXT>\n')
            f.write('</DOC>\n')
    return file_path


def rank(docs_trectext_file_path, queries_text_file_path, round_dir):
    scores_file_path = round_dir + '/scores.txt'
    scores = pd.read_csv(scores_file_path, sep=' ', header=None,
                         names=['query_id', 'Q0', 'docno', 'rank', 'score', 'run_id'])
    return scores



def upload_scores(rankings_dict):
    """
    Upload scores to database.
    :param rankings_dict: A dictionary of the form {docno: (position, score)}
    """
    db = get_database()
    documents = db.documents.find({})
    for document in documents:
        key = document["query_id"] + "-" + document["username"]
        document['position'] = docToRank[key][0]
        document['score'] = docToRank[key][1]
        document['posted_document'] = document['current_document']
        db.documents.save(document)