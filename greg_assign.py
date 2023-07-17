from pymongo import MongoClient
from copy import copy, deepcopy
from random import shuffle, seed
import csv


def read_initial_data(fname):
    stats = {}
    with open(fname) as f:
        reader = csv.DictReader(f)
        for row in reader:
            stats[row["query"].zfill(3)] = {i: row[i] for i in row if i != "query"}
    return stats


def retrieve_users():
    return ["ROMJQN", "CZ3IP0", "0V2RWW", "7XUGJ0", "RXK8H3", "Y553AZ", "5B1EWY", "THX8HK", "MEP5Y4", "CZFMFV",
            "SBHV4U", "6LWZ77", "FFBB0A", "ZQ22RB", "ZV1LFW", "8RUXW7", "PLB6ZI", "H4PVOF", "OAGQLN", "2WNFKH",
            "609XQD", "V5A8SX", "G269IH", "N1K1XQ", "0KKNZV", "1PPW7S", "PNA2MU", "Z07ABY", "SA9WFV", "4O6YND",
            "X93ZEW", "KP5G43", "GKKSVY", "GWX0BR", "2WJNDY", "UYV335", "9PFCP4", "T7KWL2", "3RXCQH", "YVHMQO",
            "5J5M18", "B8RZRL", "9DWMMZ", "6QL968", "YA7EE4", "WFC1J0", "T1FYNQ", "V0HAX1", "OK33NV", "W129AB"]


def expand_queries_to_participates(queries, number_of_rankers, number_of_participants):
    expanded_version = []
    for query in queries:
        for i in range(number_of_rankers):
            for j in range(number_of_participants):
                expanded_version.append(query + "_" + str(i) + "_" + str(j))
    return expanded_version


def expand_working_qeuries(queries, number_of_groups):
    expanded_version = []
    for query in queries:
        for i in range(number_of_groups):
            expanded_version.append(str(query) + "_" + str(i))
    return expanded_version


def get_query_to_user(user_query_map, queries):
    qtu = {q: [] for q in queries}
    for user in user_query_map:
        for query in user_query_map[user]:
            qtu[query].append(user)
    return qtu


def test_mapping(new_map, old_map):
    for user in new_map:
        for query in new_map[user]:
            prefix = query.split("_")[0]
            if prefix in old_map[user]:
                return False
    return True


def update_user_banned_queries(user_banned_queries, user_groups, user, queries):
    result = [q for q in queries if q.split("_")[1] in user_groups]
    user_banned_queries[user].extend(result)
    user_banned_queries[user] = list(set(user_banned_queries[user]))
    return user_banned_queries


def user_query_mapping(users, expanded_queries, number_of_user_per_query):
    user_query_map = {user: [] for user in users}
    user_groups = {user: [] for user in users}
    user_banned_query_map = {user: [] for user in users}
    groups = ["0", "1", "2", "3", "4"]
    user_ranker_index = {}
    working_queries = copy(expanded_queries)
    queries_competitor_number = {query: 0 for query in expanded_queries}
    while working_queries:
        for user in users:
            tmp = list(set(working_queries) - set(user_banned_query_map[user]))
            shuffle(tmp)
            if not tmp: continue
            query_to_user = get_query_to_user(user_query_map, expanded_queries)
            query = get_query_for_user(user_query_map, query_to_user, user, tmp)
            user_groups[user].append(query.split("_")[1])
            user_query_map[user].append(query)
            if not user_ranker_index.get(user, False):
                user_ranker_index[user] = query.split("_")[1]
            user_banned_query_map[user].append(query)
            more_groups = [i for i in groups if i != query.split("_")[1]]
            for group in more_groups:
                user_banned_query_map[user].append(query.split("_")[0] + "_" + group)
            user_banned_query_map = update_user_banned_queries(user_banned_query_map, user_groups[user], user, tmp)
            queries_competitor_number[query] += 1
            working_queries = [q for q in working_queries if queries_competitor_number[q] < number_of_user_per_query]
            if not working_queries:
                break
    return user_query_map


def get_query_for_user(user_to_query, query_to_user, user, working_set):
    query_overlap_count = {q: 0 for q in working_set}
    for query in working_set:
        users = set(deepcopy(query_to_user[query]))
        for q in user_to_query[user]:
            users_of_query = set(deepcopy(query_to_user[q]))
            query_overlap_count[query] += len(users.intersection(users_of_query))
    query_result = sorted(working_set, key=lambda x: query_overlap_count[x])[0]
    return query_result


def upload_data_to_mongo(data, user_query_map):
    client = MongoClient('asr2.iem.technion.ac.il', 27017)
    db = client.asr16

    for user in user_query_map:
        for query in user_query_map[user]:
            object = {}
            object["username"] = user
            object["current_document"] = data[query.split("_")[1]]["document"]
            object["posted_document"] = data[query.split("_")[1]]["document"]
            object["query_id"] = query
            object["query"] = data[query.split("_")[1]]["query_text"]
            object["description"] = data[query.split("_")[1]]["description"]
            db.documents.insert(object)


def test_number_of_queries(mapping, number_of_queries):
    for user in mapping:
        if len(mapping[user]) != number_of_queries:
            return False
    return True


seed(9001)
users = retrieve_users()
data = read_initial_data("../data/upload_texts.csv")
queries = list(data.keys())
expanded_queries = expand_working_qeuries(queries, 5)
while True:
    mapping = user_query_mapping(users, expanded_queries, 2)
    if len([mapping[a] for a in mapping if len(mapping[a]) == 2]) == 0 and len(
            [mapping[a] for a in mapping if len(mapping[a]) == 1]) == 0:
        break
if not test_number_of_queries(mapping, 3):
    print("PROBLEM WITH ASSIGNMENT MECHANISM!!!!!!")
    raise RuntimeError("exisiting due to problem in query mapping")
