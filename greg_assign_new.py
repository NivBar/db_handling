from division_utils import *
from copy import copy, deepcopy
import random as rd
import numpy as np


def get_users(num=80):
    return ['foo' + str(i) for i in range(num)]  # TODO: get from db real users


def expand_working_queries(queries, number_of_groups):
    expanded_version = []
    for query in queries:
        for i in range(number_of_groups):
            expanded_version.append(str(query) + "_" + str(i))
    return expanded_version


def map_users(users_list, queries_list, groups_dict):
    # rd.shuffle(users_list)
    num_queries_per_user = 3
    users_expanded = users_list * num_queries_per_user  # number of queries per user

    mapping = {}
    for group in groups_dict:
        num_of_users = groups_dict[group]['num_students']
        users_in_group = users_expanded[:num_of_users * len(queries_list)]
        users_expanded = users_expanded[num_of_users * len(queries_list):]
        group_map = {topic: users for topic, users in
                     zip(queries_list, np.array_split(users_in_group, len(queries_list)))}
        # group_map = {query: users_list[i*num_of_users:(i+1)*num_of_users] for i, query in enumerate(queries_list)}
        mapping[group] = group_map
        rd.shuffle(users_list)

    user_mapping = {u: [] for u in users_list}
    for group in mapping:
        for query in mapping[group]:
            for user in mapping[group][query]:
                user_mapping[user].append(query)

    assert all([len(queries) == num_queries_per_user for queries in user_mapping.values()])
    assert all([len(set(queries)) == num_queries_per_user for queries in user_mapping.values()])
    return mapping


def get_query_to_user(user_query_map, queries):
    qtu = {q: [] for q in queries}
    for user in user_query_map:
        for query in user_query_map[user]:
            qtu[query].append(user)
    return qtu


def get_query_for_user(user_to_query, query_to_user, user, working_set):
    query_overlap_count = {q: 0 for q in working_set}
    for query in working_set:
        users = set(deepcopy(query_to_user[query]))
        for q in user_to_query[user]:
            users_of_query = set(deepcopy(query_to_user[q]))
            query_overlap_count[query] += len(users.intersection(users_of_query))
    query_result = sorted(working_set, key=lambda x: query_overlap_count[x])[0]
    return query_result


def update_user_banned_queries(user_banned_queries, user_groups, user, queries):
    result = [q for q in queries if q.split("_")[1] in user_groups]
    user_banned_queries[user].extend(result)
    user_banned_queries[user] = list(set(user_banned_queries[user]))
    return user_banned_queries


def user_query_mapping(users, expanded_queries, number_of_user_per_query):
    user_query_map = {user: [] for user in users}
    user_groups = {user: [] for user in users}
    user_banned_query_map = {user: [] for user in users}
    groups = ["0", "1", "2"]
    user_ranker_index = {}
    working_queries = copy(expanded_queries)
    queries_competitor_number = {query: 0 for query in expanded_queries}
    while working_queries:
        for user in users:
            tmp = list(set(working_queries) - set(user_banned_query_map[user]))
            rd.shuffle(tmp)
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
            # working_queries = [q for q in working_queries if
            #                    queries_competitor_number[q] < number_of_user_per_query[q.split('_')[1]]]
            working_queries = [q for q in working_queries if
                               queries_competitor_number[q] < number_of_user_per_query]
            if not working_queries:
                break
    groups_abc = ["A", "B", "C"]
    return {k: [x.split("_")[0] + "_" + groups_abc[int(x.split("_")[1])] for x in v] for k, v in user_query_map.items()}


def upload_init_documents(init_data, user_topics_map):
    db = get_database()

    for user, user_topics in user_topics_map.items():
        for topic in user_topics:
            init_document_dict = {  # TODO: check if fields change because of multiple queries
                "username": user,
                "query_id": topic,
                "query": init_data[topic]["queries"],
                "current_document": init_data[topic]["document"],
                "posted_document": init_data[topic]["document"],
                "description": init_data[topic]["description"],
            }
            db.documents.insert(init_document_dict)


def map_users_new(users_list, topics_list, groups_dict):
    num_queries_per_user = 3

    topic_to_users = {topic: {group: [] for group in groups_dict} for topic in topics_list}
    # user_to_topic = {user: {} for user in users_list}

    # for group in groups_dict:
    #     min_users = groups_dict[group]['num_students']
    #     for topic in topics_list:
    #         users_in_group = sum([topic_to_users[t][group] for t in topics_list], [])
    #         users_in_topic = sum(list(topic_to_users[topic].values()), [])
    #
    #         available_users = [u for u in users_list if len(user_to_topic[u]) < num_queries_per_user
    #                            and u not in users_in_topic + users_in_group]
    #         assert len(available_users) >= min_users, "Not enough users"
    #
    #         topic_to_users[topic][group] = available_users[:min_users]

    user_to_topic = {u: [] for u in users_list}
    all_matches = [(topic, group) for topic in topics_list for group in groups_dict.keys()]

    users_unmatched = [x[1] for x in sorted([(len(v), k) for k, v in user_to_topic.items()
                                             if len(v) < num_queries_per_user])]
    while len(users_unmatched) > 0:
        print 'users_unmatched: ', len(users_unmatched), ' / ', len(users_list)
        user = rd.choice(users_unmatched)
        matches = user_to_topic[user]
        available_matches = [(topic, group) for (topic, group) in all_matches
                             if len(topic_to_users[topic][group]) < groups_dict[group]['num_students']]
        if len(matches) > 0:
            user_topics = [t[0] for t in matches]
            user_groups = [t[1] for t in matches]
            available_matches = [m for m in available_matches if m[0] not in user_topics and m[1] not in user_groups]

            other_matches_users = [u for u in users_list if any([m in user_to_topic[u] for m in matches])]
            available_matches = sorted(available_matches, key=lambda m: len(
                set(topic_to_users[m[0]][m[1]]).intersection(set(other_matches_users))))

        if len(available_matches) == 0:
            continue

        topic, group = available_matches[0]
        topic_to_users[topic][group].append(user)
        user_to_topic[user].append((topic, group))

        users_unmatched = [x[1] for x in sorted([(len(v), k) for k, v in user_to_topic.items()
                                                 if len(v) < num_queries_per_user], reverse=True)]

    print user_to_topic
    print topic_to_users

    return user_to_topic


def get_initial_data():
    db = get_database()
    init_data = db.initials.find({}).sort('query_id')
    return {d['_id']: d for d in init_data}


def init_user(username='O7FVQ9', topics=None, variants=None):
    assert len(set(topics)) == 3, "Must have 3 different topics"
    assert len(variants) == 3, "Must have 3 variants"
    assert all([v in ['no_lm', 'with_lm'] for v in variants]), "Illegal variant"

    init_data = get_initial_data()

    db = get_database()
    for topic_id, var in zip(topics, variants):
        init_document_dict = {
            "username": username,
            "query_id": "{}_{}".format(topic_id, "A" if var == "with_lm" else "B"),
            "query": str(topic_id),
            "current_document": init_data[topic_id]["doc"],
            "posted_document": init_data[topic_id]["doc"],
            "description": init_data[topic_id]["description"],
            "query1": init_data[topic_id]["queries"][0],
            "query2": init_data[topic_id]["queries"][1],
            "query3": init_data[topic_id]["queries"][2],
            "variant": var,
            "edittion_time": datetime.now(),
        }

        db.documents.insert(init_document_dict)

if __name__ == '__main__':
    import random as rd

    # groups = {
    #     'A': {'num_students': 3, 'num_bots': 2, 'description': ''},
    #     'B': {'num_students': 2, 'num_bots': 3, 'description': 'No LM'},
    #     'C': {'num_students': 2, 'num_bots': 3, 'description': '?'},
    # }
    #
    number_of_groups = 4
    number_of_users_per_group = 2
    number_of_queries_per_user = 3
    number_of_queries = 30

    num_of_participants = number_of_groups * number_of_users_per_group * number_of_queries / number_of_queries_per_user

    print 'num_of_participants: ', num_of_participants

    init_data = get_initial_data()
    users = get_users(85)

    if len(users) > num_of_participants:
        x = float(number_of_groups) / number_of_queries_per_user
        bigger_group = x * (number_of_users_per_group + 1) * number_of_queries
        more_queries = []
        for i in range(1, 5):
            more_queries.append(x * number_of_users_per_group * (number_of_queries + i))

        print 'bigger_group: ', bigger_group
        print 'more_queries: ', more_queries
        raise Exception('Too many users')


    users_with_dummies = ['dummy_{}'.format(i) for i in range(90 - len(users))] + users
    rd.shuffle(users_with_dummies)

    queries = list(init_data.keys())
    # init_user(topics=queries[:3], variants=['no_lm', 'with_lm', 'with_lm'])
    expanded_queries = expand_working_queries(queries, number_of_groups=4)

    mapping = user_query_mapping(users_with_dummies, expanded_queries, 2)
    print mapping

    topic_to_user = {}
    for user in mapping:
        for topic in mapping[user]:
            if topic not in topic_to_user:
                topic_to_user[topic] = []
            topic_to_user[topic].append(user)

    print topic_to_user

    print(len(mapping))
    print(len(topic_to_user))

    for topic in topic_to_user:
        if len([x for x in topic_to_user[topic] if "dummy" in x]) > 1:
            print("2 or more dummies in topic:" + topic)
            raise Exception("2 or more dummies in topic:" + topic)

    variants_map = {"A": "with_lm", "B": "no_lm", "C": "with_lm"}

    for user in mapping:
        groups = [x.split("_")[1] for x in mapping[user]]
        variants = [variants_map[x] for x in groups]
        # init_user(username=user, topics=mapping[user], variants=variants)
    #
    # # each user has 3 different topics
    # assert all([len(set([t.split('_')[0] for t in mapping[u]])) == 3 for u in mapping]), "Each user should have 3 different topics"
    # # each topic has at least 3 users
    # assert all([len(topic_to_user[t]) >= 3 for t in topic_to_user]), "Each topic should have at least 3 users"

    #
    # upload_init_documents(init_data, mapping)

    # db = get_database()
    # init_document_dict = {  # TODO: check if fields change because of multiple queries
    #             "username": 'O7FVQ9',
    #             "query_id": '203',
    #             "query": 'reviews of les miserables',
    #             "current_document": "Reviews of the film Les Miserables.",
    #             "posted_document":  "Reviews of the film Les Miserables.",
    #             "description": "Find movie reviews of the film 'Les Miserables'.",
    #         }
    # db.documents.insert(init_document_dict)
    #
    # init_document_dict = {  # TODO: check if fields change because of multiple queries
    #             "username": 'MNDWZS',
    #             "query_id": '111',
    #             "query": 'another query',
    #             "current_document": "blah blah.",
    #             "posted_document":  "blah blah.",
    #             "description": "query description.",
    #             "query1": 'q1',
    #             "query2": 'q2',
    #             "query3": 'q3',
    #             "position_q1": 1,
    #             "position_q2": 3,
    #             "position_q3": 2,
    #             "score1": 0.5,
    #             "score2": 0.5,
    #             "score3": 0.5,
    #             "variant": 'no_lm',
    #         }
    # db.documents.insert(init_document_dict)
    # init_document_dict = {  # TODO: check if fields change because of multiple queries
    #             "username": 'MNDWZS',
    #             "query_id": '113',
    #             "query": 'new query',
    #             "current_document": "blah blah 432.",
    #             "posted_document":  "blah blah 432.",
    #             "description": "query .......",
    #             "query1": 'q1!',
    #             "query2": 'q2!',
    #             "query3": 'q3!',
    #             "position_q1": 1,
    #             "position_q2": 3,
    #             "position_q3": 2,
    #             "score1": 0.5,
    #             "score2": 0.5,
    #             "score3": 0.5,
    #             "variant": 'with_lm',
    #         }
    # db.documents.insert(init_document_dict)