import json
import random
import pandas as pd

random.seed(0)

names = ["6A28XD", "GDRVGS", "GULFEI", "EL8Y8Y", "YVC01V", "PUORHX", "8DJE1B", "VJ0UD7", "SSKKP9", "HM0ZWN", "2F2TUQ",
         "6B84RH", "MVNR0C", "X73JKK", "GLPDBE", "Y3QIA4", "E9Y47A", "7TLUKZ", "H4RL3I", "EMBFMM", "VNFR10", "K4DLWU",
         "BX4AY8", "OC7JFW", "IMZNXD", "1A9150", "QTQ1K0", "L1SHMK", "O8SOUY", "BY689R", "B9ZFZ4", "TA56F9", "NFY421",
         "YS2053", "Q4BP7I", "9AGGHI", "N2E8FH", "D43ZWG", "MUNR8D", "5MF60U", "AX9K6V", "IJLP1V", "5AJN77", "BGKI3Z",
         "O0LP7L", "5W7YWF", "WQ9A6G", "TKGY9C", "SXUGGC", "RTHEG1", "YX265F", "JC7Y4H", "DQH01D", "G0AAMG", "SHACO3",
         "SVUVOD", "8R3U9A", "O5TXVU", "V2YISI", "2MKNSN", "5ZJSGQ", "0R9HGX", "P1O4G7", "GM2CQT", "HCHOWW", "OL1RSO",
         "XC9WRW", "GB33WW", "HSO0P3", "6J28UU", "1XCEZG", "NC8VCA", "ZPRHXI", "6SZYG7", "7TCG5B", "ZTQ93C", "VNLJSE",
         "4YRS9W", "B8P2ZU", "RX0VB2", "K5A5DZ", "64O07E", "7SKA8Z", "2TQDP3", "8HFYSE", "QQKVXX", "ZMBGG0", "5GTZEY",
         "RCROC4", "ZEL4KL"]

max_group_size = 3
random.shuffle(names)

# divide into groups having 3 groups per name and single pair appearance maximum

groups = [[] for i in range(120)]
pairs = dict()
for name in names:
    pairs[name] = set()
    # Find the groups that the name can be added to
    possible_groups = [i for i in range(120) if len(groups[i]) < max_group_size]
    for i in possible_groups:
        for other_name in groups[i]:
            if other_name in pairs[name]:
                possible_groups.remove(i)
                break

    for i in range(3):
        minimals = [x for x in possible_groups if len(groups[x]) == min(map(len, groups))]
        group = random.choice(minimals)
        for other_name in groups[group]:
            pairs[name].add(other_name)
            pairs[other_name].add(name)
        groups[group].append(name)
        possible_groups.remove(group)

# divide into categories A and B, using LLM and not using LLM

groups = sorted(groups, key=len, reverse=True)
random.shuffle(groups)
division_dict = dict()
for name in names:
    division_dict[name] = {"flag_A": False, "flag_B": False}
groups_A, groups_B = [], []

for group in groups:
    sum_A = sum(division_dict[name]["flag_A"] for name in group)
    sum_B = sum(division_dict[name]["flag_B"] for name in group)

    if sum_A == sum_B == 2:
        if len(groups_A) < len(groups_B):
            groups_A.append(group)
        else:
            groups_B.append(group)

    elif sum_A < sum_B:
        groups_A.append(group)
        for name in group:
            division_dict[name]["flag_A"] = True
    else:
        groups_B.append(group)
        for name in group:
            division_dict[name]["flag_B"] = True


# divide into topics, validating every one is not having the same topic twice


def filter_lists(input_list):
    # Create a set to keep track of seen items
    seen_items = set()
    # Create an empty list to store the filtered sub-lists
    filtered_lists = []

    # Iterate over the input list
    for sub_list in input_list:
        # Check if any item in the current sub-list has been seen before
        if any(item in seen_items for item in sub_list):
            continue  # Skip the current sub-list if it contains a seen item
        # Add the current sub-list to the filtered list if it doesn't contain any seen items
        filtered_lists.append(sub_list)
        # Add the items in the current sub-list to the set of seen items
        seen_items.update(sub_list)

    # Return the list of filtered sub-lists
    return filtered_lists


def add_dummies_and_bots(group, topic, bot_dict):
    if len(group) == 2:
        group.append("dummy")
    bots = [bot for bot in bot_dict.keys() if topic in bot_dict[bot]]
    group.extend(bots)
    return group


# add bot users
bot_dict = json.load(open("bot_docs.json"))
bot_dict = {k.encode('utf-8'): [id.encode('utf-8') for id in v] for k, v in bot_dict.items()}

topics = ["212", "210", "211", "218", "258", "274", "252", "235", "250", "255", "289", "281", "283", "233", "201",
          "203", "204", "209", "245", "244", "261", "246", "228", "226", "268", "249", "262", "272", "296", "291"]

topic_dict = dict.fromkeys(topics)

for topic in topics:
    topic_dict[topic] = []

    filtered_A = filter_lists(groups_A)
    chosen_A = random.sample(filtered_A, 2)
    topic_dict[topic].append((add_dummies_and_bots(chosen_A[0], topic, bot_dict), "A", "BERT"))
    topic_dict[topic].append((add_dummies_and_bots(chosen_A[1], topic, bot_dict), "A", "LambdaMART"))
    groups_A.remove(chosen_A[0])
    groups_A.remove(chosen_A[1])

    filtered_B = filter_lists(groups_B)
    chosen_B = random.sample(filtered_B, 2)
    topic_dict[topic].append((add_dummies_and_bots(chosen_B[0],topic,bot_dict), "B", "BERT"))
    topic_dict[topic].append((add_dummies_and_bots(chosen_B[1],topic,bot_dict), "B", "LambdaMART"))
    groups_B.remove(chosen_B[0])
    groups_B.remove(chosen_B[1])

initials = pd.read_csv("initials.csv")
rows = []

for k, v in topic_dict.items():
    init_data = initials[initials["_id"].astype(str) == k]
    for group in v:
        team, LLM, ranker = group
        for user in team:
            queries = eval(init_data["queries"].values[0])
            row = {"query_id": k, "username": user, "variant": True if LLM == "A" else False, "ranker": ranker,
                   "query": queries[0], "query1": queries[0], "query2": queries[1], "query3": queries[2],
                   "description": init_data["description"].values[0], "position_q1": team.index(user) + 1,
                   "position_q2": team.index(user) + 1, "position_q3": team.index(user) + 1, "score1": 0, "score2": 0,
                   "score3": 0, "posted_document": init_data["doc"].values[0],
                   "current_document": init_data["doc"].values[0]}

            rows.append(row)

x = 1

pd.DataFrame(rows).to_csv("doc_assignment_23.csv", index=False)
x = 1
