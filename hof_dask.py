import dask.dataframe as ds
import numpy as np


batter = ds.read_csv("Batting.csv").set_index('playerID')
candidates = ds.read_csv("candidates.csv").set_index('playerID')

batting_groupBy = batter.groupby("playerID")
candidates_groupby = candidates.groupby("playerID")

batting_sum = batting_groupBy.agg({"HR": np.sum, "AB": np.sum, "H": np.sum, "2B": np.sum,
                                   "3B": np.sum, "R": np.sum, "RBI": np.sum, "BB": np.sum, "SO": np.sum, "SB": np.sum})
candidates_sum = candidates_groupby.agg({"HR": np.sum, "AB": np.sum, "H": np.sum, "2B": np.sum,
                                         "3B": np.sum, "R": np.sum, "RBI": np.sum, "BB": np.sum, "SO": np.sum, "SB": np.sum})


batting_avg = batting_sum.assign(avg=batting_sum["H"]/batting_sum["AB"])

candidates_avg = candidates_sum.assign(
    avg=candidates_sum["H"]/candidates_sum["AB"])

batting_slug = batting_avg.assign(slug=(
    batting_sum["H"] + batting_sum["2B"] + 2 * batting_sum["3B"] + 3 * batting_sum["HR"])/batting_sum["AB"])

candidates_slug = candidates_avg.assign(slug=(
    candidates_sum["H"] + candidates_sum["2B"] + 2 * candidates_sum["3B"] + 3 * candidates_sum["HR"])/candidates_sum["AB"])

fielding = ds.read_csv("Fielding.csv", usecols=['playerID', 'POS', 'G'])

fielding_data = fielding[(fielding.POS != 'P')]

fielding_groupby = fielding_data.groupby(
    'playerID', 'POS').agg({"G": np.sum, "POS": np.max})


def f(POS): return 240 if POS == 'C' else 168 if POS == 'SS' else 12 if POS == '1B' else 132 if POS == '2B' else 84 if POS == '3B' else 48 if POS == 'OF' else None


fielding_groupby['score'] = fielding_groupby.POS.apply(f)
batting_info = batting_slug.merge(fielding_groupby, on='playerID')
candidates_info = candidates_slug.merge(fielding_groupby, on='playerID')


batting_dict = list()
candidate_dict = list()

for i in batting_info.iterrows():
    player = {
        'id': i[0],
        'G': i[1]['G'],
        'AB': i[1]['AB'],
        'R': i[1]['R'],
        'H': i[1]['H'],
        '2B': i[1]['2B'],
        '3B': i[1]['3B'],
        'HR': i[1]['HR'],
        'RBI': i[1]['RBI'],
        'BB': i[1]['BB'],
        'SO': i[1]['SO'],
        'SB': i[1]['SB'],
        'avg': i[1]['avg'],
        'slug': i[1]['slug'],
        'score': i[1]['score']
    }
    batting_dict.append(player)

for i in candidates_info.iterrows():
    player = {
        'id': i[0],
        'G': i[1]['G'],
        'AB': i[1]['AB'],
        'R': i[1]['R'],
        'H': i[1]['H'],
        '2B': i[1]['2B'],
        '3B': i[1]['3B'],
        'HR': i[1]['HR'],
        'RBI': i[1]['RBI'],
        'BB': i[1]['BB'],
        'SO': i[1]['SO'],
        'SB': i[1]['SB'],
        'avg': i[1]['avg'],
        'slug': i[1]['slug'],
        'score': i[1]['score']
    }
    candidate_dict.append(player)

def find_top_five_players(list_five, score):
    if(len(list_five) < 5):
        return True
    for i in list_five:
        if(score > i['score']):
            return True
    return False


def remove_lowest_scores(player_list):
    if(len(player_list) == 0):
        return []
    min_val = 10000
    index = -1
    for i in range(len(player_list)):
        if(player_list[i]["score"] < min_val):
            min_val = player_list[i]["score"]
            index = i
    player_list.pop(index)
    return player_list


def calculate_similarities_score(player_id):

    can_info = list()
    for i in batting_dict:
        if i['id'] != player_id['id']:
            can_info.append(i)

    player_scores = []
    for i in can_info:
        similarity_score = float(1000-((abs(player_id["G"]-i["G"])/20) + (abs(player_id['AB']-i['AB'])/75) +
                                (abs(player_id['R']-i['R'])/10) + (abs(player_id['H']-i['H'])/15) + (abs(player_id['2B']-i['2B'])/5) +
                                (abs(player_id['3B']-i['3B'])/4) + (abs(player_id['HR']-i['HR'])/2) + (abs(player_id['RBI']-i['RBI'])/10) +
                                (abs(player_id['BB']-i['BB'])/25) + (abs(player_id['SO']-i['SO'])/150) + (abs(player_id['SB']-i['SB'])/20) +
                                (abs(player_id['avg']-i['avg'])/0.001) + (abs(player_id['slug']-i['slug'])/0.002) +
                                (abs(player_id['score']-i['score']))))
        if(find_top_five_players(player_scores, similarity_score)):
            if(len(player_scores) == 5):
                remove_lowest_scores(player_scores)
            player = {}
            player['id'] = i['id']
            player['score'] = similarity_score
            player_scores.append(player)
    return player_scores


hall_of_fame = ds.read_csv("HallOfFame.csv").set_index('playerID')
hall_of_fame_group = hall_of_fame.groupby('playerID')

hall_of_fame_data = hall_of_fame_group.agg({"votes": np.max})

hall_of_fame_dict = list()

for i in hall_of_fame_data.iterrows():
    hall_of_fame_dict.append(i[0])

hall_of_fame_players = list()

def check_hall_of_fame(similar_list):
	count = 0
	for i in range(5):
		if similar_list[i]['id'] in hall_of_fame_dict:
			count += 1
	if (count >= 3):
		return True
	return False

for i in candidate_dict:
    similar_player = calculate_similarities_score(i)
    if((similar_player is not None) and check_hall_of_fame(similar_player)):
        hall_of_fame_players.append(i['id'])

if(len(hall_of_fame_players) == 0):
	print('There is no candidate who should be in the Hall of Fame')
else:
	f=open('susom.dask','w')
	content = "\n".join(hall_of_fame_players)
	f.write(content)
	f.close()