import json
import re

def importCandidates(filepath):
    '''
    Takes a filepath for a candidate master file from fec.gov
    Writes out a candidates.json file of all the candidates with the key of candidate id
    '''
    candidates = {}

    with open(filepath, 'r') as f:
        lines = f.readlines()
        for line in lines:
            data = line.strip('\n').split('|')
            candidates[data[0]] = {}
            candidates[data[0]]['name'] = data[1]
            candidates[data[0]]['party'] = data[2]
            candidates[data[0]]['state'] = data[4]
            candidates[data[0]]['office'] = data[5]
            candidates[data[0]]['district'] = data[6]
            candidates[data[0]]['incumbent_status']  = data[7]
    
    with open('candidates.json', 'w') as f:
        json.dump(candidates, f)


def countOrgContributions(record_file, contributions_file='contributions.json'):
    '''
    Take a contribution from committees file from fec.gov and a contribution json
    Writes out a contribution json with the organization name as a key to a dictionary
    of aggregated contributions by candidate
    '''
    try:
        with open(contributions_file, 'r') as f:
            contributions = json.load(f)
    except:
        contributions = {}
    
    with open(record_file, 'r') as f:
        lines = f.readlines()
        print(lines[0])
        first = True
        for line in lines:
            data = line.strip('\n').split('|')
            cand_id = data[16]
            amount = data[14]
            name = data[7]
            if first:
                first = False
                print(cand_id, amount, name)
            if name not in contributions:
                contributions[name] = {}
            if cand_id not in contributions[name]:
                contributions[name][cand_id] = 0
            contributions[name][cand_id] += float(amount)
    
    with open(contributions_file, 'w') as f:
        json.dump(contributions, f)


def orgPartyAnalysis(org, contributions_json, candidate_json):
    '''
    Takes in a organization name, contribution json, and candidate json
    Returns the party contribution distributions
    '''
    with open(contributions_json, 'r') as f:
        contributions = json.load(f)
    with open(candidate_json, 'r') as f:
        candidates = json.load(f)
    
    party_dist = {}
    org = org.upper()
    orgs_found = []

    for name in contributions:
        if re.search('^'+org+'[\s,.]*', name): # This may not always be right TODO improve this
            orgs_found.append(name)
            for cand_id in contributions[name]:
                if cand_id in candidates:
                    party = candidates[cand_id].get('party', 'N/A')
                    if party not in party_dist:
                        party_dist[party] = 0
                    party_dist[party] += contributions[name].get(cand_id, 0)
                else:
                    print('Candidate ID not found: {}'.format(cand_id))

    return party_dist, orgs_found