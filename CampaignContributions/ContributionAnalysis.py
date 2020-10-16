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