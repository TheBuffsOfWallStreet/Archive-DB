import numpy as np
import wikipedia
import re
import requests
import json
from bs4 import BeautifulSoup as soup
from bs4 import NavigableString
from sklearn.feature_extraction.text import TfidfVectorizer
# from difflib import SequenceMatcher

import NLP

def tfidf_wiki_categories(name, threshold=0.6):
    '''
    Gets wikipedia page categories from page name and runs tfidf to
    filter out key words. Returns key words and categories
    '''
    tags = wikipedia.page(name, auto_suggest=False).categories
    vectorizer = TfidfVectorizer()
    X = vectorizer.fit_transform(tags).toarray()
    words = vectorizer.get_feature_names()
    key_words = set()
    m, n = np.shape(X)
    for i in range(m):
        for j in range(n):
            if X[i][j] >= threshold:
                key_words.add(words[j])
    return key_words, tags

def get_ceo_from_content(wiki_page):
    '''
    NOTE: THIS METHOD IS NOT RELIABLE
    Takes in a wikipedia page object and returns a list of named entities that could be
    the CEO of the company in question.
    '''
    text = wiki_page.content.lower()
    entities = NLP.getEntities(text, {'PERSON'})
    ceo_idx = []
    for m in re.finditer('(ceo|chief executive officer)', text):
        ceo_idx.append(m.start())
        ceo_idx.append(m.end())
    p_map = {}
    for p in entities['PERSON']:
        try:
            for m in re.finditer(p, text):
                p_map[m.start()] = p
        except:
            pass
    ceos = set()
    for c in ceo_idx:
        min_dist = np.inf
        match = 0
        for m in p_map:
            if abs(m-c) < min_dist:
                min_dist = abs(m-c)
                match = m
        ceos.add(p_map[match])
    return list(ceos)

def get_info_table_wiki(wiki_page):
    res = requests.get(wiki_page.url, timeout=5)
    page = soup(res.text, 'html.parser')
    table = page.find('table', {'class': 'infobox vcard'})
    info = {}
    try:
        for row in table.find_all('tr'):
            th = row.find('th')
            if th:
                key = th.text.replace(u'\xa0', ' ')
                val = None
                td = row.find('td')
                if td:
                    if td.find('li'):
                        val = []
                        for li in td.find_all('li'):
                            val.append(li.text.replace(u'\xa0', ' ')) 
                    else:
                        val = td.text.replace(u'\xa0', ' ')
                info[key] = val
    except Exception as e:
        print(e)
    return info
    

page = wikipedia.page('Capital One', auto_suggest=False)
# print(get_ceo_from_content(page))
print(get_info_table_wiki(page))

with open('firm.names.wiki.csv') as file:
    data = {}
    names = file.readlines()
    matches = 0
    total = 0
    unknown = 0
    dis_errors = 0
    key_errors = 0
    page_errors = 0
    for i, n in enumerate(names):
        search = n.split(',')[0].strip().strip('"').replace('&amp;', '&')
        url = n.split(',')[3].strip().strip('"').replace(';', ',')
        if not url:
            unknown += 1
            continue
        res = wikipedia.search(search, suggestion=True)
        if res:
            print(i, search)
            if url:
                try:
                    match = False
                    page = wikipedia.page(res[0][0], auto_suggest=False)
                    p_url = page.url.split('.org/')[1]
                    if url == p_url:
                        matches += 1
                        match = True
                    else:
                        search2 = url.replace('wiki/', '')
                        page2 = wikipedia.page(search2, auto_suggest=False)
                        if p_url == page2.url.split('.org/')[1]:
                            matches += 1
                            match = True
                        else:
                            print(i, search)
                            print(url, p_url)
                            print(res)
                    if match:
                        data[search] = get_info_table_wiki(page)
                except wikipedia.exceptions.DisambiguationError as e:
                    print(i, 'Disambigution Error')
                    dis_errors += 1
                    pass
                except KeyError as e:
                    print(i, 'Key Error')
                    key_errors += 1
                    pass
                except wikipedia.exceptions.PageError as e:
                    print(i, 'Page Error')
                    page_errors += 1
                    pass
        total += 1
        # with open('company_info.json', 'w') as out:
        #         json.dump(data, out, indent=4)
        if i%100 == 0:
            print("Matches: ", matches)
            print("Total: ", total)
            print(data)
    print("Matches: ", matches)
    print("Total: ", total)
    print("Unknown: ", unknown)
    print("Dis Errors: ", dis_errors)
    print("Key Errors: ", key_errors)
    print("Page Errors: ", page_errors)