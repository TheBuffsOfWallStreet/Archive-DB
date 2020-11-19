import numpy as np
import wikipedia
from sklearn.feature_extraction.text import TfidfVectorizer
from difflib import SequenceMatcher

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



# name = input('Name: ')
# print('Suggestions: ')
# search = wikipedia.search(name)
# for i, s in enumerate(search):
#     print(i, s)
# choice = int(input('Choice: '))
# assert(choice in range(len(search)))
# keywords, tags = tfidf_wiki_categories(search[choice])
# print()
# print('Tags: ')
# print(tags)
# print()
# print('Key words: ')
# print(keywords)
p = wikipedia.page('Elon_Musk', auto_suggest=False)
print(p.summary)
with open('firm.names.wiki.csv') as file:
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
                    page = wikipedia.page(res[0][0], auto_suggest=False)
                    p_url = page.url.split('.org/')[1]
                    if url == p_url:
                        matches += 1
                    else:
                        search2 = url.replace('wiki/', '')
                        page2 = wikipedia.page(search2, auto_suggest=False)
                        if p_url == page2.url.split('.org/')[1]:
                            matches += 1
                        else:
                            print(i, search)
                            print(url, p_url)
                            print(res)
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
        if i%100 == 0:
            print("Matches: ", matches)
            print("Total: ", total)
    print("Matches: ", matches)
    print("Total: ", total)
    print("Unknown: ", unknown)
    print("Dis Errors: ", dis_errors)
    print("Key Errors: ", key_errors)
    print("Page Errors: ", page_errors)