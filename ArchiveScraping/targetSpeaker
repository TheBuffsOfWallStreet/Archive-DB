import pandas as pd


def findSpeaker():
    try:
        df = pd.read_csv('bloomberg.tv.2.csv')
    except:
        print("file could not be found in directory")


    # apply function below to remove any words matching company from description
    speaker = df.apply(removeCompany, axis=1)

    # Remove interview and undecessary symbols
    removalWords = ['Bloomberg TV Interview','BTV Interview','LIVE <GO> Interview',':','-']
    for x in removalWords:
        speaker = speaker.str.replace(x,'')

    # remove things of the nature (113223 MM)
    df['Speaker'] = speaker.str.replace(r'\([a-z,A-Z,0-9, ]*\)', '')


    #output file to csv
    df.to_csv('bloombergAltered.tv.2.csv')


def removeCompany(x):
    try:
        if(x['Name'] != None):
            stopWords = set(map(lambda x:x.lower(),x['Name'].split(' ')))
            description = x['Description'].split(' ')
            return ' '.join(w for w in description if not w.lower() in stopWords)
    except:
        print(x['Description']+" name field is empty")
        return x['Description'].split(' ')




findSpeaker()