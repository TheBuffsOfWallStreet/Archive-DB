import pandas as pd


def findSpeaker():
    '''
    Strips description column for relevant information
    '''
    df = pd.read_csv('bloomberg.tv.2.csv')

    # apply function below to remove any words matching company from description
    speaker = df.apply(removeCompany, axis=1)

    # Remove interview and undecessary symbols
    removalWords = ['Bloomberg TV Interview','BTV Interview','LIVE <GO> Interview', 'LIVE<go> Interview',':','-','CEO', 'CIO', 'President', ',']
    for x in removalWords:
        speaker = speaker.str.replace(x,'')

    #Remove any strings containing weird characters
    speaker = speaker.str.replace(r'[^ ]*(\/|&)+[^ ]*', '')

    # remove whitespace things of the nature (113223 MM)
    speaker = speaker.str.replace(r'\([a-z,A-Z,0-9, ]*\)', '').str.strip()
    df['Speaker'] = speaker.str.split().str[-2:].str.join(' ')






    #output file to csv
    df.to_csv('bloombergAltered.tv.2.csv')



def removeCompany(x):
    '''
    Removes company name from description
    '''

    try:
        if(x['Name'] != None):
            stopWords = set(map(lambda x:x.lower(),x['Name'].split(' ')))
            description = x['Description'].split(' ')
            return ' '.join(w for w in description if not w.lower() in stopWords)
    except:
        print(x['Description']+" name field is empty")
        return x['Description'].split(' ')




findSpeaker()