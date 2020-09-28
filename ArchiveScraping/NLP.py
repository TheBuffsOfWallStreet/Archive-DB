import spacy as sp
from textblob import TextBlob

nlp = sp.load("en_core_web_sm")

'''
Use spacy to search for named entities.
Return only the entity types of intrest.
'''
def getEntities(text, selected_entities = {'ORG', 'PERSON'}):
    doc = nlp(text)
    entities = {}
    for ent in selected_entities:
        entities[ent] = set()
    for ent in doc.ents:
        if ent.label_ in selected_entities:
            entities[ent.label_].add(ent.text)
    for ent in entities:
        # Convert to list so it's json serializable.
        entities[ent] = list(entities[ent])
    return entities


def getSentiment(text):
    '''
    A simple wrapper for TextBlob sentiment detector.
    Returns 2 values, polarity (-1,1) and subjectivity (0,1).
    '''
    return TextBlob(text).sentiment
