import spacy as sp

# sp.require_gpu()
sp.prefer_gpu()


def getEntities(text, selected_entities={'ORG', 'PERSON'}):
    '''
    Use spacy to search for named entities.
    Return only the entity types of intrest.
    '''
    nlp = sp.load("en_core_web_sm")
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
