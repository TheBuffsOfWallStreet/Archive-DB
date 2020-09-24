import spacy as sp

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