import nltk
import pandas as pd
from nltk.tag import StanfordNERTagger
from nltk.tokenize import word_tokenize

# TO use Stanford Named Entity Recognizer, you need to download file first
# Download website link: https://nlp.stanford.edu/software/CRF-NER.html

jar = 'stanford-ner-4.0.0/stanford-ner.jar'
model = 'stanford-ner-4.0.0/english.all.3class.distsim.crf.ser.gz'
ner_tagger = StanfordNERTagger(model, jar, encoding = 'utf8')


def name_search(file_path):
    df = pd.read_csv(file_path)
    description = df['Description']
    name_list = []

    for sentence in description:
        tokens = nltk.word_tokenize(sentence)
        tags = ner_tagger.tag(tokens)
        name = ""

        for tag in tags:
            if tag[1] == 'PERSON':
                name = name + " " + tag[0]
        name_list.append(name)

    df['Speaker'] = name_list

    df.to_csv(file_path)


