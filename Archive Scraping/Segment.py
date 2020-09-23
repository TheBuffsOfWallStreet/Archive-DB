import json

'''Just a wrapper for the Segment class'''
def openFile(filename):
    return Segment(filename)

class Segment:
    def __init__(self, filename):
        self.filename = filename
        self.content = self.load(filename)
        pass

    def load(self, filename):
        with open(filename, 'r') as file:
            text = '\n'.join(file.readlines())
        return json.loads(text)

    def info(self):
        return self.content['metadata']

    def text(self):
        return ' '.join([snippet[1] for snippet in self.content['snippets']])

    def snippets(self):
        return '\n\n'.join([' : '.join(snippet) for snippet in self.content['snippets']])
