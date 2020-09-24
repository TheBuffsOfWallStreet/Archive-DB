import json

'''Just a wrapper for the Segment class'''
def openFile(filename):
    return Segment(filename)

class Segment:
    def __init__(self, filename):
        self.filename = filename
        self.content = self.load(filename)
        pass

    '''
    Loads text from a given filename and parses JSON information.
    '''
    def load(self, filename):
        with open(filename, 'r') as file:
            text = '\n'.join(file.readlines())
        return json.loads(text)

    '''Returns Segment metadata'''
    def info(self):
        return self.content['metadata']

    '''Returns transcript as string, ignoring timestamps'''
    def text(self):
        return ' '.join([snippet[1] for snippet in self.content['snippets']])

    '''Returns transcript as formatted string including timestamps'''
    def snippets(self):
        return '\n\n'.join([' : '.join(snippet) for snippet in self.content['snippets']])

    '''
    Skeleton for new method.
    TODO: Implement function.
    Should yield the current speaker and the line they said
    '''
    def speakerLineGenerator(self):
        text = self.text()
        # for speaker in text:
        #     yield speaker_name, speaker_line

    '''
    Returns formatted string containing results from speakerLineGenerator()
    '''
    def speakerText(self):
        text = [' : '.join([speaker, line]) for speaker, line in self.speakerLineGenerator()]
        return '\n\n'.join(text)
