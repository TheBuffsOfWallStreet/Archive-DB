import json


def openFile(filename):
    '''Just a wrapper for the Segment class.'''
    return Segment(filename)


class Segment:
    def __init__(self, filename):
        self.filename = filename
        self.content = self.load(filename)
        pass

    def load(self, filename):
        '''Loads text from a given filename and parses JSON information.'''
        with open(filename, 'r') as file:
            text = '\n'.join(file.readlines())
        return json.loads(text)

    def info(self):
        '''Returns Segment metadata.'''
        return self.content['metadata']

    def text(self):
        '''Returns transcript as string, ignoring timestamps.'''
        return ' '.join([snippet[1] for snippet in self.content['snippets']])

    def snippets(self):
        '''Returns transcript as formatted string including timestamps.'''
        return '\n\n'.join([' : '.join(snippet) for snippet in self.content['snippets']])

    def speakerLineGenerator(self):
        '''
        Skeleton for new method.
        TODO: Implement function.
        Should yield the current speaker and the line they said
        '''
        text = self.text()
        
        for t in text.split('>>'):
            speaker, line = 'unknown', t
            if len(t.split(':', 1)) == 2 and len(t.split(':',1)[0]) < 20:
                speaker, line = t.split(':', 1)
            yield speaker.strip(), line.strip()

    def speakerText(self):
        '''Returns formatted string containing results from speakerLineGenerator().'''
        text = [' : '.join([speaker, line]) for speaker, line in self.speakerLineGenerator()]
        return '\n\n'.join(text)
