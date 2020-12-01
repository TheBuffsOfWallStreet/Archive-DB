import multiprocessing
from tqdm import tqdm


def runProcesses(func, cursor, max_workers=2):
    '''Wrapper for concurrent.furures with helpful performance metrics.'''
    with multiprocessing.Pool(max_workers) as p:
        bar = tqdm(p.imap(func, cursor, 1),
                   total=len(cursor),
                   unit='items',
                   smoothing=0.1)
        for _ in bar:
            pass
