from concurrent import futures
from datetime import datetime


def runProcesses(func, cursor, max_workers=2):
    '''Wrapper for concurrent.furures with helpful performance metrics.'''
    with futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
        processes = [executor.submit(func, episode) for episode in cursor]
        start_time = datetime.now()
        elapsed_time = 1
        num_per_time = 0
        time_remaining = -1
        for i, process in enumerate(futures.as_completed(processes)):
            try:
                res = process.result()
            except Exception as e:
                print(e)
            new_elapsed_time = (datetime.now() - start_time).seconds
            if new_elapsed_time > elapsed_time:
                elapsed_time = new_elapsed_time
                num_per_time = i / (elapsed_time + 1)
                time_remaining = (len(processes) - i) / (num_per_time + 0.01)
            print(f' {i}, {i/len(processes):.2%}, {num_per_time:.2f} functions/second, {elapsed_time:.0f}s elapsed, {time_remaining:.0f}s remaining', end='\r')
