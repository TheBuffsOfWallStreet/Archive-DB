from Clean import clean
from Duplicates import cleanDuplicates

if __name__ == '__main__':
    clean()
    cleanDuplicates(threshold=0.15, n_gram=5, n_days=7)
