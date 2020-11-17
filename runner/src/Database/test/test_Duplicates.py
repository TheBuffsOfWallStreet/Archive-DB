from Database import CleanDuplicates as cd


def test_unigrams():
    bag = cd.nGrams('hello world', 1)
    assert bag == {'hello', 'world'}


def test_bigrams():
    bag = cd.nGrams('hello big world', 2)
    assert bag == {'hello big', 'big world'}


def test_trigrams():
    bag = cd.nGrams('hello big world', 3)
    assert bag == {'hello big world'}


def test_cosine_similarity():
    bag1 = {'hello', 'world'}
    bag2 = {'hello', 'friend'}
    sim = cd.cosineSimilarity(bag1, bag2)
    solution = 1 / ((2 * 2)**.5 + 1)
    assert sim == solution


def test_jaccard_similarity():
    bag1 = {'hello', 'world'}
    bag2 = {'hello', 'friend'}
    sim = cd.jaccardSimilarity(bag1, bag2)
    solution = 1 / (3 + 1)
    assert sim == solution
