import gzip
import pickle


def save(fname: str, obj):
    pickle.dump(obj, gzip.open(fname, 'wb'))

def load(fname: str):
    return pickle.load(gzip.open(fname, 'rb'))