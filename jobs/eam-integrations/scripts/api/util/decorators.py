import functools


def cache_pickle(func):
    @functools.wraps(func)
    def wrapper_cache_pickle(*args, **kwargs):
        import pickle
        import os.path
        if os.path.isfile(func.__name__):
            with open(func.__name__, 'rb') as file_pi:
                return pickle.load(file_pi)
        else:
            value = func(*args, **kwargs)
            with open(func.__name__, 'wb') as file_pi:
                pickle.dump(value, file_pi)
            return value

    return wrapper_cache_pickle

