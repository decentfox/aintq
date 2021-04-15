import pickle


def pickle_data(*args, **kwargs):
    return pickle.dumps(dict(
        args=args,
        kwargs=kwargs,
    ))


def unpickle_data(data):
    return pickle.loads(data)


def generate_task_name(func):
    return f'{func.__module__}.{func.__name__}'
