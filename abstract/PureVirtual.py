import logging

"""
Decorator to throw exception for all abstract methods with no intended implementation
"""


def purevirtual(func):
    def wrapper():
        msg = "{} is a pure virtual function and must be implemented by child class".format(func.__name__)
        logging.critical(msg)
        raise NotImplementedError(msg)

    return wrapper
