import logging
from abc import abstractmethod


class MultiProcessHandler(logging.Handler):

    @abstractmethod
    def reset_for_new_process(self) -> None:
        """
        Make any required changes to account for the fact that the Handler may have been forked into a new process
        """
        pass
