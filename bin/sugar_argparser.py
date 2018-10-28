"""Scripts that implements a class that extends ArgumentParser
and custom the error message.
"""

import argparse

class SugarArgumentParser(argparse.ArgumentParser):
    """Class that extends ArgumentParser and calls print_help()
    if there is an error.
    """

    def error(self, message):
        """Prints help if there is an error.

        Args:
            message. str. The error message.
        """
        self.print_help()
        exit(0)