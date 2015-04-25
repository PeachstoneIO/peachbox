import copy
import sys

class Data(object):
    def __init__(self):
        self.true_as_of_seconds = 0

    def set_true_as_of_seconds(self, seconds):
        self.true_as_of_seconds = seconds

