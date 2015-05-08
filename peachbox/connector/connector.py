class Connector(object):
    def __init__(self):
        self.param = None

    def set_param(self, param):
        raise NotImplementedError("%s must implement set_param()" % self.__class__.__name__)
