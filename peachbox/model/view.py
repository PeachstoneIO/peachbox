class View(object):
    mart   = None
    schema = None

    @classmethod
    def row(cls, **kwargs):
        raise NotImplementedError
