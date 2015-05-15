class Chain(object):
    def __init__(self, pipelines):
        self.pipelines = pipelines

    def execute(self, rdd):
        return self.execute_chain(rdd, self.pipelines)

    def execute_chain(self, rdd, pipelines):
        if len(pipelines)==1:
            return pipelines[0].execute(rdd)
        else:
            new_rdd = pipelines[0].execute(rdd)
            return self.execute_chain(new_rdd, pipelines[1:])

