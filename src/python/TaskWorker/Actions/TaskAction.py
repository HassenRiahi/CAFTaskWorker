import logging

class TaskAction(object):
    """The ABC of all actions"""

    def __init__(self, config):
        self.logger = logging.getLogger(type(self).__name__)
        self.config = config
        self.jobtypeMapper = { "Analysis" : "Processing",
                               "PrivateMC" : "Production",
                               "Generic" : "Generic",}


    def execute(self):
        raise NotImplementedError

