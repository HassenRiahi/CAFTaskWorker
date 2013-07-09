import logging

from RESTInteractions import HTTPRequests


class TaskAction(object):
    """The abstract father of all actions"""

    def __init__(self, config, instance):
        self.logger = logging.getLogger(type(self).__name__)
        self.config = config
        self.jobtypeMapper = { "Analysis" : "Processing",
                               "PrivateMC" : "Production",
                               "Generic" : "Generic",}
        self.logger.info("Setting connections to %s" %(instance))
        self.server = HTTPRequests(instance, self.config.TaskWorker.cmscert, self.config.TaskWorker.cmskey)

    def execute(self):
        raise NotImplementedError

