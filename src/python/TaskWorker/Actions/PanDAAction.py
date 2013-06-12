from TaskWorker.Actions.TaskAction import TaskAction
from TaskWorker.DataObjects.Result import Result

class PanDAAction(TaskAction):
    """Generic PanDAAction. Probably not needed at the current stage
       but it since this should not cause a big overhead it would be 
       better to leave this here in order to eventually be ready to
       support specific PanDA interaction needs."""

    def __init__(self, pandaconfig, instance):
        TaskAction.__init__(self, pandaconfig, instance)
        #each PanDAAction needs to know the PandaServer URLs to use
        self.pandaurls = self.server.get('/crabserver/dev/info', data = {'subresource':'backendurls'})[0]['result'][0]
#        ## TODO check some specific config? otherwise this can probably be removed  and default init could be used
#        #print "specific panda action init", self

    def translateSiteName(self, sites):
        return ['ANALY_'+ s for s in sites]
