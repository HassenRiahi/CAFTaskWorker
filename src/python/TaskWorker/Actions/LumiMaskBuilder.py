import urllib
from itertools import groupby, count

from TaskWorker.Actions.TaskAction import TaskAction
from TaskWorker.DataObjects.Result import Result

class LumiMaskBuilder(TaskAction):

    def __init__(self, config, server, resturl):
        TaskAction.__init__(self, config, server, resturl)

    def execute(self, *args, **kwargs):
        """ This Action does something useful in case the user did not specify a lumi mask. In this case the report command is meaningless as
            it will never report if there are unanalyzed lumis. So, we build a lumimask starting from the infos coming from the DBS discovery
            and we push it to the crab REST interface.
        """
        files = args[0]

        if not kwargs['task']['tm_split_args']['lumis'] and not kwargs['task']['tm_split_args']['runs']:
            self.logger.info("Reconstructing lumimask as the user did not specify it")
            lumilists = {}
            self.runs = []
            self.lumis = []

            #Take all the files and create one dict containing all the lumi to analyze
            for f in files:
                for run in f['runs']:
                    if run.run not in lumilists:
                        lumilists[run.run] = run.lumis
                    else:
                        lumilists[run.run] += run.lumis

            self.logger.debug("Lumilist reconstructed: %s" % lumilists)

            #Take the dict containing the lumilist (format: {run1 : [lumi1, lumi2 ...], run2 : [lumi1, lumi2 ...] ...}),
            #group the lumis in the same range (1,2,3,4 => [1,4]) and prepare the runs and lumis as expected by the REST. Example:
            #Input:
            #lumilist = {2L: [1L, 2L, 3L, 8L, 9L, 4L, 5L, 20L, 21L, 22L], 3L: [11L, 12L, 13L], 4L: [1L, 2L, 5L, 6L, 7L, 100L]}
            #Output:
            #runs = [2', '3', '4']
            #lumis = ['1,5,8,9,20,22', '11,13', '1,2,5,7,100,100']
            for run in lumilists:
                self.runs.append(str(run))
                self.lumis.append(','.join([ (lambda currLumi=consLumis.next(), numConsLumi=sum(1 for _ in consLumis): "%s,%s" % (currLumi, currLumi + numConsLumi))()
                                                        for _, consLumis in groupby(sorted(lumilists[run]), lambda x, c=count(): c.next() - x) ])
                )

            configreq = {'workflow': kwargs['task']['tm_taskname'],
                         'subresource': 'lumimask',
                        }
            self.server.post(self.resturl, data=urllib.urlencode(configreq) + '&runs='.join([''] + self.runs) + '&lumis='.join([''] + self.lumis))

        result = Result(task=kwargs['task'], result=files)
        return result
