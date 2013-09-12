import unittest
import os

from TaskWorker_t.Actions_t.DataDiscovery_t import DataDiscoveryTest
from TaskWorker.Actions.LumiMaskBuilder import LumiMaskBuilder

from RESTInteractions import HTTPRequests
from WMCore.Services.Requests import Requests


class LumiMaskBuilderTest(unittest.TestCase):
    def testExecute(self):
        #recycle DataDiscoveryTest code to create the input of this test
        ddObj, task, requestname, datasetfiles, locations = DataDiscoveryTest.prepareObjects()
        res = ddObj.formatOutput(task=task, requestname=requestname, datasetfiles=datasetfiles, locations=locations)

        #Test the case where the lumimask is empty. Thats the most interesting case
        cert, key = Requests().getKeyCert()
        server = HTTPRequests(os.environ['REST_URL'], cert, key, version="0.debug")
        lmb = LumiMaskBuilder(None, server, "/crabserver/dev/workflowdb")

        task = {}
        task['tm_split_args'] = {}
        #this is a wf name I had in the REST db. Used to check by hand if the db was updated.
        #we should create a RESTMock for unit tests
        task['tm_taskname'] = "130719_090932_mmascher_crab_tmp"
        task['tm_split_args']['lumis'] = {}
        task['tm_split_args']['runs'] = {}
        lmb.execute(res.result,task=task)

        self.assertEqual(lmb.runs, ['1','2','3','4'])
        self.assertEqual(lmb.lumis[1:], ['1,5,8,9,20,22', '11,13', '1,2,5,7,100,100']) #first run too long to check in a unit test
