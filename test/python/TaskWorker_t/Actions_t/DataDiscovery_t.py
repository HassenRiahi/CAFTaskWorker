import unittest

from WMCore.Configuration import Configuration
from WMCore.Services.Requests import Requests
from WMCore.DataStructs.Fileset import Fileset
from WMCore.DataStructs.File import File
from TaskWorker.Actions.DataDiscovery import DataDiscovery

class DataDiscoveryTest(unittest.TestCase):
    @staticmethod
    def prepareObjects():
        #Creting the input parameters
        requestname = '130911_093053_mmascher_crab_tmp2'
        task = {'tm_taskname':requestname} #just using some locations for this dataset
        locations = {'/GenericTTbar/HC-CMSSW_5_3_1_START53_V5-v1/GEN-SIM-RECO#289639b0-146f-411b-a220-fcdff0a13cd9':[u'ganymede.hep.kbfi.ee', u'srm.ihepa.ufl.edu', u'storm-se-01.ba.infn.it']}
        #and just using some files
        datasetfiles = {}
        datasetfiles['/store/mc/HC/GenericTTbar/GEN-SIM-RECO/CMSSW_5_3_1_START53_V5-v1/0011/626F3DC8-1EAE-E111-A64C-0025902CB6B0.root'] = {'NumberOfEvents':1700L, 'BlockName':'/GenericTTbar/HC-CMSSW_5_3_1_START53_V5-v1/GEN-SIM-RECO#289639b0-146f-411b-a220-fcdff0a13cd9',
            'Lumis':{1L:[670994L, 670986L, 670958L, 671001L, 670995L, 671018L, 670988L, 671009L, 671038L, 671028L, 670942L, 670960L, 670948L, 670987L,
                    670991L, 671006L, 671012L, 671014L, 671040L, 670998L, 670983L, 670999L, 671027L, 671052L, 670984L, 671003L, 670996L, 671031L,
                    671007L, 671011L, 671022L, 670985L, 670962L, 671021L],
                2L:[1L, 2L, 3L, 8L, 9L],
                4L:[5L, 6L, 7L, 1L, 2L, 100L]},
            'Parents':['/store/mc/HC/GenericTTbar/GEN-SIM-RAW/CMSSW_5_3_1_START53_V5-v1/0011/C880E78D-1EAE-E111-897B-0025902CB6AE.root',
                '/store/mc/HC/GenericTTbar/GEN-SIM-RAW/CMSSW_5_3_1_START53_V5-v1/0011/8C7BB8D5-1EAE-E111-B269-0025901AD638.root',
                '/store/mc/HC/GenericTTbar/GEN-SIM-RAW/CMSSW_5_3_1_START53_V5-v1/0011/8215258E-1EAE-E111-8A4E-0025904B11D4.root'],
            'Checksums':{'Checksum':'922282544', 'Adler32':'39f2938b', 'Md5':'NOTSET'}, 'Size':975355735L}
        datasetfiles['/store/mc/HC/GenericTTbar/GEN-SIM-RECO/CMSSW_5_3_1_START53_V5-v1/0011/7A5634E8-03AE-E111-9363-5404A63886EC.root'] = {'NumberOfEvents':1700L, 'BlockName':'/GenericTTbar/HC-CMSSW_5_3_1_START53_V5-v1/GEN-SIM-RECO#289639b0-146f-411b-a220-fcdff0a13cd9',
            'Lumis':{1L:[670165L, 670174L, 670199L, 670081L, 670192L, 670170L, 670029L, 670093L, 670173L, 670164L, 670146L, 670079L, 670200L, 670065L,
                    670062L, 670216L, 670202L, 670150L, 670176L, 670207L, 670087L, 670179L, 670191L, 670185L, 670160L, 670182L, 670177L, 670203L, 670154L,
                    670187L, 670138L, 670162L, 670066L, 670085L],
                2L:[4L, 5L, 20L, 21L, 22L],
                3L:[11L, 12L, 13L]},
            'Parents':['/store/mc/HC/GenericTTbar/GEN-SIM-RAW/CMSSW_5_3_1_START53_V5-v1/0011/BAC6F21E-04AE-E111-A591-003048D3C90E.root',
                '/store/mc/HC/GenericTTbar/GEN-SIM-RAW/CMSSW_5_3_1_START53_V5-v1/0011/781547E6-03AE-E111-B1C0-BCAEC5329717.root',
                '/store/mc/HC/GenericTTbar/GEN-SIM-RAW/CMSSW_5_3_1_START53_V5-v1/0011/38694250-03AE-E111-B618-003048D37580.root'],
            'Checksums':{'Checksum':'2628876232', 'Adler32':'21d69fc7', 'Md5':'NOTSET'}, 'Size':974920274L}
        #Instantiate DataDiscovery
        config = Configuration()
        config.section_("MyProxy")
        config.MyProxy.serverhostcert, config.MyProxy.serverhostkey = Requests().getKeyCert()
        obj = DataDiscovery(config, None, None)

        return obj, task, requestname, datasetfiles, locations

    def testFormatOutput(self):
        ddObj, task, requestname, datasetfiles, locations = self.prepareObjects() #ddObj = DataDiscoveryObject
        res = ddObj.formatOutput(task=task, requestname=requestname, datasetfiles=datasetfiles, locations=locations)

        #Check that the output is ok
        self.assertTrue(len(res.result), 2)
        self.assertIsInstance(res.result, Fileset)
        for resFile in res.result:
            lfn = resFile['lfn']
            self.assertIsInstance(resFile, File)
            self.assertEqual(len(resFile['runs']), len(datasetfiles[lfn]['Lumis']))
            for run in resFile['runs']:
                self.assertEqual(sorted(run.lumis), sorted(datasetfiles[lfn]['Lumis'][run.run]))

if __name__ == '__main__':
    unittest.main()
