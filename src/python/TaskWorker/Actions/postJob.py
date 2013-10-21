
#!/usr/bin/python

import os
import re
import sys
import time
import json
import errno
import signal
import commands

import WMCore.Services.PhEDEx.PhEDEx as PhEDEx

import RetryJob

fts_server = 'https://fts3-pilot.cern.ch:8443'

g_Job = None

def sighandler(*args):
    if g_Job:
        g_Job.cancel()

signal.signal(signal.SIGHUP,  sighandler)
signal.signal(signal.SIGINT,  sighandler)
signal.signal(signal.SIGTERM, sighandler)

REGEX_ID = re.compile("([a-f0-9]{8,8})-([a-f0-9]{4,4})-([a-f0-9]{4,4})-([a-f0-9]{4,4})-([a-f0-9]{12,12})")

class FTSJob(object):

    def __init__(self, transfer_list, count):
        self._id = None
        self._cancel = False
        self._sleep = 20
        self._transfer_list = transfer_list
        self._count = count
        with open("copyjobfile_%s" % count, "w") as fd:
            for source, dest in transfer_list:
                fd.write("%s %s\n" % (source, dest))

    def cancel(self):
        if self._id:
            cmd = "glite-transfer-cancel -s %s %s" % (fts_server, self._id)
            print "+", cmd
            os.system(cmd)

    def submit(self):
        cmd = "glite-transfer-submit -s %s -f copyjobfile_%s" % (fts_server, self._count)
        print "+", cmd
        status, output = commands.getstatusoutput(cmd)
        if status:
            raise Exception("glite-transfer-submit exited with status %s.\n%s" % (status, output))
        output = output.strip()
        print "Resulting transfer ID: %s" % output
        return output

    def status(self, long=False):
        if long:
            cmd = "glite-transfer-status -l -s %s %s" % (fts_server, self._id)
        else:
            cmd = "glite-transfer-status -s %s %s" % (fts_server, self._id)
        print "+", cmd
        status, output = commands.getstatusoutput(cmd)
        if status:
            raise Exception("glite-transfer-status exited with status %s.\n%s" % (status, output))
        return output.strip()

    def run(self):
        self._id = self.submit()
        if not REGEX_ID.match(self._id):
            raise Exception("Invalid ID returned from FTS transfer submit")
        idx = 0
        while True:
            idx += 1
            time.sleep(self._sleep)
            status = self.status()
            print status

            if status in ['Submitted', 'Pending', 'Ready', 'Active', 'Canceling', 'Hold']:
                continue

            #if status in ['Done', 'Finished', 'FinishedDirty', 'Failed', 'Canceled']:
            #TODO: Do parsing of "-l"
            if status in ['Done', 'Finished']:
                return 0

            if status in ['FinishedDirty', 'Failed', 'Canceled']:
                print self.status(True)
                return 1


def determineSizes(transfer_list):
    sizes = []
    for pfn in transfer_list:
        cmd = "lcg-ls -D srmv2 -b -l %s" % pfn
        print "+", cmd
        status, output = commands.getstatusoutput(cmd)
        if status:
            sizes.append(-1)
            continue
        info = output.split("\n")[0].split()
        if len(info) < 5:
            print "Invalid lcg-ls output:\n%s" % output
            sizes.append(-1)
            continue
        try:
            sizes.append(info[4])
        except ValueError:
            print "Invalid lcg-ls output:\n%s" % output
            sizes.append(-1)
            continue
    return sizes


def reportResults(job_id, dest_list, sizes):
    filtered_dest = [dest_list[i] for i in range(len(dest_list)) if sizes[i] >= 0]
    filtered_sizes = [i for i in sizes if i >= 0]
    retval = 0

    cmd = 'condor_qedit %s OutputSizes "\\"%s\\""' % (job_id, ",".join(filtered_sizes))
    print "+", cmd
    status, output = commands.getstatusoutput(cmd)
    if status:
        retval = status
        print output

    cmd = 'condor_qedit %s OutputPFNs "\\"%s\\""' % (job_id, ",".join(filtered_dest))
    print "+", cmd
    status, output = commands.getstatusoutput(cmd)
    if status:
        retval = status
        print output

    return retval


def resolvePFNs(source_site, dest_site, source_dir, dest_dir, filenames):

    p = PhEDEx.PhEDEx()
    lfns = [os.path.join(source_dir, filename) for filename in filenames]
    lfns += [os.path.join(dest_dir, filename) for filename in filenames]
    dest_info = p.getPFN(nodes=[source_site, dest_site], lfns=lfns)

    results = []
    for filename in filenames:
        slfn = os.path.join(source_dir, filename)
        dlfn = os.path.join(dest_dir, filename)
        results.append((dest_info[source_site, slfn], dest_info[dest_site, dlfn]))
    return results


REQUIRED_ATTRS = ['CRAB_ReqName', 'CRAB_Id', 'CRAB_OutputData', 'CRAB_JobSW', 'CRAB_AsyncDest']

class PostJob():


    def __init__(self):
        self.ad = None
        self.crab_id = -1
        self.report = None
        self.output = None
        self.input = None
        self.outputFiles = []


    def parseAd(self):
        with open(".job.ad") as fd:
            self.ad = classad.parseOld(fd)
        for attr in REQUIRED_ATTRS:
            if attr not in self.ad:
                raise Exception("Missing attribute in job ClassAd: %s" % attr)
        self.crab_id = int(self.ad['CRAB_Id'])


    def parseJson(self):
        with open("jobReport.json.%d" % self.crab_id) as fd:
            self.full_report = json.load(fd)

        if 'steps' not in self.full_report or 'cmsRun' not in self.full_report['steps']:
            raise ValueError("Invalid jobReport.json: missing cmsRun")
        self.report = self.full_report['steps']['cmsRun']

        if 'input' not in self.report or 'output' not in self.report:
            raise ValueError("Invalid jobReport.json: missing input/output")
        self.output = self.report['output']
        self.input = self.report['input']

        for outputModule in self.output.values():
            for outputFile in outputModule:
                if outputFile.get("output_module_class") != 'PoolOutputModule':
                    continue
                fileInfo = {}
                self.outputFiles.append(fileInfo)

                fileInfo['inparentlfns'] = outputFile.get("input", [])

                fileInfo['events'] = outputFile.get("events", -1)
                fileInfo['checksums'] = outputFile.get("checksums", {"cksum": "0", "adler32": "0"})

                if 'runs' not in outputFile:
                    continue
                fileInfo['outfileruns'] = []
                fileInfo['outfilelumis'] = []
                for run, lumis in outputFile['runs']:
                    for lumi in lumis:
                        fileInfo['outfileruns'].append(run)
                        fileInfo['outfilelumis'].append(lumi)


    def fixPerms(self):
        """
        HTCondor will default to a umask of 0077 for stdout/err.  When the DAG finishes,
        the schedd will chown the sandbox to the user which runs HTCondor.

        This prevents the user who owns the DAGs from retrieving the job output as they
        are no longer world-readable.
        """
        for base_file in ["job_err", "job_out"]:
            try:
                os.chmod("%s.%d" % (base_file, self.crab_id), 0644)
            except OSError, oe:
                if oe.errno != errno.ENOENT and oe.errno != errno.EPERM:
                    raise


    def upload(self):
        for fileInfo in self.outputFiles:
            configreq = {"taskname":        self.ad['CRAB_ReqName'],
                         "outfilelumis":    fileInfo['outfilelumis'],
                         "inparentlfns":    fileInfo['inparentlfns'],
                         "globalTag":       fileInfo['globalTag'],
                         "outfileruns":     fileInfo['outfileruns'],
                         "pandajobid":      self.crab_id,
                         "outsize":         fileInfo['outsize'],
                         "publishdataname": self.ad['CRAB_OutputData'],
                         "appver":          self.ad['CRAB_JobSW'],
                         "outtype":         self.outtype, # Not implemented
                         "checksummd5":     "-1", # Not implemented
                         "checksumcksum":   fileInfo['checksums']['cksum'],
                         "checksumadler32": fileInfo['checksums']['adler32'],
                         "outlocation":     fileInfo['outlocation'], 
                         "outtmplocation":  fileInfo['outdatasetname'],
                         "acquisitionera":  self.acquisitionera, # Not implemented
                         "outlfn":          fileInfo['outlfn'],
                         "events":          fileInfo['events'],
                    }


    def getSourceSite(self):
        cmd = "condor_q -userlog job_log.%d -af MATCH_EXP_JOBGLIDEIN_CMSSite -af JOBGLIDEIN_CMSSite" % self.crab_id
        status, output = commands.getstatusoutput(cmd)
        if status:
            print "Failed to query condor user log:\n%s" % output
            return 1
        match_site, source_site = output.split('\n')[0].split(" ", 1)
        if match_site == 'Unknown' or source_site == 'Unknown':
            # Didn't find it the first time, try looking in the jobReport.json
            if self.full_report.get('executed_site', None):
                print "Getting source_site from jobReport"
                source_site = report['executed_site']
            else:
                # TODO: Testing mode. If nothing turns up, just say it wwas
                #       Nebraska
                print "Site was unknown, so we just guessed Nebraska ..."
                source_site = 'T2_US_Nebraska'
        return source_site


    def stageout(source_dir, dest_dir, *filenames):
        self.dest_site = self.ad['CRAB_AsyncDest']
        source_site = self.getSourceSite()

        transfer_list = resolvePFNs(source_site, self.dest_site, source_dir, dest_dir, filenames)
        for source, dest in transfer_list:
            print "Copying %s to %s" % (source, dest)

        # Skip the first file - it's a tarball of the stdout/err
        for outfile in zip(filenames[1:], self.outputFiles, transfer_list):
            outlfn = os.path.join(dest_dir, outfile[0])
            outfile[1]['outlfn'] = outlfn
            outfile[1]['outtmplocation'] = outfile[2][0]
            outfile[1]['outlocation'] = outfile[2][1]

        global g_Job
        g_Job = FTSJob(transfer_list, self.crab_id)
        fts_job_result = g_Job.run()

        source_list = [i[0] for i in transfer_list]
        dest_list = [i[1] for i in transfer_list]
        sizes = determineSizes(source_list)
        report_result = reportResults("%d.%d" % (self.ad['Cluster'], self.ad['Process']), dest_list, sizes)
        if report_result:
            return report_result

        failures = len([i for i in sizes if i<0])

        if failures:
            raise RuntimeError("There were %d failed stageout attempts" % failures)

        return fts_job_result


    def execute(source_dir, dest_dir, *filenames):
        self.parseAd()

        retry = RetryJob.RetryJob()
        retval = retry.execute(self.crab_id)
        if retval:
           return retval

        self.parseJson()

        self.fixPerms()
        self.stageout(source_dir, dest_dir, *filenames)
        self.upload()

        return 0

if __name__ == '__main__':
    pj = PostJob()
    sys.exit(pj.execute(*sys.argv[2:]))

