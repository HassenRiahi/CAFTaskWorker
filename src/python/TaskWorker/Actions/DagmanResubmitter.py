
class DagmanResubmitter(TaskAction.TaskAction):

    """
    Given a task name, resubmit failed tasks.

    Internally, we simply release the failed DAG.
    """

    def execute(self, *args, **kw):

        if 'task' not in kw:
            raise ValueError("No task specified.")
        task = kw['task']
        if 'user_proxy' not in task:
            raise ValueError("No proxy provided")
        proxy = task['user_proxy']

        self.logger.info("About to kill workflow: %s. Getting status first." % workflow)

        workflow = str(workflow)
        if not WORKFLOW_RE.match(workflow):
            raise Exception("Invalid workflow name.")

        loc = HTCondorLocator.HTCondorLocator(self.config)
        scheddName = loc.getSchedd()
        schedd, address = loc.getScheddObj(scheddName)

        # Release the DAG
        rootConst = "TaskType =?= \"ROOT\" && CRAB_ReqName =?= %s" % HTCondorUtils.quote(workflow)

        with AuthenticatedSubprocess(proxy) as (parent, rpipe):
            if not parent:
                schedd.act(htcondor.JobAction.Release, subDagConst)
        results = rpipe.read()
        if results != "OK":
            raise Exception("Failure when killing job: %s" % results)

