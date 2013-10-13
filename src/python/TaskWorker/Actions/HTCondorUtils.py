
import os
import traceback

import classad
import htcondor

# This is part of the classad module as of HTCondor 8.1.2
def quote(value):
    ad = classad.ClassAd()
    ad["foo"] = str(value)
    return ad.lookup("foo").__str__()
if quote in classad:
    quote = classad.quote

class AuthenticatedSubprocess():

    def __init__(self, proxy):
        self.proxy = userproxy

    def __enter__(self):
        self.r, self.w = os.pipe()
        self.rpipe = os.fdopen(self.r, 'r')
        self.wpipe = os.fdopen(self.w, 'w')
        self.pid = os.fork()
        if self.pid == 0:
            htcondor.SecMan().invalidateAllSessions()
            htcondor.param['SEC_CLIENT_AUTHENTICATION_METHODS'] = 'FS,GSI'
            htcondor.param['DELEGATE_FULL_JOB_GSI_CREDENTIALS'] = 'true'
            os.environ['X509_USER_PROXY'] = proxy
            self.rpipe.close()
        else:
            self.wpipe.close()
        return self.pid, self.rpipe

    def __exit__(self, a, b, c):
        if self.pid == 0:
            if (a == None and b == None and c == None):
                self.wpipe.write("OK")
                self.wpipe.close()
                os._exit(0)
            else:
                exceptionString = str(traceback.format_exc(c))
                # Uh, I forgot what traceback function does this without the join 
                msg = "Trapped exception in Dagman.Fork: %s %s %s \n%s" % \
                                (a,b,c,str('\n'.join(traceback.format_tb(c))))
                self.wpipe.write(msg)
                self.wpipe.close()
                os._exit(1)
        else:
            os.waitpid(self.pid, 0)

