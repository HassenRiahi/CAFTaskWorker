from multiprocessing import cpu_count
import socket

from WMCore.Configuration import Configuration

config = Configuration()

## External services url's
config.section_("Services")
config.Services.PhEDExurl = 'https://phedex.cern.ch'
config.Services.DBSUrl = 'http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet'
config.Services.MyProxy = 'myproxy.cern.ch'

config.section_("TaskWorker")
config.TaskWorker.name = socket.getfqdn().lower()
config.TaskWorker.polling = 60 #seconds
 # we can add one worker per core, plus some spare ones since most of actions wait for I/O
config.TaskWorker.nslaves = cpu_count() + cpu_count()/2
config.TaskWorker.backend = 'panda'
#config.TaskWorker.backend = 'glidein'
## the parameters here below are used to contact cmsweb services for the REST-DB interactions 
# usually this is a proxy for private deployment or a service certificate registered in CMS VO
config.TaskWorker.cmscert = '/tmp/x509up_u15305'
config.TaskWorker.cmskey = '/tmp/x509up_u15305'

## Possible values for mode are:
#   - cmsweb-dev 
#   - cmsweb-preprod
#   - cmsweb-prod
#   - private
config.TaskWorker.mode = 'private'
## If 'private' mode then an server url is needed
config.TaskWorker.resturl = 'mattia-dev01.cern.ch'

#The following parameters assumes the installation in "one box" together with the REST
config.section_("MyProxy")
config.MyProxy.serverhostcert = '/data/certs/hostcert.pem' #PLEASE SET ME!!!
config.MyProxy.serverhostkey = '/data/certs/hostkey.pem' #PLEASE SET ME!!!
config.MyProxy.uisource = '/afs/cern.ch/cms/LCG/LCG-2/UI/cms_ui_env.sh'
config.MyProxy.credpath = None #PLEASE SET ME!!!
config.MyProxy.serverdn = None #PLEASE SET ME!!!

config.section_("Sites")
config.Sites.available = ["T2_CH_CERN", "T2_IT_Pisa", "T1_US_FNAL", "T2_DE_DESY"]
