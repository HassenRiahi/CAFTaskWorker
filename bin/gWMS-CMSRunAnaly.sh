#!/bin/sh

#
# We used to rely on HTCondor file transfer plugins to handle output for us
# We changed this because HTCondor tosses the stdout/err, making the plugins
# difficult-to-impossible to run.
#

set -x
echo "SCRAM_ARCH=$SCRAM_ARCH"

if [ "X$_JOB_AD" != "X" ];
then
    CRAB_Dest=`grep '^CRAB_Dest =' $_JOB_AD | tr -d '"' | awk '{print $NF;}'`
    if [ "X$CRAB_Dest" = "X" ];
    then
        print "Unable to determine CRAB output destination directory"
        exit 2
    fi
    CRAB_localOutputFiles=`grep '^CRAB_localOutputFiles =' $_JOB_AD | tr -d '"' | awk '{print $NF;}'`
    CRAB_Id=`grep '^CRAB_Id =' $_JOB_AD | tr -d '"' | awk '{print $NF;}'`
    if [ "X$CRAB_Id" = "X" ];
    then
        print "Unable to determine CRAB Id."
        exit 2
    fi
fi

sh ./CMSRunAnaly.sh "$@"
EXIT_STATUS=$?

echo "Starting Stageout"
./cmscp.py "$PWD/cmsRun-stderr.log\?compressCount\=3\&remoteName\=cmsRun_$CRAB_Id.log" "$CRAB_Dest/cmsRun-stderr.log\?compressCount\=3\&remoteName\=cmsRun_$CRAB_Id.log"  || exit $?
./cmscp.py "$PWD/cmsRun-stdout.log\?compressCount\=3\&remoteName\=cmsRun_$CRAB_Id.log" "$CRAB_Dest/cmsRun-stdout.log\?compressCount\=3\&remoteName\=cmsRun_$CRAB_Id.log" || exit $?
./cmscp.py "$PWD/FrameworkJobReport.xml\?compressCount\=3\&remoteName\=cmsRun_$CRAB_Id.log" "$CRAB_Dest/FrameworkJobReport.xml\?compressCount\=3\&remoteName\=cmsRun_$CRAB_Id.log" || exit $?
OIFS=$IFS
IFS=" ,"
for file in $CRAB_localOutputFiles; do
    ./cmscp.py "$PWD/$file" $CRAB_Dest/$file
done
echo "Finished stageout"

exit $EXIT_STATUS

