# Aquarius Time Series (AQTS) Capture Load Test
[![Build Status](https://travis-ci.org/usgs/aqts-capture-ecosystem-switch.svg?branch=master)](https://travis-ci.org/usgs/aqts-capture-ecosystem-switch)
[![codecov](https://codecov.io/gh/usgs/aqts-capture-ecosystem-switch/branch/master/graph/badge.svg)](https://codecov.io/gh/usgs/aqts-capture-ecosystem-switch)

## Designed to:

1. Restore the production database to a new cluster nwcapture-load
2. Copy JSON files representing a full run to the bucket iow-retriever-capture-load
3. Modify the secrets manager temporarily so that all AQTS lambdas think that the nwcapture-load database is the
   nwcapture-qa database and the iow-retriever-capture-load bucket is the iow-retriever-capture-qa bucket. 
4. Enable aqts-capture-trigger
5. After some TBD amount of time, run various queries against nwcapture-load to determine if the run was
   successful or not, and whether the run completed sufficiently quickly.
6. Send some notification
7. Revert all secrets in the secrets manager that were temporarily modified
8. Delete the db cluster

## How to Clean Up Afterwards

If the test is running successfully, it should finish and clean itself up.  If there is an error, though, you can
clean up by manually running the lambdas:  restoreSecrets, deleteDbInstance, deleteDbCluster.
