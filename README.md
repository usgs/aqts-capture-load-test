# Aquarius Time Series (AQTS) Capture Load Test
[![Build Status](https://travis-ci.org/usgs/aqts-capture-ecosystem-switch.svg?branch=master)](https://travis-ci.org/usgs/aqts-capture-ecosystem-switch)
[![codecov](https://codecov.io/gh/usgs/aqts-capture-ecosystem-switch/branch/master/graph/badge.svg)](https://codecov.io/gh/usgs/aqts-capture-ecosystem-switch)

Designed to:

1. Restore the production database to a new cluster nwcapture-load
2. Disable aqts-capture-trigger
3. Copy JSON files representing a full run to the bucket iow-retriever-capture-load
4. Modify the secrets manager temporarily so that all AQTS lambdas think that the nwcapture-load database is the
   nwcapture-test database and the iow-retriever-capture-load bucket is the iow-retriever-capture-test bucket. 
5. Enable aqts-capture-trigger
6. After some TBD amount of time, run various queries against nwcapture-load to determine if the run was
   successful or not, and whether the run completed sufficiently quickly.
7. Send some notification
8. Change all secrets in the secrets manager that were temporarily modified
9. Delete the db cluster
10. Empty the load bucket
