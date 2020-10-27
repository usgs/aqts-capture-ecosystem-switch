# Aquarius Time Series (AQTS) Capture Ecosystem Switch
[![Build Status](https://travis-ci.org/usgs/aqts-capture-ecosystem-switch.svg?branch=master)](https://travis-ci.org/usgs/aqts-capture-ecosystem-switch)
[![codecov](https://codecov.io/gh/usgs/aqts-capture-ecosystem-switch/branch/master/graph/badge.svg)](https://codecov.io/gh/usgs/aqts-capture-ecosystem-switch)

AWS Lambda functions designed to turn off resources when not in use.

## Stopping and starting the nwcapture dbs

Invoke the relevant lambda function (for example, aqts-capture-ecosystem-switch-QA-StopCaptureDb) using any json payload.

## Stopping and starting the observation dbs

Invoke the relevant lambda function, but note that shutdown for the observation db is advice and not a command.  If the
observation db is running an etl job, it will not shut down.

## Creating the nwcapture-qa db

The nwcapture is created when needed.  Invoke the state machine aqts-capture-ecosystem-switch-create-db-QA and be 
prepared to wait up to two hours.

## Deleting the nwcapture-qa db

When you are finished with the nwcapture-qa db, you should delete it.  Invoke the lambda function 
aqts-capture-ecosystem-switch-deleteCaptureDb-QA

## Resizing the nwcapture-qa db

Don't attempt to run resize commands manually.  The current behavior is to increase the size of the db to the 
maximum of db.r5.4xlarge after five minutes of being at more than 50% cpu utilization (via high cpu CloudWatch alarm).
And to decrease the size of the db after 30 minutes of cpu utilization less than 10% (via low cpu CloudWatch alarm.

## What if I need to prevent automatic shutdown of nwcapture-test for a long running test?

1. AWS Console->Event Bridge->Events->Rules->aqts-capture-ecosystem-switch-sw-stoptest->Disable
2. Remember to enable again when your test is concluded.
