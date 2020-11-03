# Aquarius Time Series (AQTS) Capture Ecosystem Switch
[![Build Status](https://travis-ci.org/usgs/aqts-capture-ecosystem-switch.svg?branch=master)](https://travis-ci.org/usgs/aqts-capture-ecosystem-switch)
[![codecov](https://codecov.io/gh/usgs/aqts-capture-ecosystem-switch/branch/master/graph/badge.svg)](https://codecov.io/gh/usgs/aqts-capture-ecosystem-switch)

AWS Lambda functions designed to minimize RDS costs.


## Stopping and starting the nwcapture dbs

Invoke the relevant lambda function:

```
aqts-capture-ecosystem-switch-<STAGE>-startCaptureDb
aqts-capture-ecosystem-switch-<STAGE>-stopCaptureDb
```

## Stopping and starting the observation dbs

Invoke the relevant lambda function:

```
aqts-capture-ecosystem-switch-<STAGE>-startObservationsDb
aqts-capture-ecosystem-switch-<STAGE>-stopObservationsDb
```


Note that shutdown for the observation db is advice and not a command.  If the
observation db is running an etl job, it will not shut down.

## Creating the QA databases

The nwcapture-qa database and the observations-qa databases are created when needed.  Invoke one of these state 
machines:

```
 aqts-capture-ecosystem-switch-create-db-QA 
 aqts-capture-ecosystem-switch-create-obs-db-QA
```

Be prepared to wait up to two hours for the database to get up and running.

## Deleting the QA databases

When you are finished with a QA database, you should delete it.  Invoke one of these lambda functions 

```
aqts-capture-ecosystem-switch-deleteCaptureDb-QA
aqts-capture-ecosystem-switch-deleteObservationsDb-QA
```

## Resizing the nwcapture-qa db

Don't attempt to run resize commands manually.  There are configurable high-cpu and low-cpu alarms defined in the 
serverless.yml file which will trigger resizes.  If you absolutely must resize manually, you can pass a fake alarm
in this format:

```
{
    "detail": {
        "state": {
            "value": "ALARM"
        }
    }
}
```

For the observations databases, you would pass this payload to the relevant lambda functions:

```
aqts-capture-ecosystem-switch-<STAGE>-shrinkObsDb
aqts-capture-ecosystem-switch-<STAGE>-growObsDb
```

In the case of the capture db, the situation is more complicated because resizing the database involves disabling
and enabling the data trigger, so pass the fake alarm to these lambdas

```
aqts-capture-ecosystem-switch-<STAGE>-executeShrink
aqts-capture-ecosystem-switch-<STAGE>-executeGrow
```

## What if I need to prevent automatic shutdown of nwcapture-test for a long running test?

1. AWS Console->Event Bridge->Events->Rules->aqts-capture-ecosystem-switch-sw-stoptest->Disable
2. Remember to enable again when your test is concluded.

## Advanced troubleshooting

If you need to turn the capture database off and on independent of the trigger, you can do so using the
```troubleshoot``` lambda function.  Provide a payload in one of the following two formats:

```
{ "action": "start_capture_db"}
{ "action": "stop_capture_db"}
```

Note that stopping and starting the database without disabling and re-enabling the trigger is likely to
lead to an increase in errors reported and in the size of the error queue.  
