# Aquarius Time Series (AQTS) Capture Ecosystem Switch
[![Build Status](https://travis-ci.org/usgs/aqts-capture-ecosystem-switch.svg?branch=master)](https://travis-ci.org/usgs/aqts-capture-ecosystem-switch)
[![codecov](https://codecov.io/gh/usgs/aqts-capture-ecosystem-switch/branch/master/graph/badge.svg)](https://codecov.io/gh/usgs/aqts-capture-ecosystem-switch)

AWS Lambda functions designed to minimize RDS costs.

## Purpose

The main purpose of the ecosystem switch is to minimize RDS costs.  This is done by:  dynamically resizing RDS in
response to the current activity of the AQTS Capture system, stopping and starting the TEST databases outside of office
hours, and making it possible to create and destroy the QA databases on an as-needed basis.  There is also a "circuit breaker"
which listens for a CloudWatch alarm from the Error Handler when system is behaving poorly.  This
circuit breaker will disable the capture trigger--preventing new data from coming into the system for processing--for
a period of a couple hours, before restarting. 

## Steady State Behavior

The Ecosystem Switch will listen for the High CPU Alarm and the Low CPU Alarm.  If the High CPU Alarm is activated,
the Ecosystem Switch will:  disable the capture trigger to prevent new data from coming in, order the capture database
to grow to its maximum size, and then re-enable the capture trigger.  Similarly, if the Low CPU Alarm activates, the
Ecosystem Switch will take similar steps and shrink the capture database to its minimum size.   The Ecosystem Switch
will also listen for an alarm from the Error Handler that indicates a very large number of errors is happening.  In
response, the Ecosystem Switch will disable the capture trigger for a long period of time (currently two hours) and
will then ensure that the database is at maximum size and re-enable the capture trigger.

## How to Make an Emergency Stop

If the AQTS System enters an undesirable state (large number of alarms), the circuit breaker should automatically 
disable the capture trigger.  However, the circuit breaker is designed to address transient bad conditions and will
periodically restart things.  

If the system itself is, for example, receiving large amounts of malformed data from the retriever, the circuit breaker 
may not be a satisfactory way to stop the bad behavior.  This is because the circuit breaker will periodically restart
things every couple of hours.  If you need to stop the system cold, you want to invoke the StopCaptureDb lambda function.
This will actually shut the database down and disable the trigger permanently (until the database is restored).  However,
shutting the db down will mean that you will lose the data that comes in while the db is turned off, because starting
the db will clear the trigger and error queues.  So this method of stopping the bad behavior should be used as a last
resort.  It is possible to turn the database off and on without changing the trigger (see Advanced Troubleshooting) 
below but this is a "use at your own risk" option and should only be used by someone very familiar with the system.

## Stopping and starting the nwcapture databases

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


Note that shutdown for the observation database is advice and not a command.  If the observation database is running 
an etl job, it will not shut down.

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

## Updating the observations database on DEV

```
1.  Take a manual snapshot of the production observations db in the AWS Console
2.  Use the rundeck job "share_rds_snapshot" to share this production snapshot with the dev account
3.  Run the troubleshoot lambda in the dev account with the 'copy_dev_snapshot' action, providing the
    arn of the shared production snapshot and the namee for the new manual snapshot in the dev account
4.  Update the serverless.yml environment variable LAST_OB_DB_SNAPSHOT with the name of the new manual snapshot
```

## Advanced troubleshooting

If you need to turn the capture database without changing the state of the trigger, you can do so using the
```troubleshoot``` lambda function.  Provide a payload in one of the following two formats:

```
{ "action": "start_capture_db"}
{ "action": "stop_capture_db"}
```

Note that stopping and starting the database without disabling and re-enabling the trigger is likely to
lead to an increase in errors reported and in the size of the error queue.  
