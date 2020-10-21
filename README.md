# Aquarius Time Series (AQTS) Capture Ecosystem Switch
[![Build Status](https://travis-ci.org/usgs/aqts-capture-ecosystem-switch.svg?branch=master)](https://travis-ci.org/usgs/aqts-capture-ecosystem-switch)
[![codecov](https://codecov.io/gh/usgs/aqts-capture-ecosystem-switch/branch/master/graph/badge.svg)](https://codecov.io/gh/usgs/aqts-capture-ecosystem-switch)

AWS Lambda functions designed to turn off resources when not in use.

## The nwcapture-test database

Is scheduled to turn on at 7 am and off at 6 pm, Monday through Friday central time.

If you need to turn it on manually, use the StartCaptureDb lambda method.  If you need to turn it off, use the 
StopCaptureDb lamba.

## The nwcapture-qa database

If you need the nwcapture-qa database, you need to create it by running the 
aqts-capture-ecosystem-switch-create-db-QA state machine.  

When you are finished, you need to delete it with the DeleteCaptureDb lambda function manually.

Note that creating the aurora cluster takes up to two hours.


## What if I need to prevent automatic shutdown of nwcapture-test for a long running test?

1. AWS Console->Event Bridge->Events->Rules->aqts-capture-ecosystem-switch-sw-stoptest->Disable
2. Remember to enable again when your test is concluded.
