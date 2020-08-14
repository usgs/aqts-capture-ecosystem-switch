# Aquarius Time Series (AQTS) Capture Ecosystem Switch
[![Build Status](https://travis-ci.org/usgs/aqts-capture-ecosystem-switch.svg?branch=master)](https://travis-ci.org/usgs/aqts-capture-ecosystem-switch)
[![codecov](https://codecov.io/gh/usgs/aqts-capture-ecosystem-switch/branch/master/graph/badge.svg)](https://codecov.io/gh/usgs/aqts-capture-ecosystem-switch)

AWS Lambda function designed to turn off  test db clusters outside of work hours.

### Default behavior
The test database for AQTS will be turned on at 7 am central time and turned off at 6 pm, Monday through Friday.

The QA database will be turned off automatically every Friday at 6 pm.

### What if I need to start a database manually?
In the AWS console, find the lambda function you need and invoke it with any payload:

AWS Console->Lambda->Functions->aqts-capture-ecosystem-switch-TEST-startTestDb

AWS Console->Lambda->Functions->aqts-capture-ecosystem-switch-TEST-startQaDb


### What if I need to prevent automatic shutdown of the database for a long running test?


1. AWS Console->Event Bridge->Events->Rules->aqts-capture-ecosystem-switch-sw-stop<test or qa>->Disable
2. Remember to enable again when your test is concluded.

