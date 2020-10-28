import datetime
import json
import os

import boto3
from src.rds import RDS
from src.utils import enable_lambda_trigger, describe_db_clusters, start_db_cluster, disable_lambda_trigger, \
    stop_db_cluster, \
    purge_queue, stop_observations_db_instance, DEFAULT_DB_INSTANCE_CLASS
import logging

STAGES = ['TEST', 'QA', 'PROD-EXTERNAL']
DB = {
    "TEST": 'nwcapture-test',
    "QA": 'nwcapture-qa',
    "PROD-EXTERNAL": 'nwcapture-prod-external'
}

OBSERVATIONS_DB = {
    "TEST": 'observations-test',
    "QA": 'observations-qa',
    "PROD-EXTERNAL": 'observations-prod-external'
}

SQS = {
    "TEST": ['aqts-capture-trigger-queue-TEST', 'aqts-capture-error-queue-TEST'],
    "QA": ['aqts-capture-trigger-queue-QA', 'aqts-capture-error-queue-QA'],
    "PROD-EXTERNAL": ['aqts-capture-trigger-queue-PROD-EXTERNAL']
}

TRIGGER = {
    "TEST": ['aqts-capture-trigger-TEST-aqtsCaptureTrigger'],
    "QA": ['aqts-capture-trigger-QA-aqtsCaptureTrigger'],
    "PROD-EXTERNAL": ['aqts-capture-trigger-PROD-EXTERNAL-aqtsCaptureTrigger']
}

STAGE = os.getenv('STAGE', 'TEST')
CAPTURE_TRIGGER_QUEUE = f"aqts-capture-trigger-queue-{STAGE}"
ERROR_QUEUE = f"aqts-capture-error-queue-{STAGE}"

DEFAULT_DB_CLUSTER_IDENTIFIER = f"nwcapture-{STAGE.lower()}"
DEFAULT_DB_INSTANCE_IDENTIFIER = f"{DEFAULT_DB_CLUSTER_IDENTIFIER}-instance1"
ENGINE = 'aurora-postgresql'
NWCAPTURE_REAL = f"NWCAPTURE-DB-{STAGE}"

log_level = os.getenv('LOG_LEVEL', logging.ERROR)
logger = logging.getLogger(__name__)
logger.setLevel(log_level)

STAGE = os.getenv('STAGE', 'TEST')

cloudwatch_client = boto3.client('cloudwatch', os.getenv('AWS_DEPLOYMENT_REGION', 'us-west-2'))
secrets_client = boto3.client('secretsmanager', os.getenv('AWS_DEPLOYMENT_REGION', 'us-west-2'))
rds_client = boto3.client('rds', os.getenv('AWS_DEPLOYMENT_REGION', 'us-west-2'))
sqs_client = boto3.client('sqs', os.getenv('AWS_DEPLOYMENT_REGION', 'us-west-2'))


def _get_etl_start():
    four_days_ago = datetime.datetime.now() - datetime.timedelta(4)
    my_month = str(four_days_ago.month)
    if len(my_month) == 1:
        my_month = f"0{my_month}"
    my_day = str(four_days_ago.day)
    if len(my_day) == 1:
        my_day = f"0{my_day}"
    my_etl_start = f"{four_days_ago.year}-{my_month}-{my_day}"
    return my_etl_start


etl_start = _get_etl_start()
OBSERVATIONS_ETL_IN_PROGRESS_SQL = \
    "select count(1) from batch_job_execution where status not in ('COMPLETED', 'FAILED') and start_time > %s"

"""
DB stop and start functions
"""


def start_capture_db(event, context):
    stage = os.getenv('STAGE')
    if stage in STAGES:
        started = _start_db(DB[stage], TRIGGER[stage], SQS[stage])
    else:
        raise Exception(f"stage not recognized {os.getenv('STAGE')}")
    return {
        'statusCode': 200,
        'message': f"Started the {stage} db: {started}"
    }


def stop_capture_db(event, context):
    stage = os.getenv('STAGE')
    if stage in STAGES:
        stopped = _stop_db(DB[stage], TRIGGER[stage])
    else:
        raise Exception(f"stage not recognized {os.getenv('STAGE')}")
    return {
        'statusCode': 200,
        'message': f"Stopped the {os.getenv('STAGE')} db: {stopped}"
    }


def stop_observations_db(event, context):
    stage = os.getenv('STAGE')
    if stage not in STAGES:
        raise Exception(f"stage not recognized {os.getenv('STAGE')}")
    should_stop = run_etl_query()
    if not should_stop:
        return {
            'statusCode': 200,
            'message': f"Could not stop the {stage} observations db. It was busy."
        }
    stop_observations_db_instance(OBSERVATIONS_DB[stage])
    return {
        'statusCode': 200,
        'message': f"Stopped the {os.getenv('STAGE')} observations db."
    }


def start_observations_db(event, context):
    stage = os.getenv('STAGE')
    if stage not in STAGES:
        raise Exception(f"stage not recognized {os.getenv('STAGE')}")
    rds_client.start_db_instance(DBInstanceIdentifier=OBSERVATIONS_DB[stage])
    return {
        'statusCode': 200,
        'message': f"Started the {stage} observations db."
    }


def control_db_utilization(event, context):
    """
    Right now we are only listening for the error handler alarm, because it
    is the last alarm to get triggered when we are going into a death spiral.
    :param event:
    :param context:
    :return:
    """
    logger.info(event)
    alarm_state = event["detail"]["state"]["value"]
    stage = os.getenv('STAGE')
    if stage not in STAGES:
        raise Exception(f"stage not recognized {os.getenv('STAGE')}")
    if alarm_state == "ALARM":
        logger.info(f"Disabling trigger {TRIGGER[stage]} because error handler is in alarm")
        disable_lambda_trigger(TRIGGER[stage])
    elif alarm_state == "OK":
        """
        We do NOT want to enable the trigger if the db is not up and running.
        """
        active_dbs = describe_db_clusters('stop')
        if DB[stage] in active_dbs:
            logger.info(f"Enabling trigger {TRIGGER[stage]} for {DB[stage]} because error handler is okay")
            enable_lambda_trigger(TRIGGER[stage])


def run_etl_query(rds=None):
    """
    If we look in the batch_job_executions table and something is in a state other than COMPLETED or FAILED,
    assume an ETL is in progress and don't shut the db down.

    Also, if someone aborts an ETL job in Jenkins, we can get in a messed up state where a job execution has
    STARTED but never fails or completes, so check the start time and if it's more than 4 days ignore those and
    shut the db down anyway.
    """
    if rds is None:
        rds = RDS(os.getenv('DB_HOST'), os.getenv('DB_USER'), os.getenv('DB_NAME'), os.getenv('DB_PASSWORD'))
    result = rds.execute_sql(OBSERVATIONS_ETL_IN_PROGRESS_SQL, (etl_start,))
    if result[0] > 0:
        logger.debug(f"Cannot shutdown down observations db because {result[0]} processes are running")
        return False
    elif result[0] == 0:
        logger.debug("Shutting down observations db because no processes are running")
        return True
    else:
        raise Exception(f"something wrong with db result {result}")


def _start_db(db, triggers, queue_name):
    purge_queue(queue_name)
    cluster_identifiers = describe_db_clusters("start")
    started = False
    for cluster_identifier in cluster_identifiers:
        if cluster_identifier == db:
            start_db_cluster(db)
            started = True
            enable_lambda_trigger(triggers)
    return started


def _stop_db(db, triggers):
    cluster_identifiers = describe_db_clusters("stop")
    stopped = False
    disable_lambda_trigger(triggers)
    for cluster_identifier in cluster_identifiers:
        if cluster_identifier == db:
            stop_db_cluster(db)
            stopped = True
    return stopped


"""
Miscellaneous functions
"""


def _validate():
    if os.environ['STAGE'] != "QA":
        raise Exception("This lambda is currently only supported on the QA tier")
