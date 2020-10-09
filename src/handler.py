import os
import boto3
from src.rds import RDS
from src.utils import enable_triggers, describe_db_clusters, start_db_cluster, disable_triggers, stop_db_cluster, \
    purge_queue, stop_observations_db_instance
import logging

DB = {
    "TEST": 'nwcapture-test',
    "QA": 'nwcapture-qa',
    "PROD": 'nwcapture-prod-external'
}

OBSERVATIONS_DB = {
    "TEST": 'observations-test',
    "QA": 'observations-qa',
    "PROD": 'observations-prod-external'
}

SQS = {
    "TEST": 'aqts-capture-trigger-queue-TEST',
    "QA": 'aqts-capture-trigger-queue-QA',
    "PROD": 'aqts-capture-trigger-queue-PROD-EXTERNAL'
}

TRIGGER = {
    "TEST": ['aqts-capture-trigger-TEST-aqtsCaptureTrigger'],
    "QA": ['aqts-capture-trigger-QA-aqtsCaptureTrigger'],
    "PROD": ['aqts-capture-trigger-PROD-EXTERNAL-aqtsCaptureTrigger']
}

OBSERVATIONS_ETL_IN_PROGRESS_SQL = "select count(1) from batch_job_execution where status not in ('COMPLETED', 'FAILED')"

log_level = os.getenv('LOG_LEVEL', logging.ERROR)
logger = logging.getLogger(__name__)
logger.setLevel(log_level)

STAGE = os.getenv('STAGE')

cloudwatch_client = boto3.client('cloudwatch', os.getenv('AWS_DEPLOYMENT_REGION', 'us-west-2'))
rds_client = boto3.client('rds', os.getenv('AWS_DEPLOYMENT_REGION', 'us-west-2'))


def start_capture_db(event, context):
    stage = os.getenv('STAGE')
    if stage in ["TEST", "QA", "PROD"]:
        started = _start_db(DB[stage], TRIGGER[stage], SQS[stage])
    else:
        raise Exception(f"stage not recognized {os.getenv('STAGE')}")
    return {
        'statusCode': 200,
        'message': f"Started the {stage} db: {started}"
    }


def stop_capture_db(event, context):
    stage = os.getenv('STAGE')
    if stage in ["TEST", "QA", "PROD"]:
        stopped = _stop_db(DB[stage], TRIGGER[stage])
    else:
        raise Exception(f"stage not recognized {os.getenv('STAGE')}")
    return {
        'statusCode': 200,
        'message': f"Stopped the {os.getenv('STAGE')} db: {stopped}"
    }


def stop_observations_db(event, context):
    stage = os.getenv('STAGE')
    if stage not in ('TEST', 'QA', 'PROD'):
        raise Exception(f"stage not recognized {os.getenv('STAGE')}")
    should_stop = _run_query()
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
    if stage not in ('TEST', 'QA', 'PROD'):
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
    if stage not in ('TEST', 'QA', 'PROD'):
        raise Exception(f"stage not recognized {os.getenv('STAGE')}")
    if alarm_state == "ALARM":
        logger.info(f"Disabling trigger {TRIGGER[stage]} because error handler is in alarm")
        disable_triggers(TRIGGER[stage])
    elif alarm_state == "OK":
        logger.info(f"Enabling trigger {TRIGGER[stage]} because error handler is okay")
        enable_triggers(TRIGGER[stage])


def _run_query():
    """
    If we look in the batch_job_executions table and something is in a state other than COMPLETED or FAILED,
    assume an ETL is in progress and don't shut the db down.
    """
    rds = RDS()
    result = rds.execute_sql(OBSERVATIONS_ETL_IN_PROGRESS_SQL)
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
            enable_triggers(triggers)
    return started


def _stop_db(db, triggers):
    cluster_identifiers = describe_db_clusters("stop")
    stopped = False
    disable_triggers(triggers)
    for cluster_identifier in cluster_identifiers:
        if cluster_identifier == db:
            stop_db_cluster(db)
            stopped = True
    return stopped
