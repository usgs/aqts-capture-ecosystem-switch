import datetime
import json
import os

import boto3
from src.utils import enable_lambda_trigger, disable_lambda_trigger, DEFAULT_DB_INSTANCE_CLASS
import logging

TRIGGER = {
    "TEST": ['aqts-capture-trigger-TEST-aqtsCaptureTrigger'],
    "QA": ['aqts-capture-trigger-QA-aqtsCaptureTrigger'],
    "PROD": ['aqts-capture-trigger-PROD-EXTERNAL-aqtsCaptureTrigger']
}

STAGE = os.getenv('STAGE', 'TEST')

DEFAULT_DB_CLUSTER_IDENTIFIER = f"nwcapture-{STAGE.lower()}"
DEFAULT_DB_INSTANCE_IDENTIFIER = f"{DEFAULT_DB_CLUSTER_IDENTIFIER}-instance1"
ENGINE = 'aurora-postgresql'
NWCAPTURE_REAL = f"NWCAPTURE-DB-{STAGE}"

SMALL_DB_SIZE = 'db.r5.xlarge'
BIG_DB_SIZE = DEFAULT_DB_INSTANCE_CLASS

cloudwatch_client = boto3.client('cloudwatch', os.getenv('AWS_DEPLOYMENT_REGION', 'us-west-2'))
rds_client = boto3.client('rds', os.getenv('AWS_DEPLOYMENT_REGION', 'us-west-2'))

log_level = os.getenv('LOG_LEVEL', logging.ERROR)
logger = logging.getLogger(__name__)
logger.setLevel(log_level)

"""
DB resize functions
"""


def disable_trigger(event, context):
    disable_lambda_trigger(TRIGGER[STAGE])


def enable_trigger(event, context):
    if _is_cluster_available(DEFAULT_DB_CLUSTER_IDENTIFIER):
        enable_lambda_trigger(TRIGGER[STAGE])


def shrink_db(event, context):
    logger.info(event)
    response = rds_client.describe_db_instances(DBInstanceIdentifier=DEFAULT_DB_INSTANCE_IDENTIFIER)
    db_instance_class = str(response['DBInstances'][0]['DBInstanceClass'])
    if db_instance_class == SMALL_DB_SIZE:
        logger.info(f"Cannot shrink the db because it already shrank")
    elif not _is_cluster_available(DEFAULT_DB_CLUSTER_IDENTIFIER):
        print("throwing exception because db not available")
        raise Exception("Cluster is not available")
    else:
        logger.info("Disabling the trigger!")
        disable_lambda_trigger(TRIGGER[STAGE])
        response = rds_client.modify_db_instance(
            DBInstanceIdentifier=DEFAULT_DB_INSTANCE_IDENTIFIER,
            DBInstanceClass=SMALL_DB_SIZE,
            ApplyImmediately=True
        )
        logger.info(f"Shrinking DB, please stand by. {response}")


def grow_db(event, context):
    logger.info(event)

    response = rds_client.describe_db_instances(DBInstanceIdentifier=DEFAULT_DB_INSTANCE_IDENTIFIER)
    db_instance_class = str(response['DBInstances'][0]['DBInstanceClass'])
    if db_instance_class == BIG_DB_SIZE:
        logger.info("DB is already grown")
    elif not _is_cluster_available(DEFAULT_DB_CLUSTER_IDENTIFIER):
        raise Exception("Cluster is not available")
    else:
        logger.info("Disabling the trigger!")
        disable_lambda_trigger(TRIGGER[STAGE])
        response = rds_client.modify_db_instance(
            DBInstanceIdentifier=DEFAULT_DB_INSTANCE_IDENTIFIER,
            DBInstanceClass=BIG_DB_SIZE,
            ApplyImmediately=True
        )
        logger.info(f"Growing the DB, please stand by. {response}")


def execute_shrink_machine(event, context):
    arn = os.environ['SHRINK_STATE_MACHINE_ARN']
    payload = {}
    alarm_state = event["detail"]["state"]["value"]
    if alarm_state == "ALARM":
        _execute_state_machine(arn, json.dumps(payload))
        return True
    return False


def execute_grow_machine(event, context):
    arn = os.environ['GROW_STATE_MACHINE_ARN']
    payload = {}
    alarm_state = event["detail"]["state"]["value"]
    if alarm_state == "ALARM":
        _execute_state_machine(arn, json.dumps(payload))
        return True
    return False


def _get_cpu_utilization(db_instance_identifier, period_in_seconds, total_time):
    response = cloudwatch_client.get_metric_data(
        MetricDataQueries=[
            {
                'Id': 'loadTestCpuUtilization',
                'MetricStat': {
                    'Metric': {
                        'Namespace': 'AWS/RDS',
                        'MetricName': 'CPUUtilization',
                        'Dimensions': [
                            {
                                "Name": "DBInstanceIdentifier",
                                "Value": db_instance_identifier
                            }]
                    },
                    'Period': period_in_seconds,
                    'Stat': 'Average',
                }
            }
        ],
        StartTime=(datetime.datetime.now() - datetime.timedelta(seconds=total_time)).timestamp(),
        EndTime=datetime.datetime.now().timestamp()
    )
    return response


def _validate():
    if os.environ['STAGE'] != "QA":
        raise Exception("This lambda is currently only supported on the QA tier")


def _is_cluster_available(cluster_id):
    response = rds_client.describe_db_clusters()
    all_dbs = response['DBClusters']
    available_clusters = [x['DBClusterIdentifier'] for x in all_dbs if x['Status'] == 'available']
    if cluster_id in available_clusters:
        return True
    else:
        raise Exception(f"DB {DEFAULT_DB_CLUSTER_IDENTIFIER} is not ready yet")


def _execute_state_machine(state_machine_arn, invocation_payload, region='us-west-2'):
    sf = boto3.client('stepfunctions', region_name=region)
    resp = sf.start_execution(
        stateMachineArn=state_machine_arn,
        input=invocation_payload
    )
    return resp
