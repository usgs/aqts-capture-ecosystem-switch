import datetime
import json
import os

import boto3
from src.utils import enable_lambda_trigger, disable_lambda_trigger, DEFAULT_DB_INSTANCE_CLASS, CAPTURE_INSTANCE_TAGS, \
    OBSERVATION_INSTANCE_TAGS
import logging

TRIGGER = {
    "TEST": ['aqts-capture-trigger-TEST-aqtsCaptureTrigger'],
    "QA": ['aqts-capture-trigger-QA-aqtsCaptureTrigger'],
    "PROD-EXTERNAL": ['aqts-capture-trigger-PROD-EXTERNAL-aqtsCaptureTrigger']
}

STAGE = os.getenv('STAGE', 'TEST')

DEFAULT_DB_CLUSTER_IDENTIFIER = f"nwcapture-{STAGE.lower()}"
DEFAULT_DB_INSTANCE_IDENTIFIER = f"{DEFAULT_DB_CLUSTER_IDENTIFIER}-instance1"
ENGINE = 'aurora-postgresql'
NWCAPTURE_REAL = f"NWCAPTURE-DB-{STAGE}"

SMALL_DB_SIZE = 'db.r5.xlarge'
BIG_DB_SIZE = DEFAULT_DB_INSTANCE_CLASS
BIG_OB_DB_SIZE = 'db.r5.2xlarge'
SMALL_OB_DB_SIZE = 'db.r5.xlarge'

cloudwatch_client = boto3.client('cloudwatch', os.getenv('AWS_DEPLOYMENT_REGION', 'us-west-2'))
rds_client = boto3.client('rds', os.getenv('AWS_DEPLOYMENT_REGION', 'us-west-2'))

log_level = os.getenv('LOG_LEVEL', logging.ERROR)
logger = logging.getLogger(__name__)
logger.setLevel(log_level)

LIST_OF_LAMBDAS = [
    f"aqts-capture-discrete-loader-{STAGE}-loadDiscrete",
    f"aqts-capture-dvstat-transform-{STAGE}-transform",
    f"aqts-capture-error-handler-{STAGE}-aqtsErrorHandler",
    f"aqts-capture-field-visit-metadata-{STAGE}-preProcess",
    f"aqts-capture-field-visit-transform-{STAGE}-transform",
    f"aqts-capture-pruner-{STAGE}-pruneTimeSeries",
    f"aqts-capture-raw-load-{STAGE}-iowCaptureExtraSmall",
    f"aqts-capture-raw-load-{STAGE}-iowCapture",
    f"aqts-capture-raw-load-{STAGE}-iowCaptureSmall",
    f"aqts-capture-raw-load-{STAGE}-iowCaptureMedium",
    f"aqts-capture-trigger-{STAGE}-aqtsCaptureTrigger",
    f"aqts-capture-ts-corrected-{STAGE}-preProcess",
    f"aqts-capture-ts-description-{STAGE}-processTsDescription",
    f"aqts-capture-ts-field-visit-{STAGE}-preProcess",
    f"aqts-capture-ts-loader-{STAGE}-loadTimeSeries",
]

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
        raise Exception("Cluster is not available")
    else:
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


def execute_recover_machine(event, context):
    arn = os.environ['RECOVER_STATE_MACHINE_ARN']
    payload = {}
    _execute_state_machine(arn, json.dumps(payload))



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
    """
    If we are limiting resize functionality to specific tiers for any reason do it here.
    """
    return True


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


def shrink_observations_db(event, context):
    _validate_observations_resize()
    alarm_state = event["detail"]["state"]["value"]
    if alarm_state == "ALARM":
        logger.info(event)
        ob_id = f"observations-{STAGE.lower()}"
        response = rds_client.describe_db_instances(DBInstanceIdentifier=ob_id)
        db_instance_class = str(response['DBInstances'][0]['DBInstanceClass'])
        if db_instance_class == SMALL_OB_DB_SIZE:
            logger.info(f"Cannot shrink the observations db because it already shrank")
        else:
            logger.info("Disabling the trigger!")
            response = rds_client.modify_db_instance(
                DBInstanceIdentifier=ob_id,
                DBInstanceClass=SMALL_OB_DB_SIZE,
                ApplyImmediately=True
            )
            logger.info(f"Shrinking observations DB, please stand by. {response}")


def grow_observations_db(event, context):
    _validate_observations_resize()
    alarm_state = event["detail"]["state"]["value"]
    if alarm_state == "ALARM":
        logger.info(event)
        ob_id = f"observations-{STAGE.lower()}"
        response = rds_client.describe_db_instances(DBInstanceIdentifier=ob_id)
        db_instance_class = str(response['DBInstances'][0]['DBInstanceClass'])
        if db_instance_class == BIG_OB_DB_SIZE:
            logger.info(f"Cannot grow the observations db because it already shrank")
        else:
            logger.info("Disabling the trigger!")
            response = rds_client.modify_db_instance(
                DBInstanceIdentifier=ob_id,
                DBInstanceClass=BIG_OB_DB_SIZE,
                ApplyImmediately=True
            )
            logger.info(f"Growing observations DB, please stand by. {response}")


def enable_provisioned_concurrency(event, context):
    client = boto3.client('lambda', os.getenv('AWS_DEPLOYMENT_REGION', 'us-west-2'))
    for function_name in LIST_OF_LAMBDAS:
        response = client.list_versions_by_function(FunctionName=function_name)
        latest_version = _get_function_version(response)
        response = client.put_provisioned_concurrency_config(
            FunctionName=function_name,
            Qualifier=latest_version,
            ProvisionedConcurrentExecutions=10
        )
        logger.info(f"enabling_provisioned_concurrency:\n {response}")


def _get_function_version(response):
    versions = response['Versions']
    latest_version = "-1"
    for version in versions:
        this_version = version['Version']
        if this_version > latest_version and 'LATEST' not in this_version:
            latest_version = this_version
    return latest_version


def disable_provisioned_concurrency(event, context):
    client = boto3.client('lambda', os.getenv('AWS_DEPLOYMENT_REGION', 'us-west-2'))

    for function_name in LIST_OF_LAMBDAS:
        response = client.list_versions_by_function(FunctionName=function_name)
        latest_version = _get_function_version(response)
        response = client.delete_provisioned_concurrency_config(
            FunctionName=function_name,
            Qualifier=latest_version
        )
        logger.info(f"disabling_provisioned_concurrency:\n {response}")


def _validate_observations_resize():
    if os.environ['STAGE'] in ('DEV', 'TEST', 'QA'):
        return
    raise Exception(f"Cannot resize the observations db on tier {os.environ['STAGE']}")
